import { sourcePool, targetPool, config } from './config.js';

interface TableInfo {
  schema: string;
  name: string;
  definition: string;
}

interface SequenceInfo {
  schema: string;
  name: string;
  definition: string;
  lastValue: bigint;
}

interface IndexInfo {
  schema: string;
  table: string;
  name: string;
  definition: string;
}

interface ConstraintInfo {
  schema: string;
  table: string;
  name: string;
  definition: string;
  type: string;
}

interface EnumInfo {
  schema: string;
  name: string;
  labels: string[];
}

interface ExtensionInfo {
  name: string;
  schema: string;
}

interface ViewInfo {
  schema: string;
  name: string;
  definition: string;
}

interface GrantInfo {
  schema: string;
  tableName: string;
  grantee: string;
  privileges: string[];
}

// Get installed extensions
export async function getExtensions(): Promise<ExtensionInfo[]> {
  const result = await sourcePool.query(`
    SELECT extname as name, nspname as schema
    FROM pg_extension e
    JOIN pg_namespace n ON e.extnamespace = n.oid
    WHERE extname NOT IN ('plpgsql')
    ORDER BY extname
  `);
  return result.rows;
}

// Get enum types
export async function getEnums(): Promise<EnumInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');
  const result = await sourcePool.query(`
    SELECT
      n.nspname as schema,
      t.typname as name,
      array_agg(e.enumlabel ORDER BY e.enumsortorder) as labels
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_namespace n ON t.typnamespace = n.oid
    WHERE n.nspname IN (${schemaList})
    GROUP BY n.nspname, t.typname
    ORDER BY n.nspname, t.typname
  `);
  return result.rows.map(row => ({
    schema: row.schema,
    name: row.name,
    // Handle both array and string format (pg returns {a,b,c} string via some drivers)
    labels: Array.isArray(row.labels)
      ? row.labels
      : row.labels.replace(/^\{|\}$/g, '').split(','),
  }));
}

// Get sequences
export async function getSequences(): Promise<SequenceInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');
  const result = await sourcePool.query(`
    SELECT
      schemaname as schema,
      sequencename as name,
      start_value,
      min_value,
      max_value,
      increment_by,
      cycle,
      cache_size
    FROM pg_sequences
    WHERE schemaname IN (${schemaList})
    ORDER BY schemaname, sequencename
  `);

  const sequences: SequenceInfo[] = [];
  for (const row of result.rows) {
    // Get current value
    const lastValueResult = await sourcePool.query(
      `SELECT last_value FROM "${row.schema}"."${row.name}"`
    );
    const lastValue = lastValueResult.rows[0]?.last_value || row.start_value;

    const definition = `CREATE SEQUENCE IF NOT EXISTS "${row.schema}"."${row.name}"
      START WITH ${row.start_value}
      INCREMENT BY ${row.increment_by}
      MINVALUE ${row.min_value}
      MAXVALUE ${row.max_value}
      ${row.cycle ? 'CYCLE' : 'NO CYCLE'}
      CACHE ${row.cache_size}`;

    sequences.push({
      schema: row.schema,
      name: row.name,
      definition,
      lastValue,
    });
  }

  return sequences;
}

// Get tables with their CREATE statements
export async function getTables(): Promise<TableInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');
  const excludeList = config.excludeTables.map(t => `'${t}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      t.table_schema as schema,
      t.table_name as name
    FROM information_schema.tables t
    WHERE t.table_schema IN (${schemaList})
      AND t.table_type = 'BASE TABLE'
      ${excludeList.length > 0 ? `AND t.table_name NOT IN (${excludeList})` : ''}
    ORDER BY t.table_schema, t.table_name
  `);

  const tables: TableInfo[] = [];
  for (const row of result.rows) {
    const columnsResult = await sourcePool.query(`
      SELECT
        column_name,
        data_type,
        udt_schema,
        udt_name,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable,
        column_default,
        identity_generation
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [row.schema, row.name]);

    const columnDefs = columnsResult.rows.map(col => {
      let dataType = col.data_type;

      // Handle user-defined types (enums)
      if (dataType === 'USER-DEFINED') {
        dataType = `"${col.udt_schema}"."${col.udt_name}"`;
      } else if (dataType === 'ARRAY') {
        dataType = `${col.udt_name.replace(/^_/, '')}[]`;
      } else if (dataType === 'character varying' && col.character_maximum_length) {
        dataType = `varchar(${col.character_maximum_length})`;
      } else if (dataType === 'numeric' && col.numeric_precision) {
        dataType = `numeric(${col.numeric_precision}, ${col.numeric_scale || 0})`;
      }

      let def = `"${col.column_name}" ${dataType}`;

      if (col.identity_generation) {
        def += ` GENERATED ${col.identity_generation} AS IDENTITY`;
      } else if (col.column_default) {
        def += ` DEFAULT ${col.column_default}`;
      }

      if (col.is_nullable === 'NO') {
        def += ' NOT NULL';
      }

      return def;
    });

    const definition = `CREATE TABLE IF NOT EXISTS "${row.schema}"."${row.name}" (\n  ${columnDefs.join(',\n  ')}\n)`;
    tables.push({ schema: row.schema, name: row.name, definition });
  }

  return tables;
}

// Get indexes
export async function getIndexes(): Promise<IndexInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      schemaname as schema,
      tablename as table,
      indexname as name,
      indexdef as definition
    FROM pg_indexes
    WHERE schemaname IN (${schemaList})
      AND indexname NOT LIKE '%_pkey'
    ORDER BY schemaname, tablename, indexname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    table: row.table,
    name: row.name,
    definition: row.definition.replace(/^CREATE INDEX/, 'CREATE INDEX IF NOT EXISTS'),
  }));
}

// Get constraints (primary keys, foreign keys, unique, check)
export async function getConstraints(): Promise<ConstraintInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      n.nspname as schema,
      t.relname as table,
      c.conname as name,
      c.contype as type,
      pg_get_constraintdef(c.oid) as definition
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname IN (${schemaList})
    ORDER BY
      CASE c.contype
        WHEN 'p' THEN 1  -- primary key first
        WHEN 'u' THEN 2  -- unique
        WHEN 'c' THEN 3  -- check
        WHEN 'f' THEN 4  -- foreign key last
      END,
      n.nspname, t.relname, c.conname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    table: row.table,
    name: row.name,
    type: row.type,
    definition: row.definition,
  }));
}

// Get views
export async function getViews(): Promise<ViewInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      schemaname as schema,
      viewname as name,
      definition
    FROM pg_views
    WHERE schemaname IN (${schemaList})
    ORDER BY schemaname, viewname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    name: row.name,
    definition: row.definition,
  }));
}

// Get table grants for Supabase roles
export async function getGrants(): Promise<GrantInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      table_schema as schema,
      table_name,
      grantee,
      array_agg(privilege_type ORDER BY privilege_type) as privileges
    FROM information_schema.table_privileges
    WHERE table_schema IN (${schemaList})
      AND grantee IN ('anon', 'authenticated', 'service_role')
    GROUP BY table_schema, table_name, grantee
    ORDER BY table_schema, table_name, grantee
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    tableName: row.table_name,
    grantee: row.grantee,
    // Handle both array and string format (pg returns {a,b,c} string via some drivers)
    privileges: Array.isArray(row.privileges)
      ? row.privileges
      : row.privileges.replace(/^\{|\}$/g, '').split(','),
  }));
}

// Main schema migration function
export async function migrateSchema(): Promise<void> {
  console.log('📦 Starting schema migration...\n');
  const targetClient = await targetPool.connect();

  try {
    // 1. Create schemas
    console.log('📂 Creating schemas...');
    for (const schema of config.schemas) {
      await targetClient.query(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);
      console.log(`   ✅ Schema: ${schema}`);
    }

    // 2. Install extensions
    console.log('\n🔌 Installing extensions...');
    const extensions = await getExtensions();
    for (const ext of extensions) {
      try {
        await targetClient.query(`CREATE EXTENSION IF NOT EXISTS "${ext.name}" SCHEMA "${ext.schema}"`);
        console.log(`   ✅ Extension: ${ext.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Extension ${ext.name}: ${message}`);
      }
    }

    // 3. Create enum types
    console.log('\n🏷️  Creating enum types...');
    const enums = await getEnums();
    for (const enumType of enums) {
      const labels = enumType.labels.map(l => `'${l}'`).join(', ');
      try {
        await targetClient.query(`
          DO $$ BEGIN
            CREATE TYPE "${enumType.schema}"."${enumType.name}" AS ENUM (${labels});
          EXCEPTION
            WHEN duplicate_object THEN null;
          END $$;
        `);
        console.log(`   ✅ Enum: ${enumType.schema}.${enumType.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Enum ${enumType.schema}.${enumType.name}: ${message}`);
      }
    }

    // 4. Create sequences
    console.log('\n🔢 Creating sequences...');
    const sequences = await getSequences();
    for (const seq of sequences) {
      try {
        await targetClient.query(seq.definition);
        await targetClient.query(`SELECT setval('"${seq.schema}"."${seq.name}"', ${seq.lastValue}, true)`);
        console.log(`   ✅ Sequence: ${seq.schema}.${seq.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Sequence ${seq.schema}.${seq.name}: ${message}`);
      }
    }

    // 5. Create tables
    console.log('\n📋 Creating tables...');
    const tables = await getTables();
    for (const table of tables) {
      try {
        await targetClient.query(table.definition);
        console.log(`   ✅ Table: ${table.schema}.${table.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ❌ Table ${table.schema}.${table.name}: ${message}`);
      }
    }

    // 6. Add constraints (primary keys, unique, check first)
    console.log('\n🔗 Adding constraints...');
    const constraints = await getConstraints();
    const fkConstraints = constraints.filter(c => c.type === 'f');
    const otherConstraints = constraints.filter(c => c.type !== 'f');

    for (const constraint of otherConstraints) {
      try {
        await targetClient.query(`
          ALTER TABLE "${constraint.schema}"."${constraint.table}"
          ADD CONSTRAINT "${constraint.name}" ${constraint.definition}
        `);
        console.log(`   ✅ Constraint: ${constraint.schema}.${constraint.table}.${constraint.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        if (!message.includes('already exists')) {
          console.log(`   ⚠️  Constraint ${constraint.name}: ${message}`);
        }
      }
    }

    // 7. Create indexes
    console.log('\n📇 Creating indexes...');
    const indexes = await getIndexes();
    for (const index of indexes) {
      try {
        await targetClient.query(index.definition);
        console.log(`   ✅ Index: ${index.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        if (!message.includes('already exists')) {
          console.log(`   ⚠️  Index ${index.name}: ${message}`);
        }
      }
    }

    // 8. Add foreign key constraints (after all tables exist)
    console.log('\n🔗 Adding foreign key constraints...');
    for (const constraint of fkConstraints) {
      try {
        await targetClient.query(`
          ALTER TABLE "${constraint.schema}"."${constraint.table}"
          ADD CONSTRAINT "${constraint.name}" ${constraint.definition}
        `);
        console.log(`   ✅ FK: ${constraint.schema}.${constraint.table}.${constraint.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        if (!message.includes('already exists')) {
          console.log(`   ⚠️  FK ${constraint.name}: ${message}`);
        }
      }
    }

    // 9. Create views (after tables and functions exist)
    console.log('\n👁️  Creating views...');
    const views = await getViews();
    for (const view of views) {
      try {
        // Drop existing view first to handle definition changes
        await targetClient.query(`DROP VIEW IF EXISTS "${view.schema}"."${view.name}" CASCADE`);
        await targetClient.query(`CREATE VIEW "${view.schema}"."${view.name}" AS ${view.definition}`);
        console.log(`   ✅ View: ${view.schema}.${view.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ❌ View ${view.schema}.${view.name}: ${message}`);
      }
    }

    console.log('\n✅ Schema migration completed!\n');
  } finally {
    targetClient.release();
  }
}

// Migrate table grants for Supabase roles
export async function migrateGrants(): Promise<void> {
  console.log('🔑 Starting grants migration...\n');
  const targetClient = await targetPool.connect();

  try {
    // Ensure Supabase roles exist
    console.log('👤 Ensuring Supabase roles exist...');
    const roles = ['anon', 'authenticated', 'service_role'];
    for (const role of roles) {
      try {
        await targetClient.query(`
          DO $$ BEGIN
            CREATE ROLE "${role}" NOLOGIN;
          EXCEPTION
            WHEN duplicate_object THEN null;
          END $$;
        `);
        console.log(`   ✅ Role: ${role}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Role ${role}: ${message}`);
      }
    }

    // Get and apply grants
    console.log('\n📜 Applying table grants...');
    const grants = await getGrants();

    for (const grant of grants) {
      const privilegeList = grant.privileges.join(', ');
      try {
        await targetClient.query(`
          GRANT ${privilegeList} ON "${grant.schema}"."${grant.tableName}" TO "${grant.grantee}"
        `);
        console.log(`   ✅ ${grant.grantee}: ${grant.schema}.${grant.tableName} (${privilegeList})`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Grant ${grant.grantee} on ${grant.tableName}: ${message}`);
      }
    }

    // Grant usage on schemas
    console.log('\n📂 Granting schema usage...');
    for (const schema of config.schemas) {
      for (const role of roles) {
        try {
          await targetClient.query(`GRANT USAGE ON SCHEMA "${schema}" TO "${role}"`);
        } catch (error: unknown) {
          // Ignore errors
        }
      }
      console.log(`   ✅ Schema ${schema}: granted to ${roles.join(', ')}`);
    }

    // Grant usage on sequences
    console.log('\n🔢 Granting sequence usage...');
    for (const schema of config.schemas) {
      try {
        await targetClient.query(`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "${schema}" TO anon, authenticated, service_role`);
        console.log(`   ✅ Sequences in ${schema}: granted`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Sequences in ${schema}: ${message}`);
      }
    }

    // Grant execute on functions
    console.log('\n⚡ Granting function execute...');
    for (const schema of config.schemas) {
      try {
        await targetClient.query(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "${schema}" TO anon, authenticated, service_role`);
        console.log(`   ✅ Functions in ${schema}: granted`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ⚠️  Functions in ${schema}: ${message}`);
      }
    }

    console.log('\n✅ Grants migration completed!\n');
  } finally {
    targetClient.release();
  }
}

// Export schema to SQL file (for review or manual migration)
export async function exportSchemaToSQL(): Promise<string> {
  let sql = '-- Schema Migration Script\n';
  sql += '-- Generated by supabase-migration\n';
  sql += `-- Date: ${new Date().toISOString()}\n\n`;

  // 1. Create schemas
  sql += '-- ==========================================\n';
  sql += '-- SCHEMAS\n';
  sql += '-- ==========================================\n';
  for (const schema of config.schemas) {
    sql += `CREATE SCHEMA IF NOT EXISTS "${schema}";\n`;
  }
  sql += '\n';

  // 2. Extensions
  sql += '-- ==========================================\n';
  sql += '-- EXTENSIONS\n';
  sql += '-- ==========================================\n';
  const extensions = await getExtensions();
  for (const ext of extensions) {
    sql += `CREATE EXTENSION IF NOT EXISTS "${ext.name}" SCHEMA "${ext.schema}";\n`;
  }
  sql += '\n';

  // 3. Enum types
  sql += '-- ==========================================\n';
  sql += '-- ENUM TYPES\n';
  sql += '-- ==========================================\n';
  const enums = await getEnums();
  for (const enumType of enums) {
    const labels = enumType.labels.map(l => `'${l}'`).join(', ');
    sql += `DO $$ BEGIN\n`;
    sql += `  CREATE TYPE "${enumType.schema}"."${enumType.name}" AS ENUM (${labels});\n`;
    sql += `EXCEPTION WHEN duplicate_object THEN null;\n`;
    sql += `END $$;\n\n`;
  }

  // 4. Sequences
  sql += '-- ==========================================\n';
  sql += '-- SEQUENCES\n';
  sql += '-- ==========================================\n';
  const sequences = await getSequences();
  for (const seq of sequences) {
    sql += `${seq.definition};\n`;
    sql += `SELECT setval('"${seq.schema}"."${seq.name}"', ${seq.lastValue}, true);\n\n`;
  }

  // 5. Tables
  sql += '-- ==========================================\n';
  sql += '-- TABLES\n';
  sql += '-- ==========================================\n';
  const tables = await getTables();
  for (const table of tables) {
    sql += `${table.definition};\n\n`;
  }

  // 6. Constraints (non-FK)
  sql += '-- ==========================================\n';
  sql += '-- CONSTRAINTS (Primary Keys, Unique, Check)\n';
  sql += '-- ==========================================\n';
  const constraints = await getConstraints();
  const fkConstraints = constraints.filter(c => c.type === 'f');
  const otherConstraints = constraints.filter(c => c.type !== 'f');

  for (const constraint of otherConstraints) {
    sql += `ALTER TABLE "${constraint.schema}"."${constraint.table}" `;
    sql += `ADD CONSTRAINT "${constraint.name}" ${constraint.definition};\n`;
  }
  sql += '\n';

  // 7. Indexes
  sql += '-- ==========================================\n';
  sql += '-- INDEXES\n';
  sql += '-- ==========================================\n';
  const indexes = await getIndexes();
  for (const index of indexes) {
    sql += `${index.definition};\n`;
  }
  sql += '\n';

  // 8. Foreign Keys
  sql += '-- ==========================================\n';
  sql += '-- FOREIGN KEYS\n';
  sql += '-- ==========================================\n';
  for (const constraint of fkConstraints) {
    sql += `ALTER TABLE "${constraint.schema}"."${constraint.table}" `;
    sql += `ADD CONSTRAINT "${constraint.name}" ${constraint.definition};\n`;
  }
  sql += '\n';

  // 9. Views
  sql += '-- ==========================================\n';
  sql += '-- VIEWS\n';
  sql += '-- ==========================================\n';
  const views = await getViews();
  for (const view of views) {
    sql += `DROP VIEW IF EXISTS "${view.schema}"."${view.name}" CASCADE;\n`;
    sql += `CREATE VIEW "${view.schema}"."${view.name}" AS ${view.definition};\n\n`;
  }

  return sql;
}

// Export grants to SQL file
export async function exportGrantsToSQL(): Promise<string> {
  let sql = '-- Grants Migration Script\n';
  sql += '-- Generated by supabase-migration\n\n';

  const roles = ['anon', 'authenticated', 'service_role'];

  // Create roles
  sql += '-- ==========================================\n';
  sql += '-- ROLES\n';
  sql += '-- ==========================================\n';
  for (const role of roles) {
    sql += `DO $$ BEGIN\n`;
    sql += `  CREATE ROLE "${role}" NOLOGIN;\n`;
    sql += `EXCEPTION WHEN duplicate_object THEN null;\n`;
    sql += `END $$;\n`;
  }
  sql += '\n';

  // Schema usage grants
  sql += '-- ==========================================\n';
  sql += '-- SCHEMA USAGE\n';
  sql += '-- ==========================================\n';
  for (const schema of config.schemas) {
    for (const role of roles) {
      sql += `GRANT USAGE ON SCHEMA "${schema}" TO "${role}";\n`;
    }
  }
  sql += '\n';

  // Table grants
  sql += '-- ==========================================\n';
  sql += '-- TABLE GRANTS\n';
  sql += '-- ==========================================\n';
  const grants = await getGrants();
  for (const grant of grants) {
    const privilegeList = grant.privileges.join(', ');
    sql += `GRANT ${privilegeList} ON "${grant.schema}"."${grant.tableName}" TO "${grant.grantee}";\n`;
  }
  sql += '\n';

  // Sequence grants
  sql += '-- ==========================================\n';
  sql += '-- SEQUENCE GRANTS\n';
  sql += '-- ==========================================\n';
  for (const schema of config.schemas) {
    sql += `GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "${schema}" TO anon, authenticated, service_role;\n`;
  }
  sql += '\n';

  // Function grants
  sql += '-- ==========================================\n';
  sql += '-- FUNCTION GRANTS\n';
  sql += '-- ==========================================\n';
  for (const schema of config.schemas) {
    sql += `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "${schema}" TO anon, authenticated, service_role;\n`;
  }

  return sql;
}
