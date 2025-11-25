import { sourcePool, targetPool, config } from './config.js';

interface TableInfo {
  schema: string;
  name: string;
  rowCount: number;
  columns: string[];
  primaryKey: string | null;
}

interface MigrationStats {
  table: string;
  totalRows: number;
  migratedRows: number;
  duration: number;
  status: 'success' | 'failed' | 'skipped';
  error?: string;
}

// Get tables with row counts and column info
export async function getTablesForDataMigration(): Promise<TableInfo[]> {
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
    // Get row count
    const countResult = await sourcePool.query(
      `SELECT COUNT(*) as count FROM "${row.schema}"."${row.name}"`
    );

    // Get columns
    const columnsResult = await sourcePool.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [row.schema, row.name]);

    // Get primary key
    const pkResult = await sourcePool.query(`
      SELECT a.attname
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE i.indisprimary
        AND n.nspname = $1
        AND c.relname = $2
      LIMIT 1
    `, [row.schema, row.name]);

    tables.push({
      schema: row.schema,
      name: row.name,
      rowCount: parseInt(countResult.rows[0].count, 10),
      columns: columnsResult.rows.map(r => r.column_name),
      primaryKey: pkResult.rows[0]?.attname || null,
    });
  }

  // Sort by dependency order (tables with FK references should come last)
  return sortByDependency(tables);
}

// Sort tables by foreign key dependency
async function sortByDependency(tables: TableInfo[]): Promise<TableInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  // Get foreign key relationships
  const fkResult = await sourcePool.query(`
    SELECT
      n1.nspname || '.' || c1.relname as child,
      n2.nspname || '.' || c2.relname as parent
    FROM pg_constraint con
    JOIN pg_class c1 ON con.conrelid = c1.oid
    JOIN pg_namespace n1 ON c1.relnamespace = n1.oid
    JOIN pg_class c2 ON con.confrelid = c2.oid
    JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
    WHERE con.contype = 'f'
      AND n1.nspname IN (${schemaList})
  `);

  const dependencies: Map<string, Set<string>> = new Map();

  for (const row of fkResult.rows) {
    if (!dependencies.has(row.child)) {
      dependencies.set(row.child, new Set());
    }
    dependencies.get(row.child)!.add(row.parent);
  }

  // Topological sort
  const sorted: TableInfo[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();

  function visit(table: TableInfo): void {
    const key = `${table.schema}.${table.name}`;

    if (visited.has(key)) return;
    if (visiting.has(key)) {
      // Circular dependency - just add it
      sorted.push(table);
      visited.add(key);
      return;
    }

    visiting.add(key);

    // Visit dependencies first
    const deps = dependencies.get(key) || new Set();
    for (const dep of deps) {
      const depTable = tables.find(t => `${t.schema}.${t.name}` === dep);
      if (depTable) {
        visit(depTable);
      }
    }

    visiting.delete(key);
    visited.add(key);
    sorted.push(table);
  }

  for (const table of tables) {
    visit(table);
  }

  return sorted;
}

// Migrate data for a single table
async function migrateTableData(table: TableInfo): Promise<MigrationStats> {
  const startTime = Date.now();
  const stats: MigrationStats = {
    table: `${table.schema}.${table.name}`,
    totalRows: table.rowCount,
    migratedRows: 0,
    duration: 0,
    status: 'success',
  };

  if (table.rowCount === 0) {
    stats.status = 'skipped';
    stats.duration = Date.now() - startTime;
    return stats;
  }

  const targetClient = await targetPool.connect();

  try {
    // Clear existing data
    await targetClient.query(`TRUNCATE "${table.schema}"."${table.name}" CASCADE`);

    // Build column list
    const columns = table.columns.map(c => `"${c}"`).join(', ');

    // Migrate in batches
    let offset = 0;
    const batchSize = config.batchSize;

    while (offset < table.rowCount) {
      // Fetch batch from source
      const orderBy = table.primaryKey ? `ORDER BY "${table.primaryKey}"` : '';
      const selectQuery = `
        SELECT ${columns}
        FROM "${table.schema}"."${table.name}"
        ${orderBy}
        LIMIT ${batchSize} OFFSET ${offset}
      `;

      const sourceData = await sourcePool.query(selectQuery);

      if (sourceData.rows.length === 0) break;

      // Build INSERT statement with parameterized values
      const values: unknown[] = [];
      const valuePlaceholders: string[] = [];

      for (let rowIdx = 0; rowIdx < sourceData.rows.length; rowIdx++) {
        const row = sourceData.rows[rowIdx];
        const rowValues: string[] = [];

        for (let colIdx = 0; colIdx < table.columns.length; colIdx++) {
          const paramIdx = rowIdx * table.columns.length + colIdx + 1;
          rowValues.push(`$${paramIdx}`);
          values.push(row[table.columns[colIdx]]);
        }

        valuePlaceholders.push(`(${rowValues.join(', ')})`);
      }

      const insertQuery = `
        INSERT INTO "${table.schema}"."${table.name}" (${columns})
        VALUES ${valuePlaceholders.join(', ')}
      `;

      await targetClient.query(insertQuery, values);
      stats.migratedRows += sourceData.rows.length;
      offset += batchSize;

      // Progress indicator
      const progress = Math.round((stats.migratedRows / table.rowCount) * 100);
      process.stdout.write(`\r   📊 ${table.schema}.${table.name}: ${stats.migratedRows}/${table.rowCount} (${progress}%)`);
    }

    process.stdout.write('\n');
  } catch (error: unknown) {
    stats.status = 'failed';
    stats.error = error instanceof Error ? error.message : String(error);
  } finally {
    targetClient.release();
  }

  stats.duration = Date.now() - startTime;
  return stats;
}

// Main data migration function
export async function migrateData(): Promise<MigrationStats[]> {
  console.log('📦 Starting data migration...\n');

  const tables = await getTablesForDataMigration();
  console.log(`📋 Found ${tables.length} tables to migrate\n`);

  // Show table summary
  const totalRows = tables.reduce((sum, t) => sum + t.rowCount, 0);
  console.log(`📊 Total rows to migrate: ${totalRows.toLocaleString()}\n`);

  // Disable triggers temporarily for faster import
  const targetClient = await targetPool.connect();
  try {
    await targetClient.query('SET session_replication_role = replica');
  } finally {
    targetClient.release();
  }

  const results: MigrationStats[] = [];

  for (const table of tables) {
    const stats = await migrateTableData(table);
    results.push(stats);

    if (stats.status === 'failed') {
      console.log(`   ❌ ${stats.table}: ${stats.error}`);
    } else if (stats.status === 'skipped') {
      console.log(`   ⏭️  ${stats.table}: skipped (empty)`);
    } else {
      console.log(`   ✅ ${stats.table}: ${stats.migratedRows} rows in ${stats.duration}ms`);
    }
  }

  // Re-enable triggers
  const targetClient2 = await targetPool.connect();
  try {
    await targetClient2.query('SET session_replication_role = DEFAULT');
  } finally {
    targetClient2.release();
  }

  // Summary
  const successful = results.filter(r => r.status === 'success').length;
  const failed = results.filter(r => r.status === 'failed').length;
  const skipped = results.filter(r => r.status === 'skipped').length;
  const totalMigrated = results.reduce((sum, r) => sum + r.migratedRows, 0);
  const totalDuration = results.reduce((sum, r) => sum + r.duration, 0);

  console.log('\n' + '='.repeat(50));
  console.log('📊 Data Migration Summary');
  console.log('='.repeat(50));
  console.log(`   Tables: ${successful} success, ${failed} failed, ${skipped} skipped`);
  console.log(`   Rows migrated: ${totalMigrated.toLocaleString()}`);
  console.log(`   Total duration: ${(totalDuration / 1000).toFixed(2)}s`);
  console.log('='.repeat(50) + '\n');

  return results;
}

// Export data to SQL INSERT statements
export async function exportDataToSQL(): Promise<string> {
  const tables = await getTablesForDataMigration();
  const lines: string[] = [
    '-- ============================================',
    '-- DATA EXPORT',
    `-- Generated: ${new Date().toISOString()}`,
    '-- ============================================',
    '',
    '-- Disable triggers for faster import',
    'SET session_replication_role = replica;',
    '',
  ];

  for (const table of tables) {
    if (table.rowCount === 0) continue;

    const tableName = `"${table.schema}"."${table.name}"`;
    lines.push(`-- Table: ${tableName} (${table.rowCount} rows)`);
    lines.push(`TRUNCATE ${tableName} CASCADE;`);

    // Fetch all data
    const columns = table.columns.map(c => `"${c}"`).join(', ');
    const orderBy = table.primaryKey ? `ORDER BY "${table.primaryKey}"` : '';

    let offset = 0;
    const batchSize = config.batchSize;

    while (offset < table.rowCount) {
      const result = await sourcePool.query(`
        SELECT ${columns}
        FROM ${tableName}
        ${orderBy}
        LIMIT ${batchSize} OFFSET ${offset}
      `);

      if (result.rows.length === 0) break;

      for (const row of result.rows) {
        const values = table.columns.map(col => {
          const val = row[col];
          if (val === null) return 'NULL';
          if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
          if (typeof val === 'number') return String(val);
          if (val instanceof Date) return `'${val.toISOString()}'`;
          if (typeof val === 'object') return `'${JSON.stringify(val).replace(/'/g, "''")}'`;
          return `'${String(val).replace(/'/g, "''")}'`;
        });
        lines.push(`INSERT INTO ${tableName} (${columns}) VALUES (${values.join(', ')});`);
      }

      offset += batchSize;
      process.stdout.write(`\r   📊 ${table.schema}.${table.name}: ${Math.min(offset, table.rowCount)}/${table.rowCount}`);
    }

    lines.push('');
    process.stdout.write('\n');
  }

  lines.push('-- Re-enable triggers');
  lines.push('SET session_replication_role = DEFAULT;');
  lines.push('');

  return lines.join('\n');
}

// Verify data migration
export async function verifyDataMigration(): Promise<void> {
  console.log('🔍 Verifying data migration...\n');

  const tables = await getTablesForDataMigration();
  let allMatch = true;

  for (const table of tables) {
    // Count rows in target
    const targetResult = await targetPool.query(
      `SELECT COUNT(*) as count FROM "${table.schema}"."${table.name}"`
    );
    const targetCount = parseInt(targetResult.rows[0].count, 10);

    if (targetCount === table.rowCount) {
      console.log(`   ✅ ${table.schema}.${table.name}: ${targetCount} rows (match)`);
    } else {
      console.log(`   ❌ ${table.schema}.${table.name}: ${targetCount}/${table.rowCount} rows (mismatch)`);
      allMatch = false;
    }
  }

  if (allMatch) {
    console.log('\n✅ All table row counts match!\n');
  } else {
    console.log('\n⚠️  Some tables have row count mismatches.\n');
  }
}
