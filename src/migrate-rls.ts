import { sourcePool, targetPool, config } from './config.js';

interface RLSPolicy {
  schema: string;
  table: string;
  name: string;
  permissive: string;
  roles: string[];
  cmd: string;
  qual: string | null;
  withCheck: string | null;
}

interface TableRLS {
  schema: string;
  table: string;
  rlsEnabled: boolean;
  rlsForced: boolean;
}

// Get RLS status for tables
export async function getTableRLSStatus(): Promise<TableRLS[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      n.nspname as schema,
      c.relname as table,
      c.relrowsecurity as rls_enabled,
      c.relforcerowsecurity as rls_forced
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname IN (${schemaList})
      AND c.relkind = 'r'
    ORDER BY n.nspname, c.relname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    table: row.table,
    rlsEnabled: row.rls_enabled,
    rlsForced: row.rls_forced,
  }));
}

// Get all RLS policies
export async function getRLSPolicies(): Promise<RLSPolicy[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      n.nspname as schema,
      c.relname as table,
      p.polname as name,
      CASE p.polpermissive WHEN true THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END as permissive,
      CASE p.polroles
        WHEN '{0}' THEN ARRAY['public']
        ELSE ARRAY(SELECT rolname FROM pg_roles WHERE oid = ANY(p.polroles))
      END as roles,
      CASE p.polcmd
        WHEN 'r' THEN 'SELECT'
        WHEN 'a' THEN 'INSERT'
        WHEN 'w' THEN 'UPDATE'
        WHEN 'd' THEN 'DELETE'
        WHEN '*' THEN 'ALL'
      END as cmd,
      pg_get_expr(p.polqual, p.polrelid) as qual,
      pg_get_expr(p.polwithcheck, p.polrelid) as with_check
    FROM pg_policy p
    JOIN pg_class c ON p.polrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname IN (${schemaList})
    ORDER BY n.nspname, c.relname, p.polname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    table: row.table,
    name: row.name,
    permissive: row.permissive,
    // Handle both array and string format (pg returns {a,b,c} string via some drivers)
    roles: Array.isArray(row.roles)
      ? row.roles
      : row.roles.replace(/^\{|\}$/g, '').split(',').filter((r: string) => r),
    cmd: row.cmd,
    qual: row.qual,
    withCheck: row.with_check,
  }));
}

// Generate CREATE POLICY statement
function generatePolicySQL(policy: RLSPolicy): string {
  const roles = policy.roles.map(r => r === 'public' ? 'public' : `"${r}"`).join(', ');

  let sql = `CREATE POLICY "${policy.name}" ON "${policy.schema}"."${policy.table}"`;
  sql += `\n  AS ${policy.permissive}`;
  sql += `\n  FOR ${policy.cmd}`;
  sql += `\n  TO ${roles}`;

  if (policy.qual) {
    sql += `\n  USING (${policy.qual})`;
  }

  if (policy.withCheck) {
    sql += `\n  WITH CHECK (${policy.withCheck})`;
  }

  return sql;
}

// Main RLS migration function
export async function migrateRLS(): Promise<void> {
  console.log('🔒 Starting RLS policy migration...\n');
  const targetClient = await targetPool.connect();

  try {
    // 1. Enable RLS on tables
    console.log('🔐 Enabling RLS on tables...');
    const tableRLS = await getTableRLSStatus();

    for (const table of tableRLS) {
      if (table.rlsEnabled) {
        try {
          await targetClient.query(`ALTER TABLE "${table.schema}"."${table.table}" ENABLE ROW LEVEL SECURITY`);
          console.log(`   ✅ RLS enabled: ${table.schema}.${table.table}`);

          if (table.rlsForced) {
            await targetClient.query(`ALTER TABLE "${table.schema}"."${table.table}" FORCE ROW LEVEL SECURITY`);
            console.log(`   ✅ RLS forced: ${table.schema}.${table.table}`);
          }
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error);
          console.log(`   ⚠️  RLS ${table.schema}.${table.table}: ${message}`);
        }
      }
    }

    // 2. Create policies
    console.log('\n📜 Creating RLS policies...');
    const policies = await getRLSPolicies();

    for (const policy of policies) {
      const sql = generatePolicySQL(policy);
      try {
        // Drop existing policy if exists
        await targetClient.query(`DROP POLICY IF EXISTS "${policy.name}" ON "${policy.schema}"."${policy.table}"`);
        await targetClient.query(sql);
        console.log(`   ✅ Policy: ${policy.schema}.${policy.table}.${policy.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ❌ Policy ${policy.name}: ${message}`);
        console.log(`      SQL: ${sql.split('\n')[0]}...`);
      }
    }

    console.log(`\n✅ RLS migration completed! (${policies.length} policies)\n`);
  } finally {
    targetClient.release();
  }
}

// Export policies to SQL file (for manual review)
export async function exportRLSToSQL(): Promise<string> {
  const tableRLS = await getTableRLSStatus();
  const policies = await getRLSPolicies();

  let sql = '-- RLS Migration Script\n';
  sql += '-- Generated by supabase-migration\n\n';

  // Enable RLS statements
  sql += '-- Enable RLS\n';
  for (const table of tableRLS.filter(t => t.rlsEnabled)) {
    sql += `ALTER TABLE "${table.schema}"."${table.table}" ENABLE ROW LEVEL SECURITY;\n`;
    if (table.rlsForced) {
      sql += `ALTER TABLE "${table.schema}"."${table.table}" FORCE ROW LEVEL SECURITY;\n`;
    }
  }

  // Policy statements
  sql += '\n-- Policies\n';
  for (const policy of policies) {
    sql += `\n-- Policy: ${policy.schema}.${policy.table}.${policy.name}\n`;
    sql += `DROP POLICY IF EXISTS "${policy.name}" ON "${policy.schema}"."${policy.table}";\n`;
    sql += generatePolicySQL(policy) + ';\n';
  }

  return sql;
}
