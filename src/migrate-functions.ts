import { sourcePool, targetPool, config } from './config.js';

interface FunctionInfo {
  schema: string;
  name: string;
  args: string;
  returnType: string;
  definition: string;
  language: string;
  volatility: string;
  isStrict: boolean;
  securityDefiner: boolean;
}

interface TriggerInfo {
  schema: string;
  table: string;
  name: string;
  timing: string;
  events: string;
  functionSchema: string;
  functionName: string;
  definition: string;
}

// Get user-defined functions
export async function getFunctions(): Promise<FunctionInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      n.nspname as schema,
      p.proname as name,
      pg_get_function_arguments(p.oid) as args,
      pg_get_function_result(p.oid) as return_type,
      pg_get_functiondef(p.oid) as definition,
      l.lanname as language,
      CASE p.provolatile
        WHEN 'i' THEN 'IMMUTABLE'
        WHEN 's' THEN 'STABLE'
        WHEN 'v' THEN 'VOLATILE'
      END as volatility,
      p.proisstrict as is_strict,
      p.prosecdef as security_definer
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    JOIN pg_language l ON p.prolang = l.oid
    WHERE n.nspname IN (${schemaList})
      AND p.prokind = 'f'  -- 'f' = ordinary function (excludes aggregates, procedures, window functions)
      AND l.lanname != 'c'
    ORDER BY n.nspname, p.proname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    name: row.name,
    args: row.args,
    returnType: row.return_type,
    definition: row.definition,
    language: row.language,
    volatility: row.volatility,
    isStrict: row.is_strict,
    securityDefiner: row.security_definer,
  }));
}

// Get triggers
export async function getTriggers(): Promise<TriggerInfo[]> {
  const schemaList = config.schemas.map(s => `'${s}'`).join(',');

  const result = await sourcePool.query(`
    SELECT
      n.nspname as schema,
      c.relname as table,
      t.tgname as name,
      CASE
        WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
        WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
        ELSE 'AFTER'
      END as timing,
      array_to_string(ARRAY[
        CASE WHEN t.tgtype & 4 = 4 THEN 'INSERT' END,
        CASE WHEN t.tgtype & 8 = 8 THEN 'DELETE' END,
        CASE WHEN t.tgtype & 16 = 16 THEN 'UPDATE' END,
        CASE WHEN t.tgtype & 32 = 32 THEN 'TRUNCATE' END
      ]::text[], ' OR ') as events,
      pn.nspname as function_schema,
      p.proname as function_name,
      pg_get_triggerdef(t.oid) as definition
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_proc p ON t.tgfoid = p.oid
    JOIN pg_namespace pn ON p.pronamespace = pn.oid
    WHERE n.nspname IN (${schemaList})
      AND NOT t.tgisinternal
    ORDER BY n.nspname, c.relname, t.tgname
  `);

  return result.rows.map(row => ({
    schema: row.schema,
    table: row.table,
    name: row.name,
    timing: row.timing,
    events: row.events,
    functionSchema: row.function_schema,
    functionName: row.function_name,
    definition: row.definition,
  }));
}

// Main function migration
export async function migrateFunctions(): Promise<void> {
  console.log('⚡ Starting function migration...\n');
  const targetClient = await targetPool.connect();

  try {
    const functions = await getFunctions();
    console.log(`📦 Found ${functions.length} functions to migrate\n`);

    for (const func of functions) {
      try {
        // Drop existing function first (to handle signature changes)
        const strippedArgs = stripDefaultValues(func.args);
        const dropSql = `DROP FUNCTION IF EXISTS "${func.schema}"."${func.name}"(${strippedArgs}) CASCADE`;
        try {
          await targetClient.query(dropSql);
        } catch {
          // DROP failed, but CREATE OR REPLACE should still work
        }

        // Create function
        await targetClient.query(func.definition);
        console.log(`   ✅ Function: ${func.schema}.${func.name}(${func.args.substring(0, 30)}...)`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ❌ Function ${func.schema}.${func.name}: ${message}`);
      }
    }

    console.log(`\n✅ Function migration completed! (${functions.length} functions)\n`);
  } finally {
    targetClient.release();
  }
}

// Main trigger migration
export async function migrateTriggers(): Promise<void> {
  console.log('🎯 Starting trigger migration...\n');
  const targetClient = await targetPool.connect();

  try {
    const triggers = await getTriggers();
    console.log(`📦 Found ${triggers.length} triggers to migrate\n`);

    for (const trigger of triggers) {
      try {
        // Drop existing trigger
        await targetClient.query(`DROP TRIGGER IF EXISTS "${trigger.name}" ON "${trigger.schema}"."${trigger.table}"`);

        // Create trigger using the original definition
        await targetClient.query(trigger.definition);
        console.log(`   ✅ Trigger: ${trigger.schema}.${trigger.table}.${trigger.name}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`   ❌ Trigger ${trigger.name}: ${message}`);
      }
    }

    console.log(`\n✅ Trigger migration completed! (${triggers.length} triggers)\n`);
  } finally {
    targetClient.release();
  }
}

// Remove DEFAULT values from function arguments (for DROP FUNCTION)
function stripDefaultValues(args: string): string {
  // Remove DEFAULT ... from each argument
  // e.g., "prefix text, limits int DEFAULT 100" -> "prefix text, limits int"
  return args
    .split(',')
    .map(arg => arg.replace(/\s+DEFAULT\s+.*/i, '').trim())
    .join(', ');
}

// Export functions to SQL
export async function exportFunctionsToSQL(): Promise<string> {
  const functions = await getFunctions();
  const triggers = await getTriggers();

  let sql = '-- Functions and Triggers Migration Script\n';
  sql += '-- Generated by supabase-migration\n\n';

  // Functions
  sql += '-- ========== FUNCTIONS ==========\n\n';
  for (const func of functions) {
    const dropArgs = stripDefaultValues(func.args);
    sql += `-- Function: ${func.schema}.${func.name}\n`;
    sql += `DROP FUNCTION IF EXISTS "${func.schema}"."${func.name}"(${dropArgs}) CASCADE;\n`;
    sql += func.definition + ';\n\n';
  }

  // Triggers
  sql += '\n-- ========== TRIGGERS ==========\n\n';
  for (const trigger of triggers) {
    sql += `-- Trigger: ${trigger.schema}.${trigger.table}.${trigger.name}\n`;
    sql += `DROP TRIGGER IF EXISTS "${trigger.name}" ON "${trigger.schema}"."${trigger.table}";\n`;
    sql += trigger.definition + ';\n\n';
  }

  return sql;
}
