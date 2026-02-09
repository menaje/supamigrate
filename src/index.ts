#!/usr/bin/env node
import { testConnections, closePools, config, initPools, getTargetPool } from './config.js';
import { migrateSchema, migrateGrants, exportSchemaToSQL, exportGrantsToSQL } from './migrate-schema.js';
import { migrateRLS, exportRLSToSQL } from './migrate-rls.js';
import { migrateFunctions, migrateTriggers, exportFunctionsToSQL } from './migrate-functions.js';
import { migrateData, verifyDataMigration, exportDataToSQL } from './migrate-data.js';
import { migrateStorage, verifyStorageMigration } from './migrate-storage.js';
import fs from 'fs/promises';

type MigrationStep = 'schema' | 'functions' | 'triggers' | 'data' | 'rls' | 'grants' | 'storage' | 'verify';

interface MigrationOptions {
  steps: MigrationStep[];
  exportSql: boolean;
  applySql: string | null;  // SQL file path to apply
  dryRun: boolean;
}

function parseArgs(): MigrationOptions {
  const args = process.argv.slice(2);

  const options: MigrationOptions = {
    steps: [],
    exportSql: false,
    applySql: null,
    dryRun: false,
  };

  // Default: run all steps
  const allSteps: MigrationStep[] = ['schema', 'functions', 'triggers', 'data', 'rls', 'grants', 'storage', 'verify'];

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case '--schema':
        options.steps.push('schema');
        break;
      case '--functions':
        options.steps.push('functions');
        break;
      case '--triggers':
        options.steps.push('triggers');
        break;
      case '--data':
        options.steps.push('data');
        break;
      case '--rls':
        options.steps.push('rls');
        break;
      case '--grants':
        options.steps.push('grants');
        break;
      case '--storage':
        options.steps.push('storage');
        break;
      case '--verify':
        options.steps.push('verify');
        break;
      case '--export-sql':
        options.exportSql = true;
        break;
      case '--apply-sql':
        // Next argument is the file path, or use default
        if (args[i + 1] && !args[i + 1].startsWith('--')) {
          options.applySql = args[++i];
        } else {
          options.applySql = 'migration-complete.sql';
        }
        break;
      case '--dry-run':
        options.dryRun = true;
        break;
      case '--all':
        options.steps = [...allSteps];
        break;
      case '--help':
        printHelp();
        process.exit(0);
    }
  }

  // Default to all steps if none specified
  if (options.steps.length === 0) {
    options.steps = [...allSteps];
  }

  return options;
}

function printHelp(): void {
  console.log(`
╔═══════════════════════════════════════════════════════════════╗
║         Supabase Migration Tool - Cloud to Self-hosted        ║
╚═══════════════════════════════════════════════════════════════╝

Usage: npm run migrate [options]

Options:
  --schema      Migrate database schema (tables, indexes, constraints, views)
  --functions   Migrate stored functions
  --triggers    Migrate triggers
  --data        Migrate table data
  --rls         Migrate RLS policies
  --grants      Migrate role grants (anon, authenticated, service_role)
  --storage     Migrate storage buckets and files (requires Supabase API keys)
  --verify      Verify data migration
  --all         Run all migration steps (default)
  --export-sql  Export SQL files (schema only, no data)
  --apply-sql [file]  Apply SQL file to target DB (default: migration-complete.sql)
  --dry-run     Show what would be migrated without executing
  --help        Show this help message

Examples:
  npm run migrate                    # Run full migration
  npm run migrate --schema --data    # Only schema and data
  npm run migrate --export-sql       # Export SQL files for review
  npm run migrate --apply-sql        # Apply migration-complete.sql to target
  npm run migrate --apply-sql migration-schema.sql  # Apply specific file
  npm run migrate --verify           # Verify existing migration

Environment Variables (set in .env):

  # Recommended: Connection String format (6 variables total)
  SOURCE_DB_URL               PostgreSQL connection string for Supabase Cloud
                              e.g., postgresql://postgres.xxx:password@pooler.supabase.com:6543/postgres
  SOURCE_SUPABASE_URL         Supabase Cloud project URL (for Storage)
                              e.g., https://xxx.supabase.co
  SOURCE_SUPABASE_SERVICE_KEY Supabase Cloud service role key

  TARGET_DB_URL               PostgreSQL connection string for self-hosted
                              e.g., postgresql://postgres:password@localhost:54322/postgres
  TARGET_SUPABASE_URL         Self-hosted Supabase URL (for Storage)
                              e.g., http://localhost:8000
  TARGET_SUPABASE_SERVICE_KEY Self-hosted service role key

  # Alternative: Individual parameters (legacy, fallback)
  SOURCE_DB_HOST, SOURCE_DB_PORT, SOURCE_DB_NAME, SOURCE_DB_USER, SOURCE_DB_PASSWORD
  TARGET_DB_HOST, TARGET_DB_PORT, TARGET_DB_NAME, TARGET_DB_USER, TARGET_DB_PASSWORD

  # Migration Options
  MIGRATE_SCHEMAS      Comma-separated schemas (default: public)
  BATCH_SIZE           Data migration batch size (default: 1000)
`);
}

// Parse SQL into statements, handling dollar-quoted blocks correctly
function parseSQLStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = '';
  let dollarQuoteTag: string | null = null;  // Track which $tag$ we're in
  const lines = sql.split('\n');

  for (const line of lines) {
    const trimmed = line.trim();

    // Skip empty lines and comments at statement start
    if (!current && (!trimmed || trimmed.startsWith('--'))) {
      continue;
    }

    current += line + '\n';

    // Track dollar-quoted strings ($$ or $function$ or $tag$)
    // Match all dollar quote tags in the line
    const dollarQuoteRegex = /\$([a-zA-Z_]*)\$/g;
    let match;
    while ((match = dollarQuoteRegex.exec(line)) !== null) {
      const tag = match[0];  // e.g., "$$" or "$function$"
      if (dollarQuoteTag === null) {
        // Start of dollar-quoted string
        dollarQuoteTag = tag;
      } else if (dollarQuoteTag === tag) {
        // End of dollar-quoted string (matching tag)
        dollarQuoteTag = null;
      }
      // If tags don't match, we're still inside the original block
    }

    // If not in dollar-quoted block and line ends with semicolon, statement complete
    if (dollarQuoteTag === null && trimmed.endsWith(';')) {
      const stmt = current.trim();
      if (stmt && !stmt.startsWith('--')) {
        statements.push(stmt.slice(0, -1)); // Remove trailing semicolon
      }
      current = '';
    }
  }

  // Handle any remaining statement
  if (current.trim()) {
    statements.push(current.trim());
  }

  return statements;
}

async function applySQL(filePath: string): Promise<void> {
  console.log(`📥 Applying SQL file: ${filePath}\n`);

  // Check if file exists
  try {
    await fs.access(filePath);
  } catch {
    throw new Error(`File not found: ${filePath}`);
  }

  // Read SQL file
  const sql = await fs.readFile(filePath, 'utf-8');
  const statements = parseSQLStatements(sql);

  console.log(`   📄 Found ${statements.length} SQL statements\n`);

  const tgtPool = await getTargetPool();
  const targetClient = await tgtPool.connect();
  let success = 0;
  let failed = 0;

  try {
    for (let i = 0; i < statements.length; i++) {
      const stmt = statements[i];
      // Skip empty statements or comments
      if (!stmt || stmt.startsWith('--')) continue;

      try {
        await targetClient.query(stmt);
        success++;
        // Show progress every 10 statements
        if ((success + failed) % 10 === 0) {
          process.stdout.write(`   ⏳ Progress: ${success + failed}/${statements.length}\r`);
        }
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        // Skip "already exists" errors
        if (message.includes('already exists') || message.includes('duplicate')) {
          success++;
        } else {
          failed++;
          // Show first part of failed statement
          const preview = stmt.substring(0, 60).replace(/\n/g, ' ');
          console.log(`   ⚠️  ${preview}...`);
          console.log(`      Error: ${message}\n`);
        }
      }
    }

    console.log(`\n✅ SQL applied!`);
    console.log(`   Success: ${success}`);
    console.log(`   Failed: ${failed}\n`);
  } finally {
    targetClient.release();
  }
}

async function exportSQL(): Promise<void> {
  console.log('📄 Exporting SQL files...\n');

  // Export Schema (tables, indexes, constraints, views, enums, sequences)
  console.log('   📋 Exporting schema...');
  const schemaSQL = await exportSchemaToSQL();
  await fs.writeFile('migration-schema.sql', schemaSQL);
  console.log('   ✅ migration-schema.sql');

  // Export Functions and Triggers
  console.log('   ⚡ Exporting functions and triggers...');
  const functionsSQL = await exportFunctionsToSQL();
  await fs.writeFile('migration-functions.sql', functionsSQL);
  console.log('   ✅ migration-functions.sql');

  // Export RLS policies
  console.log('   🔒 Exporting RLS policies...');
  const rlsSQL = await exportRLSToSQL();
  await fs.writeFile('migration-rls.sql', rlsSQL);
  console.log('   ✅ migration-rls.sql');

  // Export Grants
  console.log('   🔑 Exporting grants...');
  const grantsSQL = await exportGrantsToSQL();
  await fs.writeFile('migration-grants.sql', grantsSQL);
  console.log('   ✅ migration-grants.sql');

  // Export Data
  console.log('   📦 Exporting data...');
  const dataSQL = await exportDataToSQL();
  await fs.writeFile('migration-data.sql', dataSQL);
  console.log('   ✅ migration-data.sql');

  // Create combined file (schema only)
  console.log('   📦 Creating combined file (schema only)...');
  const combinedSQL = [
    '-- ============================================',
    '-- COMPLETE SCHEMA MIGRATION SCRIPT',
    '-- Generated by supabase-migration',
    `-- Date: ${new Date().toISOString()}`,
    '-- ============================================\n',
    schemaSQL,
    '\n-- ============================================\n',
    functionsSQL,
    '\n-- ============================================\n',
    rlsSQL,
    '\n-- ============================================\n',
    grantsSQL,
  ].join('\n');
  await fs.writeFile('migration-complete.sql', combinedSQL);
  console.log('   ✅ migration-complete.sql');

  // Create combined file with data
  console.log('   📦 Creating combined file (with data)...');
  const combinedWithDataSQL = [
    '-- ============================================',
    '-- COMPLETE MIGRATION SCRIPT WITH DATA',
    '-- Generated by supabase-migration',
    `-- Date: ${new Date().toISOString()}`,
    '-- ============================================\n',
    schemaSQL,
    '\n-- ============================================\n',
    functionsSQL,
    '\n-- ============================================\n',
    dataSQL,
    '\n-- ============================================\n',
    rlsSQL,
    '\n-- ============================================\n',
    grantsSQL,
  ].join('\n');
  await fs.writeFile('migration-complete-with-data.sql', combinedWithDataSQL);
  console.log('   ✅ migration-complete-with-data.sql');

  console.log('\n✅ SQL files exported!\n');
  console.log('📁 Generated files:');
  console.log('   - migration-schema.sql              (tables, indexes, constraints, views, enums, sequences)');
  console.log('   - migration-functions.sql           (functions, triggers)');
  console.log('   - migration-rls.sql                 (RLS policies)');
  console.log('   - migration-grants.sql              (role permissions)');
  console.log('   - migration-data.sql                (all table data)');
  console.log('   - migration-complete.sql            (schema only, no data)');
  console.log('   - migration-complete-with-data.sql  (all combined with data)\n');
}

async function main(): Promise<void> {
  const options = parseArgs();

  console.log(`
╔═══════════════════════════════════════════════════════════════╗
║         Supabase Migration Tool - Cloud to Self-hosted        ║
╚═══════════════════════════════════════════════════════════════╝
`);

  console.log('📋 Configuration:');
  console.log(`   Schemas: ${config.schemas.join(', ')}`);
  console.log(`   Batch size: ${config.batchSize}`);
  console.log(`   Steps: ${options.steps.join(', ')}`);
  const mode = options.dryRun ? 'DRY RUN'
    : options.exportSql ? 'EXPORT SQL'
    : options.applySql ? `APPLY SQL (${options.applySql})`
    : 'LIVE MIGRATION';
  console.log(`   Mode: ${mode}`);
  console.log('');

  try {
    // Test connections (sourceOnly for export mode)
    await testConnections(options.exportSql);

    // Initialize legacy pool exports for migration modules
    await initPools();

    if (options.exportSql) {
      await exportSQL();
      return;
    }

    if (options.applySql) {
      await applySQL(options.applySql);
      return;
    }

    if (options.dryRun) {
      console.log('🔍 Dry run mode - showing what would be migrated:\n');
      // TODO: Add dry run logic to show migration plan
      console.log('   (Dry run not fully implemented yet)\n');
      return;
    }

    // Run migration steps in order
    const startTime = Date.now();

    if (options.steps.includes('schema')) {
      await migrateSchema();
    }

    if (options.steps.includes('functions')) {
      await migrateFunctions();
    }

    if (options.steps.includes('triggers')) {
      await migrateTriggers();
    }

    if (options.steps.includes('data')) {
      await migrateData();
    }

    if (options.steps.includes('rls')) {
      await migrateRLS();
    }

    if (options.steps.includes('grants')) {
      await migrateGrants();
    }

    if (options.steps.includes('storage')) {
      await migrateStorage();
    }

    if (options.steps.includes('verify')) {
      await verifyDataMigration();
      if (options.steps.includes('storage')) {
        await verifyStorageMigration();
      }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n🎉 Migration completed in ${duration}s!\n`);
  } catch (error) {
    console.error('\n❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await closePools();
  }
}

main();
