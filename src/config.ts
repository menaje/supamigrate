import 'dotenv/config';
import pg from 'pg';

const { Pool } = pg;

export interface MigrationConfig {
  schemas: string[];
  batchSize: number;
  excludeTables: string[];
  excludeSchemas: string[];
  // Supabase API config (for Storage migration)
  source: {
    supabaseUrl?: string;
    supabaseKey?: string;
  };
  target: {
    supabaseUrl?: string;
    supabaseKey?: string;
  };
}

// Extract project ref from Supabase URL
function extractProjectRef(supabaseUrl?: string): string | null {
  if (!supabaseUrl) return null;
  // https://xxx.supabase.co -> xxx
  const match = supabaseUrl.match(/https?:\/\/([^.]+)\.supabase\.co/);
  return match ? match[1] : null;
}

// Extract project ref from Service Key JWT
function extractRefFromJwt(serviceKey?: string): string | null {
  if (!serviceKey) return null;
  try {
    // JWT format: header.payload.signature
    const parts = serviceKey.split('.');
    if (parts.length !== 3) return null;

    // Decode payload (base64url)
    const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const decoded = Buffer.from(payload, 'base64').toString('utf-8');
    const json = JSON.parse(decoded);

    return json.ref || null;
  } catch {
    return null;
  }
}

// Supabase Cloud pooler regions (aws-0 and aws-1 variants)
const POOLER_REGIONS = [
  // aws-1 variants (newer)
  'aws-1-ap-northeast-2',  // Seoul
  'aws-1-us-east-1',       // N. Virginia
  'aws-1-us-west-1',       // N. California
  'aws-1-eu-west-1',       // Ireland
  'aws-1-eu-central-1',    // Frankfurt
  'aws-1-ap-southeast-1',  // Singapore
  'aws-1-ap-northeast-1',  // Tokyo
  // aws-0 variants (older)
  'aws-0-ap-northeast-2',  // Seoul
  'aws-0-us-east-1',       // N. Virginia
  'aws-0-us-west-1',       // N. California
  'aws-0-eu-west-1',       // Ireland
  'aws-0-eu-central-1',    // Frankfurt
  'aws-0-ap-southeast-1',  // Singapore
  'aws-0-ap-northeast-1',  // Tokyo
];

// Build DB connection string from Supabase credentials
async function buildSupabaseDbUrl(
  supabaseUrl?: string,
  serviceKey?: string,
  dbPassword?: string,
  explicitDbUrl?: string
): Promise<string | null> {
  // Priority 1: Explicit DB URL
  if (explicitDbUrl) {
    return explicitDbUrl;
  }

  // Priority 2: Build from Supabase credentials
  if (!dbPassword) {
    console.log('   ⚠️  DB_PASSWORD not set, cannot auto-detect connection');
    return null;
  }

  // Get project ref from URL or JWT
  const refFromUrl = extractProjectRef(supabaseUrl);
  const refFromJwt = extractRefFromJwt(serviceKey);
  const projectRef = refFromUrl || refFromJwt;

  if (!projectRef) {
    console.log('   ⚠️  Could not extract project ref from URL or JWT');
    return null;
  }

  console.log(`   🔍 Project ref: ${projectRef}`);
  console.log(`   🔍 Detecting region...`);

  // Try to detect region by testing connections
  for (const region of POOLER_REGIONS) {
    const poolerHost = `${region}.pooler.supabase.com`;
    const testUrl = `postgresql://postgres.${projectRef}:${dbPassword}@${poolerHost}:6543/postgres`;

    try {
      const testPool = new Pool({
        connectionString: testUrl,
        ssl: { rejectUnauthorized: false },
        connectionTimeoutMillis: 5000,
      });
      const client = await testPool.connect();
      client.release();
      await testPool.end();

      console.log(`   ✅ Detected region: ${region}`);
      return testUrl;
    } catch (err) {
      // Try next region
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes('password') || message.includes('authentication')) {
        console.log(`   ❌ Region ${region}: authentication failed`);
      }
    }
  }

  console.log(`   ⚠️  No region detected, falling back to direct connection`);
  // Fallback: try direct connection
  const directHost = `db.${projectRef}.supabase.co`;
  return `postgresql://postgres:${dbPassword}@${directHost}:5432/postgres`;
}

// Default: only public schema (auth/storage are managed by Supabase)
export const config: MigrationConfig = {
  schemas: (process.env.MIGRATE_SCHEMAS || 'public').split(',').map(s => s.trim()).filter(s => s !== 'auth' && s !== 'storage'),
  batchSize: parseInt(process.env.BATCH_SIZE || '1000', 10),
  excludeTables: [
    // Supabase internal tables
    'schema_migrations',
    'supabase_migrations',
  ],
  excludeSchemas: [
    'pg_catalog',
    'information_schema',
    'pg_toast',
    'pg_temp_1',
    'pg_toast_temp_1',
    'supabase_migrations',
    'graphql',
    'graphql_public',
    'realtime',
    'pgsodium',
    'pgsodium_masks',
    'vault',
    '_realtime',
    'net',
    'supabase_functions',
  ],
  // Supabase API config
  source: {
    supabaseUrl: process.env.SOURCE_SUPABASE_URL,
    supabaseKey: process.env.SOURCE_SUPABASE_SERVICE_KEY,
  },
  target: {
    supabaseUrl: process.env.TARGET_SUPABASE_URL,
    supabaseKey: process.env.TARGET_SUPABASE_SERVICE_KEY,
  },
};

// Lazy-initialized pools (to allow async region detection)
let _sourcePool: pg.Pool | null = null;
let _targetPool: pg.Pool | null = null;

// Create pool with fallback options
function createPoolSync(
  connectionString: string | undefined,
  fallback: {
    host?: string;
    port: number;
    database: string;
    user: string;
    password?: string;
  },
  ssl: boolean | { rejectUnauthorized: boolean }
): pg.Pool {
  if (connectionString) {
    return new Pool({
      connectionString,
      ssl,
    });
  }
  return new Pool({
    host: fallback.host,
    port: fallback.port,
    database: fallback.database,
    user: fallback.user,
    password: fallback.password,
    ssl,
  });
}

// Get or create source pool
export async function getSourcePool(): Promise<pg.Pool> {
  if (_sourcePool) return _sourcePool;

  // Try to build URL from Supabase credentials
  const dbUrl = await buildSupabaseDbUrl(
    process.env.SOURCE_SUPABASE_URL,
    process.env.SOURCE_SUPABASE_SERVICE_KEY,
    process.env.SOURCE_DB_PASSWORD,
    process.env.SOURCE_DB_URL
  );

  if (dbUrl) {
    _sourcePool = new Pool({
      connectionString: dbUrl,
      ssl: { rejectUnauthorized: false },
    });
  } else {
    // Fallback to individual params
    _sourcePool = createPoolSync(
      undefined,
      {
        host: process.env.SOURCE_DB_HOST,
        port: parseInt(process.env.SOURCE_DB_PORT || '5432', 10),
        database: process.env.SOURCE_DB_NAME || 'postgres',
        user: process.env.SOURCE_DB_USER || 'postgres',
        password: process.env.SOURCE_DB_PASSWORD,
      },
      { rejectUnauthorized: false }
    );
  }

  return _sourcePool;
}

// Get or create target pool
export async function getTargetPool(): Promise<pg.Pool> {
  if (_targetPool) return _targetPool;

  // For self-hosted, check if it's localhost or Supabase Cloud
  const targetUrl = process.env.TARGET_SUPABASE_URL || '';
  const isLocalhost = targetUrl.includes('localhost') || targetUrl.includes('127.0.0.1');

  if (isLocalhost) {
    // Self-hosted: use direct connection
    _targetPool = createPoolSync(
      process.env.TARGET_DB_URL,
      {
        host: process.env.TARGET_DB_HOST || 'localhost',
        port: parseInt(process.env.TARGET_DB_PORT || '54322', 10),
        database: process.env.TARGET_DB_NAME || 'postgres',
        user: process.env.TARGET_DB_USER || 'postgres',
        password: process.env.TARGET_DB_PASSWORD,
      },
      process.env.TARGET_DB_SSL === 'true' ? { rejectUnauthorized: false } : false
    );
  } else {
    // Another Supabase Cloud instance
    const dbUrl = await buildSupabaseDbUrl(
      process.env.TARGET_SUPABASE_URL,
      process.env.TARGET_SUPABASE_SERVICE_KEY,
      process.env.TARGET_DB_PASSWORD,
      process.env.TARGET_DB_URL
    );

    if (dbUrl) {
      _targetPool = new Pool({
        connectionString: dbUrl,
        ssl: { rejectUnauthorized: false },
      });
    } else {
      _targetPool = createPoolSync(
        undefined,
        {
          host: process.env.TARGET_DB_HOST || 'localhost',
          port: parseInt(process.env.TARGET_DB_PORT || '54322', 10),
          database: process.env.TARGET_DB_NAME || 'postgres',
          user: process.env.TARGET_DB_USER || 'postgres',
          password: process.env.TARGET_DB_PASSWORD,
        },
        false
      );
    }
  }

  return _targetPool;
}

// Legacy exports - these will be initialized by initPools()
export let sourcePool: pg.Pool;
export let targetPool: pg.Pool;

// Initialize pools (must be called before using sourcePool/targetPool)
export async function initPools(): Promise<void> {
  sourcePool = await getSourcePool();
  targetPool = await getTargetPool();
}

export async function testConnections(sourceOnly: boolean = false): Promise<void> {
  console.log('🔌 Testing database connections...\n');

  // Use async pool getters for auto-detection
  const srcPool = await getSourcePool();

  try {
    const sourceClient = await srcPool.connect();
    const sourceVersion = await sourceClient.query('SELECT version()');
    console.log('✅ Source DB connected');
    console.log(`   ${sourceVersion.rows[0].version.split(',')[0]}\n`);
    sourceClient.release();
  } catch (error) {
    throw new Error(`❌ Source DB connection failed: ${error}`);
  }

  if (!sourceOnly) {
    const tgtPool = await getTargetPool();
    try {
      const targetClient = await tgtPool.connect();
      const targetVersion = await targetClient.query('SELECT version()');
      console.log('✅ Target DB connected');
      console.log(`   ${targetVersion.rows[0].version.split(',')[0]}\n`);
      targetClient.release();
    } catch (error) {
      throw new Error(`❌ Target DB connection failed: ${error}`);
    }
  }
}

export async function closePools(): Promise<void> {
  if (_sourcePool) await _sourcePool.end();
  if (_targetPool) await _targetPool.end();
  // Also close legacy pools if they were used
  try { await sourcePool.end(); } catch { /* ignore */ }
  try { await targetPool.end(); } catch { /* ignore */ }
}
