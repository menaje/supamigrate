import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { config } from './config.js';

interface BucketInfo {
  id: string;
  name: string;
  public: boolean;
  file_size_limit?: number | null;
  allowed_mime_types?: string[] | null;
}

interface FileInfo {
  name: string;
  bucket_id: string;
  metadata: Record<string, unknown>;
}

interface StorageMigrationStats {
  bucketsCreated: number;
  bucketsFailed: number;
  filesUploaded: number;
  filesFailed: number;
  totalBytes: number;
}

// Create Supabase clients for Storage API access
function createStorageClients(): { source: SupabaseClient; target: SupabaseClient } | null {
  const sourceUrl = config.source.supabaseUrl;
  const sourceKey = config.source.supabaseKey;
  const targetUrl = config.target.supabaseUrl;
  const targetKey = config.target.supabaseKey;

  if (!sourceUrl || !sourceKey) {
    console.log('   ⚠️  SOURCE_SUPABASE_URL and SOURCE_SUPABASE_SERVICE_KEY required for storage migration');
    return null;
  }

  if (!targetUrl || !targetKey) {
    console.log('   ⚠️  TARGET_SUPABASE_URL and TARGET_SUPABASE_SERVICE_KEY required for storage migration');
    return null;
  }

  const source = createClient(sourceUrl, sourceKey, {
    auth: { persistSession: false },
  });

  const target = createClient(targetUrl, targetKey, {
    auth: { persistSession: false },
  });

  return { source, target };
}

// List all buckets from source
async function listBuckets(client: SupabaseClient): Promise<BucketInfo[]> {
  const { data, error } = await client.storage.listBuckets();

  if (error) {
    console.log(`   ❌ Failed to list buckets: ${error.message}`);
    return [];
  }

  return data.map(bucket => ({
    id: bucket.id,
    name: bucket.name,
    public: bucket.public,
    file_size_limit: bucket.file_size_limit,
    allowed_mime_types: bucket.allowed_mime_types,
  }));
}

// List all files in a bucket
async function listAllFiles(
  client: SupabaseClient,
  bucketId: string,
  path: string = ''
): Promise<FileInfo[]> {
  const files: FileInfo[] = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const { data, error } = await client.storage
      .from(bucketId)
      .list(path, { limit, offset });

    if (error) {
      console.log(`   ⚠️  Error listing files in ${bucketId}/${path}: ${error.message}`);
      break;
    }

    if (!data || data.length === 0) break;

    for (const item of data) {
      if (item.id === null) {
        // It's a folder, recurse
        const subPath = path ? `${path}/${item.name}` : item.name;
        const subFiles = await listAllFiles(client, bucketId, subPath);
        files.push(...subFiles);
      } else {
        // It's a file
        files.push({
          name: path ? `${path}/${item.name}` : item.name,
          bucket_id: bucketId,
          metadata: item.metadata || {},
        });
      }
    }

    if (data.length < limit) break;
    offset += limit;
  }

  return files;
}

// Download file from source
async function downloadFile(
  client: SupabaseClient,
  bucketId: string,
  filePath: string
): Promise<Blob | null> {
  const { data, error } = await client.storage
    .from(bucketId)
    .download(filePath);

  if (error) {
    console.log(`   ❌ Failed to download ${bucketId}/${filePath}: ${error.message}`);
    return null;
  }

  return data;
}

// Upload file to target
async function uploadFile(
  client: SupabaseClient,
  bucketId: string,
  filePath: string,
  fileData: Blob
): Promise<boolean> {
  const { error } = await client.storage
    .from(bucketId)
    .upload(filePath, fileData, {
      upsert: true,
    });

  if (error) {
    console.log(`   ❌ Failed to upload ${bucketId}/${filePath}: ${error.message}`);
    return false;
  }

  return true;
}

// Create bucket in target
async function createBucket(
  client: SupabaseClient,
  bucket: BucketInfo
): Promise<boolean> {
  const { error } = await client.storage.createBucket(bucket.id, {
    public: bucket.public,
    fileSizeLimit: bucket.file_size_limit || undefined,
    allowedMimeTypes: bucket.allowed_mime_types || undefined,
  });

  if (error) {
    if (error.message.includes('already exists')) {
      console.log(`   ⏭️  Bucket ${bucket.name}: already exists`);
      return true;
    }
    console.log(`   ❌ Failed to create bucket ${bucket.name}: ${error.message}`);
    return false;
  }

  return true;
}

// Main storage migration function
export async function migrateStorage(): Promise<StorageMigrationStats> {
  console.log('📦 Starting storage migration...\n');

  const stats: StorageMigrationStats = {
    bucketsCreated: 0,
    bucketsFailed: 0,
    filesUploaded: 0,
    filesFailed: 0,
    totalBytes: 0,
  };

  const clients = createStorageClients();
  if (!clients) {
    console.log('\n⚠️  Storage migration skipped (missing credentials)\n');
    return stats;
  }

  const { source, target } = clients;

  try {
    // 1. List and create buckets
    console.log('🪣 Migrating buckets...');
    const buckets = await listBuckets(source);

    if (buckets.length === 0) {
      console.log('   ⏭️  No buckets to migrate\n');
      return stats;
    }

    console.log(`   📦 Found ${buckets.length} buckets\n`);

    for (const bucket of buckets) {
      console.log(`\n📁 Bucket: ${bucket.name} (${bucket.public ? 'public' : 'private'})`);

      // Create bucket in target
      const created = await createBucket(target, bucket);
      if (created) {
        stats.bucketsCreated++;
      } else {
        stats.bucketsFailed++;
        continue;
      }

      // 2. List and migrate files
      console.log(`   📄 Listing files...`);
      const files = await listAllFiles(source, bucket.id);
      console.log(`   📄 Found ${files.length} files`);

      for (const file of files) {
        process.stdout.write(`   📥 ${file.name}...`);

        // Download from source
        const fileData = await downloadFile(source, bucket.id, file.name);
        if (!fileData) {
          stats.filesFailed++;
          continue;
        }

        // Upload to target
        const uploaded = await uploadFile(target, bucket.id, file.name, fileData);
        if (uploaded) {
          stats.filesUploaded++;
          stats.totalBytes += fileData.size;
          process.stdout.write(` ✅ (${formatBytes(fileData.size)})\n`);
        } else {
          stats.filesFailed++;
        }
      }
    }

    // Summary
    console.log('\n' + '='.repeat(50));
    console.log('📊 Storage Migration Summary');
    console.log('='.repeat(50));
    console.log(`   Buckets: ${stats.bucketsCreated} created, ${stats.bucketsFailed} failed`);
    console.log(`   Files: ${stats.filesUploaded} uploaded, ${stats.filesFailed} failed`);
    console.log(`   Total size: ${formatBytes(stats.totalBytes)}`);
    console.log('='.repeat(50) + '\n');

  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`\n❌ Storage migration error: ${message}\n`);
  }

  return stats;
}

// Verify storage migration
export async function verifyStorageMigration(): Promise<void> {
  console.log('🔍 Verifying storage migration...\n');

  const clients = createStorageClients();
  if (!clients) {
    console.log('   ⚠️  Skipped (missing credentials)\n');
    return;
  }

  const { source, target } = clients;

  const sourceBuckets = await listBuckets(source);
  const targetBuckets = await listBuckets(target);

  const targetBucketIds = new Set(targetBuckets.map(b => b.id));

  for (const bucket of sourceBuckets) {
    const sourceFiles = await listAllFiles(source, bucket.id);

    if (targetBucketIds.has(bucket.id)) {
      const targetFiles = await listAllFiles(target, bucket.id);

      if (sourceFiles.length === targetFiles.length) {
        console.log(`   ✅ ${bucket.name}: ${sourceFiles.length} files (match)`);
      } else {
        console.log(`   ❌ ${bucket.name}: ${targetFiles.length}/${sourceFiles.length} files (mismatch)`);
      }
    } else {
      console.log(`   ❌ ${bucket.name}: bucket missing in target`);
    }
  }

  console.log('');
}

// Helper function to format bytes
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
