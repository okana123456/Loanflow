// Bripta document migration: Supabase Storage -> private Google Cloud Storage.
// Runs only on an ephemeral GitHub-hosted runner. Document bytes stay in memory; nothing is
// written to disk, uploaded as an artifact or logged. Logs show counts, numeric ledger ids and
// error codes only. Safe to rerun: every step is recorded in public.bripta_gcs_document_migration.
//
// BRIPTA_MODE=migrate  copy -> verify checksum -> switch link -> verify app signed link -> record
// BRIPTA_MODE=cleanup  delete ONLY owner-approved Supabase objects (Storage API), re-verifying first
import { createClient } from '@supabase/supabase-js';
import { createGcs, sha256Hex } from '../supabase/functions/bripta-documents/bripta-gcs.mjs';

const PROJECT = 'https://nngscmpsxtqqjzcnsrbi.supabase.co';
const SUPABASE_BUCKET = 'bripta-client-documents';
const serviceKey = process.env.BRIPTA_SUPABASE_SERVICE_ROLE_KEY;
const gcsToken = process.env.GCS_ACCESS_TOKEN;
const gcsBucket = process.env.BRIPTA_GCS_BUCKET;
const mode = process.env.BRIPTA_MODE || 'migrate';
const limit = Math.min(Math.max(Number(process.env.BRIPTA_MAX_FILES || 5), 1), 600);
if (!serviceKey) throw new Error('Secret BRIPTA_SUPABASE_SERVICE_ROLE_KEY is missing.');
if (!gcsToken || !gcsBucket) throw new Error('Google workload identity token or BRIPTA_GCS_BUCKET is missing.');

const supabase = createClient(PROJECT, serviceKey, { auth: { persistSession: false, autoRefreshToken: false } });
const storage = supabase.storage.from(SUPABASE_BUCKET);
const gcs = createGcs({ bucket: gcsBucket, accessToken: gcsToken });

class StepError extends Error { constructor(code) { super(code); this.code = code; } }
const step = code => { throw new StepError(code); };
const safeCode = e => (e instanceof StepError ? e.code : e?.step ? `gcs_${e.step}_${e.status}` : 'unexpected')
  .replace(/[^A-Za-z0-9_]/g, '').slice(0, 60);
async function rpc(name, args) {
  const { data, error } = await supabase.rpc(name, args);
  if (error) step(`${name}_${String(error.code || 'err').replace(/[^A-Za-z0-9]/g, '')}`);
  return data;
}

async function verifyThroughApp(id) {
  const res = await fetch(`${PROJECT}/functions/v1/bripta-documents`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${serviceKey}`, apikey: serviceKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'verify_migration', ledger_id: id }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok || !body.ok) step(`app_verify_${String(body.code || res.status).replace(/[^a-z0-9_]/gi, '')}`);
}

async function copyOne(row) {
  const { data: blob, error } = await storage.download(row.supabase_path);
  if (error || !blob) step('supabase_download');
  const bytes = new Uint8Array(await blob.arrayBuffer());
  if (bytes.byteLength !== Number(row.expected_bytes)) step('original_size_changed');
  const sha = await sha256Hex(bytes);
  if (row.sha256 && row.sha256 !== sha) step('original_checksum_changed');
  const contentType = ['image/jpeg', 'image/png', 'image/webp', 'application/pdf'].includes(blob.type) ? blob.type : 'application/octet-stream';
  const up = await gcs.uploadNew(row.gcs_object_key, bytes, contentType, sha);
  let generation = up.generation;
  if (up.exists) { // earlier interrupted attempt: accept only an identical object
    const meta = await gcs.metadata(row.gcs_object_key);
    if (!meta || meta.size !== bytes.byteLength || meta.sha256 !== sha) step('existing_gcs_object_mismatch');
    generation = meta.generation;
  } else if (up.size !== bytes.byteLength || up.sha256 !== sha) step('gcs_upload_mismatch');
  // Independent read-back: byte count and SHA-256 must match the Supabase original.
  const back = await gcs.download(row.gcs_object_key, generation);
  if (back.byteLength !== bytes.byteLength || (await sha256Hex(back)) !== sha) step('gcs_checksum_mismatch');
  if (!(await rpc('bripta_gcs_migration_mark_copied', { p_id: row.id, p_sha256: sha, p_generation: generation, p_bytes: bytes.byteLength })))
    step('mark_copied_rejected');
}

async function migrateOne(row) {
  if (row.state === 'claimed' || row.state === 'failed') await copyOne(row);
  else if (row.state === 'copied') {
    const meta = await gcs.metadata(row.gcs_object_key);
    if (!meta || meta.generation !== Number(row.gcs_generation) || meta.sha256 !== row.sha256) step('copied_object_missing');
  }
  if (row.state !== 'switched') {
    const outcome = await rpc('bripta_gcs_migration_switch', { p_id: row.id });
    if (outcome === 'document_changed') return 'skipped';
    if (outcome !== 'switched' && outcome !== 'already_switched') step(`switch_${outcome}`);
  }
  try { await verifyThroughApp(row.id); }
  catch (e) { // never leave staff on an unverified copy
    await supabase.rpc('bripta_gcs_migration_rollback', { p_id: row.id });
    throw e;
  }
  if (!(await rpc('bripta_gcs_migration_record', { p_id: row.id, p_state: 'verified', p_error: null }))) step('record_rejected');
  return 'verified';
}

async function migrate() {
  const counts = { verified: 0, skipped: 0, failed: 0 };
  const seen = new Set();
  let consecutiveFailures = 0;
  while (seen.size < limit) {
    const rows = (await rpc('bripta_gcs_migration_claim', { p_limit: Math.min(10, limit - seen.size + 5), p_bucket: gcsBucket }))
      .filter(r => !seen.has(r.id));
    if (!rows.length) break;
    for (const row of rows) {
      if (seen.size >= limit) break;
      seen.add(row.id);
      try {
        counts[await migrateOne(row)]++;
        consecutiveFailures = 0;
      } catch (e) {
        const code = safeCode(e);
        counts.failed++;
        await supabase.rpc('bripta_gcs_migration_record', { p_id: row.id, p_state: 'failed', p_error: code });
        console.error(`ledger ${row.id}: ${code}`);
        if (++consecutiveFailures >= 3) throw new Error('Stopped after 3 consecutive failures; originals are unchanged.');
      }
    }
    if (seen.size && seen.size % 25 === 0) console.log(`progress: ${JSON.stringify(counts)}`);
  }
  console.log(`migrate complete: ${JSON.stringify(counts)} (processed ${seen.size}, limit ${limit})`);
  if (counts.failed) process.exitCode = 1;
}

async function cleanup() {
  const counts = { deleted: 0, skipped: 0, failed: 0 };
  const seen = new Set();
  while (seen.size < limit) {
    const rows = (await rpc('bripta_document_cleanup_next', { p_limit: 20 })).filter(r => !seen.has(r.object_id));
    if (!rows.length) break;
    for (const row of rows) {
      if (seen.size >= limit) break;
      seen.add(row.object_id);
      try {
        if (row.category === 'migrated_original') { // Google copy must still be the verified one
          const meta = row.gcs_object_key ? await gcs.metadata(row.gcs_object_key) : null;
          if (!meta || meta.generation !== Number(row.gcs_generation) || meta.sha256 !== row.sha256) {
            await rpc('bripta_document_cleanup_mark', { p_object_id: row.object_id, p_skip_reason: 'gcs_unverified' });
            counts.skipped++; continue;
          }
        }
        const { error } = await storage.remove([row.object_path]);
        if (error) step('storage_remove');
        if (!(await rpc('bripta_document_cleanup_mark', { p_object_id: row.object_id, p_skip_reason: null }))) step('removal_not_confirmed');
        counts.deleted++;
      } catch (e) {
        counts.failed++;
        console.error(`cleanup object ${seen.size}: ${safeCode(e)}`);
      }
    }
  }
  console.log(`cleanup complete: ${JSON.stringify(counts)}`);
  if (counts.failed) process.exitCode = 1;
}

if (mode === 'migrate') await migrate();
else if (mode === 'cleanup') await cleanup();
else throw new Error('BRIPTA_MODE must be migrate or cleanup');
