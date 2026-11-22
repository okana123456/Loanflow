// Bripta private client/guarantor documents: server-side access checks, Google Cloud Storage
// uploads, short-lived signed links, replacement and permanent deletion.
// Secrets (Supabase function secrets only): BRIPTA_GCS_BUCKET, BRIPTA_GCS_SIGNER_KEY (service-account JSON).
// Logs contain action and step names only: never paths, keys, tokens, URLs or personal details.
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';
import { createGcs, newBriptaObjectKey, sha256Hex, validateDocument, BRIPTA_DOCUMENT_KINDS } from './bripta-gcs.mjs';

const SUPABASE_BUCKET = 'bripta-client-documents';
const LINK_SECONDS = 60;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, apikey, content-type, x-client-info',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};
const json = (body: Record<string, unknown>, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });

const url = Deno.env.get('SUPABASE_URL') || '';
const serviceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '';
const bucketName = Deno.env.get('BRIPTA_GCS_BUCKET') || '';
let signer: ReturnType<typeof createGcs> | null = null;
function gcs() {
  if (!signer) {
    const raw = Deno.env.get('BRIPTA_GCS_SIGNER_KEY') || '';
    if (!raw || !bucketName) throw new Error('gcs_not_configured');
    const parsed = JSON.parse(raw);
    signer = createGcs({ bucket: bucketName, credentials: { client_email: parsed.client_email, private_key: parsed.private_key } });
  }
  return signer;
}
const admin = createClient(url, serviceKey, { auth: { persistSession: false, autoRefreshToken: false } });

function sameSecret(a: string, b: string) {
  if (!a || !b || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
function dbStatus(error: { code?: string } | null) {
  if (!error) return 200;
  if (error.code === '42501') return 403;
  if (error.code === '40001') return 409;
  if (error.code === 'P0002') return 404;
  if (error.code === '23503') return 409;
  return 500;
}
const safeMessage = (error: { code?: string; message?: string } | null, fallback: string) =>
  ['42501', '40001', 'P0002', '23503'].includes(String(error?.code)) ? String(error?.message || fallback).slice(0, 160) : fallback;

type Actor = { uid: string; email: string };
type Doc = {
  storage_provider: string; object_path: string | null; gcs_bucket: string | null; gcs_object_key: string | null;
  gcs_generation: number | null; sha256: string | null; mime_type: string; file_size: number;
};

async function lookup(actor: Actor, clientId: string, kind: string | null) {
  const { data, error } = await admin.rpc('bripta_document_lookup', { p_uid: actor.uid, p_email: actor.email, p_client: clientId, p_kind: kind });
  return { docs: (data || []) as Doc[], error };
}

async function signedLink(doc: Doc): Promise<string> {
  if (doc.storage_provider === 'gcs') {
    if (doc.gcs_bucket !== bucketName) throw new Error('bucket_mismatch');
    return await gcs().signedGetUrl(doc.gcs_object_key!, { expiresSeconds: LINK_SECONDS, contentType: doc.mime_type });
  }
  // Supabase fallback while migration is in progress (access was already checked above).
  const { data, error } = await admin.storage.from(SUPABASE_BUCKET).createSignedUrl(doc.object_path!, LINK_SECONDS);
  if (error || !data?.signedUrl) throw new Error('supabase_link_failed');
  return data.signedUrl;
}

async function handleSign(actor: Actor, body: Record<string, unknown>) {
  const clientId = String(body.client_id || ''), kind = String(body.kind || '');
  if (!UUID.test(clientId) || !BRIPTA_DOCUMENT_KINDS.includes(kind)) return json({ ok: false, message: 'Invalid document request.' }, 400);
  const { docs, error } = await lookup(actor, clientId, kind);
  if (error) return json({ ok: false, message: safeMessage(error, 'Could not check access.') }, dbStatus(error));
  if (!docs.length) return json({ ok: false, message: 'Document not attached.' }, 404);
  return json({ ok: true, url: await signedLink(docs[0]), expires_in: LINK_SECONDS });
}

async function handleUpload(actor: Actor, req: Request) {
  const length = Number(req.headers.get('content-length') || 0);
  if (length > 9 * 1024 * 1024) return json({ ok: false, message: 'Maximum file size is 8 MB.' }, 413);
  const form = await req.formData();
  const clientId = String(form.get('client_id') || ''), kind = String(form.get('kind') || '');
  const expectedPrevious = String(form.get('expected_previous') || '');
  const file = form.get('file');
  if (!UUID.test(clientId) || !BRIPTA_DOCUMENT_KINDS.includes(kind) || !(file instanceof Blob))
    return json({ ok: false, message: 'Invalid upload request.' }, 400);
  const bytes = new Uint8Array(await file.arrayBuffer());
  const invalid = validateDocument(kind, bytes, file.type || null);
  if (invalid) return json({ ok: false, message: invalid }, 415);
  const mime = file.type;
  const digest = await sha256Hex(bytes);

  // Access check + duplicate detection before anything is stored.
  const { docs, error } = await lookup(actor, clientId, kind);
  if (error) return json({ ok: false, message: safeMessage(error, 'Could not check access.') }, dbStatus(error));
  const current = docs[0];
  if (current && (current.sha256 === digest || (current.storage_provider === 'supabase' && current.object_path?.includes(`/${digest}.`))))
    return json({ ok: true, status: 'unchanged' });
  const currentRef = current ? (current.gcs_object_key || current.object_path || '') : '';
  if (currentRef !== expectedPrevious) return json({ ok: false, message: 'The document changed; reopen and try again.' }, 409);

  const key = newBriptaObjectKey();
  const stored = await gcs().uploadNew(key, bytes, mime, digest);
  if (stored.exists || stored.size !== bytes.byteLength || stored.sha256 !== digest) {
    if (!stored.exists && stored.generation) await gcs().deleteGeneration(key, stored.generation).catch(() => {});
    return json({ ok: false, message: 'Upload could not be verified; nothing was changed.' }, 502);
  }
  const fileName = String((file as File).name || 'document').replace(/[\r\n\t]/g, ' ').slice(0, 200);
  const { data: committed, error: commitError } = await admin.rpc('bripta_document_gcs_commit', {
    p_uid: actor.uid, p_email: actor.email, p_client: clientId, p_kind: kind, p_expected_previous: expectedPrevious,
    p_bucket: bucketName, p_key: key, p_generation: stored.generation, p_sha256: digest, p_size: bytes.byteLength,
    p_mime: mime, p_file_name: fileName,
  });
  if (commitError) {
    try { await gcs().deleteGeneration(key, stored.generation); }
    catch { await admin.rpc('bripta_document_queue_object', { p_provider: 'gcs', p_bucket: bucketName, p_key: key, p_generation: stored.generation, p_client: clientId, p_kind: kind, p_reason: 'failed_commit' }); }
    return json({ ok: false, message: safeMessage(commitError, 'Document link was not saved.') }, dbStatus(commitError) === 200 ? 500 : dbStatus(commitError));
  }
  // Replacement confirmed: remove the previous Google Cloud object (exact generation only).
  // A previous Supabase original is queued for the approved cleanup instead.
  const prev = (committed || [])[0];
  if (prev?.previous_gcs_key) {
    try { await gcs().deleteGeneration(prev.previous_gcs_key, prev.previous_gcs_generation); }
    catch {
      await admin.rpc('bripta_document_queue_object', { p_provider: 'gcs', p_bucket: prev.previous_gcs_bucket, p_key: prev.previous_gcs_key,
        p_generation: prev.previous_gcs_generation, p_client: clientId, p_kind: kind, p_reason: 'replaced_by_staff' });
    }
  }
  return json({ ok: true, status: 'saved', storage: 'gcs' });
}

async function handleDeleteClient(actor: Actor, body: Record<string, unknown>) {
  const clientId = String(body.client_id || '');
  if (!UUID.test(clientId)) return json({ ok: false, message: 'Invalid client.' }, 400);
  const { data, error } = await admin.rpc('bripta_delete_client_with_documents', { p_uid: actor.uid, p_email: actor.email, p_client: clientId });
  if (error) return json({ ok: false, message: safeMessage(error, 'Client could not be deleted.') }, dbStatus(error));
  let pending = 0;
  for (const item of (data || []) as { queue_id: number; provider: string; bucket: string; object_key: string; gcs_generation: number }[]) {
    try {
      if (item.provider === 'gcs') {
        if (item.bucket !== bucketName) throw new Error('bucket_mismatch');
        await gcs().deleteGeneration(item.object_key, item.gcs_generation);
      } else {
        const { error: removeError } = await admin.storage.from(SUPABASE_BUCKET).remove([item.object_key]);
        if (removeError) throw new Error('remove_failed');
      }
      const { data: done } = await admin.rpc('bripta_document_queue_done', { p_id: item.queue_id });
      if (!done) pending++;
    } catch { pending++; }
  }
  return json({ ok: true, status: 'deleted', pending_object_cleanup: pending });
}

// Migration job only (service-role bearer): proves the switched record opens through the same signed-link path.
async function handleVerifyMigration(body: Record<string, unknown>) {
  const id = Number(body.ledger_id);
  if (!Number.isSafeInteger(id) || id <= 0) return json({ ok: false, message: 'Invalid ledger id.' }, 400);
  const { data, error } = await admin.rpc('bripta_gcs_migration_target', { p_id: id });
  const target = (data || [])[0];
  if (error || !target) return json({ ok: false, code: 'not_switched' }, 409);
  const link = await signedLink({ ...target, storage_provider: 'gcs', object_path: null });
  const res = await fetch(link);
  if (!res.ok) return json({ ok: false, code: `signed_get_${res.status}` }, 502);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const ok = bytes.byteLength === Number(target.file_size) && (await sha256Hex(bytes)) === target.sha256;
  return json({ ok, code: ok ? 'verified' : 'checksum_mismatch' }, ok ? 200 : 502);
}

Deno.serve(async req => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: cors });
  if (req.method !== 'POST') return json({ ok: false, message: 'Use POST.' }, 405);
  const token = /^Bearer (.+)$/i.exec(req.headers.get('authorization') || '')?.[1] || '';
  if (!token) return json({ ok: false, message: 'Sign in to Bripta first.' }, 401);
  let action = 'unknown';
  try {
    const isUpload = (req.headers.get('content-type') || '').startsWith('multipart/form-data');
    if (sameSecret(token, serviceKey)) {
      if (isUpload) return json({ ok: false }, 400);
      const body = await req.json();
      action = String(body?.action || '');
      return action === 'verify_migration' ? await handleVerifyMigration(body) : json({ ok: false }, 403);
    }
    const { data: { user }, error: userError } = await admin.auth.getUser(token);
    if (userError || !user) return json({ ok: false, message: 'Your login expired. Sign in again.' }, 401);
    const actor = { uid: user.id, email: user.email || '' };
    if (isUpload) { action = 'upload'; return await handleUpload(actor, req); }
    const body = await req.json();
    action = String(body?.action || '');
    if (action === 'sign') return await handleSign(actor, body);
    if (action === 'delete_client') return await handleDeleteClient(actor, body);
    return json({ ok: false, message: 'Unknown action.' }, 400);
  } catch (error) {
    const step = error instanceof Error ? String((error as { step?: string }).step || error.message).replace(/[^a-z_]/gi, '').slice(0, 40) : 'unknown';
    console.error('bripta-documents failed', action, step);
    return json({ ok: false, message: 'Document service error; nothing was changed.' }, 500);
  }
});
