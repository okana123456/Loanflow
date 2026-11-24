// Bripta private client/guarantor documents: server-side access checks, Backblaze B2 (private bucket)
// uploads, short-lived signed links, replacement and permanent deletion.
// Secrets (Supabase function secrets only): BRIPTA_B2_ENDPOINT, BRIPTA_B2_BUCKET, BRIPTA_B2_KEY_ID, BRIPTA_B2_APP_KEY.
// Logs contain action and step names only: never paths, keys, tokens, URLs or personal details.
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.57.0';
import { createB2, newBriptaObjectKey, sha256Hex, validateDocument, BRIPTA_DOCUMENT_KINDS } from './bripta-b2.mjs';

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
const bucketName = Deno.env.get('BRIPTA_B2_BUCKET') || '';
let client: ReturnType<typeof createB2> | null = null;
function store() {
  if (!client) {
    client = createB2({
      endpoint: Deno.env.get('BRIPTA_B2_ENDPOINT') || '', bucket: bucketName,
      keyId: Deno.env.get('BRIPTA_B2_KEY_ID') || '', applicationKey: Deno.env.get('BRIPTA_B2_APP_KEY') || '',
    });
  }
  return client;
}
const admin = createClient(url, serviceKey, { auth: { persistSession: false, autoRefreshToken: false } });

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
  storage_provider: string; object_path: string | null; ext_bucket: string | null; ext_object_key: string | null;
  ext_version: string | null; sha256: string | null; mime_type: string; file_size: number;
};

async function lookup(actor: Actor, clientId: string, kind: string | null) {
  const { data, error } = await admin.rpc('bripta_document_lookup', { p_uid: actor.uid, p_email: actor.email, p_client: clientId, p_kind: kind });
  return { docs: (data || []) as Doc[], error };
}

async function signedLink(doc: Doc): Promise<string> {
  if (doc.storage_provider === 'b2') {
    if (doc.ext_bucket !== bucketName) throw new Error('bucket_mismatch');
    return await store().signedGetUrl(doc.ext_object_key!, { expiresSeconds: LINK_SECONDS, contentType: doc.mime_type, version: doc.ext_version || undefined });
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

async function removeOldProfile(clientId: string, previous: { previous_path: string | null; previous_key: string | null; previous_version: string | null }) {
  if (previous.previous_key && previous.previous_version) {
    try { await store().deleteVersion(previous.previous_key, previous.previous_version); }
    catch { await admin.rpc('bripta_document_queue_object', { p_provider: 'b2', p_bucket: bucketName,
      p_key: previous.previous_key, p_version: previous.previous_version, p_client: clientId,
      p_kind: 'profile_photo', p_reason: 'replaced_by_staff' }); }
  } else if (previous.previous_path && previous.previous_path !== 'b2') {
    // Existing Supabase photos stay in the review queue until a separate approved cleanup.
    await admin.rpc('bripta_document_queue_object', { p_provider: 'supabase', p_bucket: 'client-photos',
      p_key: previous.previous_path, p_version: null, p_client: clientId,
      p_kind: 'profile_photo', p_reason: 'replaced_by_staff' });
  }
}

async function handleProfileSign(actor: Actor, body: Record<string, unknown>) {
  const clientId = String(body.client_id || '');
  if (!UUID.test(clientId)) return json({ ok: false, message: 'Invalid client.' }, 400);
  const { data, error } = await admin.rpc('bripta_b2_profile_lookup', { p_uid: actor.uid, p_email: actor.email, p_client: clientId });
  if (error) return json({ ok: false, message: safeMessage(error, 'Could not open photo.') }, dbStatus(error));
  const photo = (data || [])[0];
  if (!photo) return json({ ok: false, message: 'Photo not found.' }, 404);
  if (photo.ext_bucket !== bucketName) return json({ ok: false, message: 'Photo bucket mismatch.' }, 502);
  const photoUrl = await store().signedGetUrl(photo.object_key, { expiresSeconds: LINK_SECONDS,
    contentType: 'image/jpeg', version: photo.ext_version });
  return json({ ok: true, url: photoUrl, expires_in: LINK_SECONDS });
}

async function handleProfileUpload(actor: Actor, form: FormData) {
  const clientId = String(form.get('client_id') || ''), previous = String(form.get('expected_previous') || '');
  const file = form.get('file');
  if (!UUID.test(clientId) || !(file instanceof Blob) || file.type !== 'image/jpeg' || !file.size || file.size > 280000)
    return json({ ok: false, message: 'Use a cropped JPG photo under 280 KB.' }, 415);
  const bytes = new Uint8Array(await file.arrayBuffer());
  if (bytes[0] !== 0xff || bytes[1] !== 0xd8 || bytes.at(-2) !== 0xff || bytes.at(-1) !== 0xd9)
    return json({ ok: false, message: 'Invalid JPG photo.' }, 415);
  // Check access before uploading private data to B2.
  const { data: existing, error: accessError } = await admin.rpc('bripta_b2_profile_lookup', { p_uid: actor.uid, p_email: actor.email, p_client: clientId });
  if (accessError) return json({ ok: false, message: safeMessage(accessError, 'Could not check photo access.') }, dbStatus(accessError));
  const key = `bripta/photos/v1/${crypto.randomUUID()}`, digest = await sha256Hex(bytes);
  if (previous === 'b2' && existing?.[0]?.sha256 === digest)
    return json({ ok: true, status: 'unchanged', storage: 'b2' });
  const put = await store().putObject(key, bytes, 'image/jpeg', digest);
  const meta = await store().head(key);
  if (!meta?.version || meta.version !== put.version || meta.size !== bytes.length || meta.sha256 !== digest) {
    if (put.version) {
      try { await store().deleteVersion(key, put.version); }
      catch { await admin.rpc('bripta_document_queue_object', { p_provider: 'b2', p_bucket: bucketName,
        p_key: key, p_version: put.version, p_client: clientId, p_kind: 'profile_photo', p_reason: 'failed_commit' }); }
    }
    return json({ ok: false, message: 'Photo upload could not be verified.' }, 502);
  }
  const { data, error } = await admin.rpc('bripta_b2_profile_commit', { p_uid: actor.uid, p_email: actor.email,
    p_client: clientId, p_previous: previous, p_bucket: bucketName, p_key: key, p_version: meta.version, p_sha256: digest, p_size: bytes.length });
  if (error) {
    try { await store().deleteVersion(key, meta.version); }
    catch { await admin.rpc('bripta_document_queue_object', { p_provider: 'b2', p_bucket: bucketName,
      p_key: key, p_version: meta.version, p_client: clientId, p_kind: 'profile_photo', p_reason: 'failed_commit' }); }
    return json({ ok: false, message: safeMessage(error, 'Photo could not be saved.') }, dbStatus(error));
  }
  await removeOldProfile(clientId, (data || [])[0] || {});
  return json({ ok: true, status: 'saved', storage: 'b2' });
}

async function handleProfileRemove(actor: Actor, body: Record<string, unknown>) {
  const clientId = String(body.client_id || '');
  if (!UUID.test(clientId)) return json({ ok: false, message: 'Invalid client.' }, 400);
  const { data, error } = await admin.rpc('bripta_b2_profile_remove', { p_uid: actor.uid, p_email: actor.email, p_client: clientId });
  if (error) return json({ ok: false, message: safeMessage(error, 'Could not remove photo.') }, dbStatus(error));
  await removeOldProfile(clientId, (data || [])[0] || {});
  return json({ ok: true, status: 'removed' });
}

async function handleUpload(actor: Actor, req: Request, form: FormData) {
  const length = Number(req.headers.get('content-length') || 0);
  if (length > 9 * 1024 * 1024) return json({ ok: false, message: 'Maximum file size is 8 MB.' }, 413);
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
  const currentRef = current ? (current.ext_object_key || current.object_path || '') : '';
  if (currentRef !== expectedPrevious) return json({ ok: false, message: 'The document changed; reopen and try again.' }, 409);

  const key = newBriptaObjectKey();
  // B2 verifies the signed SHA-256 of the body on upload; then confirm size, checksum tag and version.
  const put = await store().putObject(key, bytes, mime, digest);
  const meta = await store().head(key);
  const stored = { version: put.version || meta?.version || '' };
  if (!meta || !stored.version || meta.version !== stored.version || meta.size !== bytes.byteLength || meta.sha256 !== digest) {
    if (stored.version) await store().deleteVersion(key, stored.version).catch(() => {});
    return json({ ok: false, message: 'Upload could not be verified; nothing was changed.' }, 502);
  }
  const fileName = String((file as File).name || 'document').replace(/[\r\n\t]/g, ' ').slice(0, 200);
  const { data: committed, error: commitError } = await admin.rpc('bripta_document_b2_commit', {
    p_uid: actor.uid, p_email: actor.email, p_client: clientId, p_kind: kind, p_expected_previous: expectedPrevious,
    p_bucket: bucketName, p_key: key, p_version: stored.version, p_sha256: digest, p_size: bytes.byteLength,
    p_mime: mime, p_file_name: fileName,
  });
  if (commitError) {
    try { await store().deleteVersion(key, stored.version); }
    catch { await admin.rpc('bripta_document_queue_object', { p_provider: 'b2', p_bucket: bucketName, p_key: key, p_version: stored.version, p_client: clientId, p_kind: kind, p_reason: 'failed_commit' }); }
    return json({ ok: false, message: safeMessage(commitError, 'Document link was not saved.') }, dbStatus(commitError) === 200 ? 500 : dbStatus(commitError));
  }
  // Replacement confirmed: remove the previous B2 object (exact version only).
  // A previous Supabase original is queued for the approved cleanup instead.
  const prev = (committed || [])[0];
  if (prev?.previous_ext_key) {
    try { await store().deleteVersion(prev.previous_ext_key, prev.previous_ext_version); }
    catch {
      await admin.rpc('bripta_document_queue_object', { p_provider: 'b2', p_bucket: prev.previous_ext_bucket, p_key: prev.previous_ext_key,
        p_version: prev.previous_ext_version, p_client: clientId, p_kind: kind, p_reason: 'replaced_by_staff' });
    }
  }
  return json({ ok: true, status: 'saved', storage: 'b2' });
}

async function handleDeleteClient(actor: Actor, body: Record<string, unknown>) {
  const clientId = String(body.client_id || '');
  if (!UUID.test(clientId)) return json({ ok: false, message: 'Invalid client.' }, 400);
  const { data, error } = await admin.rpc('bripta_delete_client_with_documents', { p_uid: actor.uid, p_email: actor.email, p_client: clientId });
  if (error) return json({ ok: false, message: safeMessage(error, 'Client could not be deleted.') }, dbStatus(error));
  let pending = 0;
  for (const item of (data || []) as { queue_id: number; provider: string; bucket: string; object_key: string; ext_version: string }[]) {
    try {
      if (item.provider === 'b2') {
        if (item.bucket !== bucketName) throw new Error('bucket_mismatch');
        await store().deleteVersion(item.object_key, item.ext_version);
      } else {
        if (![SUPABASE_BUCKET, 'client-photos'].includes(item.bucket)) throw new Error('bucket_mismatch');
        const { error: removeError } = await admin.storage.from(item.bucket).remove([item.object_key]);
        if (removeError) throw new Error('remove_failed');
      }
      const { data: done } = await admin.rpc('bripta_document_queue_done', { p_id: item.queue_id });
      if (!done) pending++;
    } catch { pending++; }
  }
  return json({ ok: true, status: 'deleted', pending_object_cleanup: pending });
}

// Migration job only: proves the switched record opens through the same signed-link path.
// The caller's key is checked by Postgres itself: bripta_b2_migration_target() only answers the
// service role, so this accepts the legacy service_role JWT and new sb_secret_ keys alike.
async function handleVerifyMigration(token: string, body: Record<string, unknown>) {
  const id = Number(body.ledger_id);
  if (!Number.isSafeInteger(id) || id <= 0) return json({ ok: false, message: 'Invalid ledger id.' }, 400);
  const caller = createClient(url, token, { auth: { persistSession: false, autoRefreshToken: false } });
  const { data, error } = await caller.rpc('bripta_b2_migration_target', { p_id: id });
  if (error) return json({ ok: false, code: error.code === '42501' ? 'not_service_role' : 'target_lookup_failed' }, 403);
  const target = (data || [])[0];
  if (!target) return json({ ok: false, code: 'not_switched' }, 409);
  const link = await signedLink({ ...target, storage_provider: 'b2', object_path: null });
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
    if (isUpload && Number(req.headers.get('content-length') || 0) > 9 * 1024 * 1024)
      return json({ ok: false, message: 'Maximum file size is 8 MB.' }, 413);
    const form = isUpload ? await req.formData() : null;
    const body = isUpload ? {} : await req.json();
    action = isUpload ? String(form?.get('action') || 'upload') : String(body?.action || '');
    if (action === 'verify_migration') return await handleVerifyMigration(token, body);
    const { data: { user }, error: userError } = await admin.auth.getUser(token);
    if (userError || !user) return json({ ok: false, message: 'Your login expired. Sign in again.' }, 401);
    const actor = { uid: user.id, email: user.email || '' };
    if (action === 'upload_profile_photo' && form) return await handleProfileUpload(actor, form);
    if (isUpload) return await handleUpload(actor, req, form!);
    if (action === 'sign') return await handleSign(actor, body);
    if (action === 'sign_profile_photo') return await handleProfileSign(actor, body);
    if (action === 'remove_profile_photo') return await handleProfileRemove(actor, body);
    if (action === 'delete_client') return await handleDeleteClient(actor, body);
    return json({ ok: false, message: 'Unknown action.' }, 400);
  } catch (error) {
    const step = error instanceof Error ? String((error as { step?: string }).step || error.message).replace(/[^a-z_]/gi, '').slice(0, 40) : 'unknown';
    console.error('bripta-documents failed', action, step);
    return json({ ok: false, message: 'Document service error; nothing was changed.' }, 500);
  }
});
