// Minimal Backblaze B2 client (S3-compatible API, AWS Signature V4) for Bripta documents.
// Plain ESM using fetch + WebCrypto so the same code runs in Supabase Edge (Deno) and Node 22+.
// Never logs object bytes, keys, credentials or signed URLs.

export const BRIPTA_OBJECT_KEY_PATTERN = /^bripta\/docs\/v1\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const enc = new TextEncoder();
const hex = buf => Array.from(new Uint8Array(buf), b => b.toString(16).padStart(2, '0')).join('');
export const EMPTY_SHA256 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

export async function sha256Hex(bytes) {
  return hex(await crypto.subtle.digest('SHA-256', typeof bytes === 'string' ? enc.encode(bytes) : bytes));
}
async function hmac(key, data) {
  const k = await crypto.subtle.importKey('raw', typeof key === 'string' ? enc.encode(key) : key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC', k, enc.encode(data)));
}
// RFC 3986 encoding as required by SigV4.
export function rfc3986(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
}
const encodePath = path => path.split('/').map(rfc3986).join('/');
const amzDate = now => now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');

export function newBriptaObjectKey() {
  return `bripta/docs/v1/${crypto.randomUUID()}`;
}

/** Low-level SigV4 (exported for test vectors). path must already be the decoded object path, e.g. /bucket/key. */
export async function sigV4({ method, host, path, query = {}, headers = {}, payloadHash, region, accessKeyId, secretAccessKey, now = new Date(), presignSeconds }) {
  const datetime = amzDate(now);
  const date = datetime.slice(0, 8);
  const scope = `${date}/${region}/s3/aws4_request`;
  const allHeaders = { host, ...Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), String(v).trim()])) };
  const signedHeaders = Object.keys(allHeaders).sort().join(';');
  const q = { ...query };
  if (presignSeconds) {
    Object.assign(q, {
      'X-Amz-Algorithm': 'AWS4-HMAC-SHA256', 'X-Amz-Credential': `${accessKeyId}/${scope}`,
      'X-Amz-Date': datetime, 'X-Amz-Expires': String(presignSeconds), 'X-Amz-SignedHeaders': signedHeaders,
    });
  } else {
    allHeaders['x-amz-date'] = datetime;
    allHeaders['x-amz-content-sha256'] = payloadHash;
  }
  const headerNames = Object.keys(allHeaders).sort();
  const canonicalQuery = Object.keys(q).sort().map(k => `${rfc3986(k)}=${rfc3986(q[k])}`).join('&');
  const canonical = [method, encodePath(path), canonicalQuery,
    headerNames.map(k => `${k}:${allHeaders[k]}`).join('\n') + '\n', headerNames.join(';'),
    presignSeconds ? 'UNSIGNED-PAYLOAD' : payloadHash].join('\n');
  const stringToSign = ['AWS4-HMAC-SHA256', datetime, scope, await sha256Hex(canonical)].join('\n');
  let key = await hmac(`AWS4${secretAccessKey}`, date);
  for (const part of [region, 's3', 'aws4_request']) key = await hmac(key, part);
  const signature = hex(await hmac(key, stringToSign));
  const url = `https://${host}${encodePath(path)}${canonicalQuery ? `?${canonicalQuery}` : ''}`;
  if (presignSeconds) return { url: `${url}${canonicalQuery ? '&' : '?'}X-Amz-Signature=${signature}` };
  delete allHeaders.host; // fetch sets Host from the URL
  allHeaders.authorization = `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${scope}, SignedHeaders=${headerNames.join(';')}, Signature=${signature}`;
  return { url, headers: allHeaders };
}

export class StorageError extends Error {
  constructor(step, status) { super(`B2 ${step} failed (${status})`); this.step = step; this.status = status; }
}

/**
 * @param {{endpoint:string,bucket:string,keyId:string,applicationKey:string,fetchImpl?:typeof fetch}} opts
 * endpoint: the bucket's S3 endpoint, e.g. s3.us-west-004.backblazeb2.com
 */
export function createB2({ endpoint, bucket, keyId, applicationKey, fetchImpl = fetch }) {
  const host = String(endpoint || '').replace(/^https?:\/\//, '').replace(/\/+$/, '');
  const region = /^s3\.([a-z0-9-]+)\.backblazeb2\.com$/.exec(host)?.[1];
  if (!region) throw new Error('Invalid B2 S3 endpoint');
  if (!/^[a-z0-9][a-z0-9-]{4,48}[a-z0-9]$/.test(bucket || '')) throw new Error('Invalid B2 bucket name');
  if (!keyId || !applicationKey) throw new Error('B2 application key is missing');
  const creds = { region, accessKeyId: keyId, secretAccessKey: applicationKey, host };
  const checkKey = key => { if (!BRIPTA_OBJECT_KEY_PATTERN.test(key)) throw new Error('Unexpected object key shape'); };
  const objPath = key => `/${bucket}/${key}`;
  async function send(method, key, { query = {}, headers = {}, body, payloadHash = EMPTY_SHA256 } = {}) {
    const signed = await sigV4({ ...creds, method, path: objPath(key), query, headers, payloadHash });
    return fetchImpl(signed.url, { method, headers: signed.headers, body });
  }

  return {
    bucket,
    /** Upload a new object. The signed SHA-256 payload hash makes B2 reject any byte mismatch. */
    async putObject(key, bytes, contentType, sha256) {
      checkKey(key);
      const res = await send('PUT', key, {
        body: bytes, payloadHash: sha256,
        headers: { 'content-type': contentType, 'cache-control': 'private, max-age=0, no-store', 'x-amz-meta-sha256': sha256 },
      });
      if (!res.ok) throw new StorageError('upload', res.status);
      const version = res.headers.get('x-amz-version-id');
      return { version: version || null };
    },
    async head(key) {
      checkKey(key);
      const res = await send('HEAD', key);
      if (res.status === 404) return null;
      if (!res.ok) throw new StorageError('head', res.status);
      return {
        size: Number(res.headers.get('content-length')), sha256: res.headers.get('x-amz-meta-sha256'),
        version: res.headers.get('x-amz-version-id'), contentType: res.headers.get('content-type'),
      };
    },
    /** Reads one exact version into memory (checksum verification only). */
    async download(key, version) {
      checkKey(key);
      const res = await send('GET', key, { query: version ? { versionId: version } : {} });
      if (!res.ok) throw new StorageError('download', res.status);
      return new Uint8Array(await res.arrayBuffer());
    },
    /** Permanently deletes one exact version. */
    async deleteVersion(key, version) {
      checkKey(key);
      if (!version) throw new Error('A version is required for deletion');
      const res = await send('DELETE', key, { query: { versionId: version } });
      if (res.status === 404) return 'missing';
      if (!res.ok) throw new StorageError('delete', res.status);
      return 'deleted';
    },
    /** Pre-signed GET link, at most 5 minutes. */
    async signedGetUrl(key, { expiresSeconds = 60, contentType, version, now = new Date() } = {}) {
      checkKey(key);
      const query = { 'response-content-disposition': 'inline' };
      if (contentType) query['response-content-type'] = contentType;
      if (version) query.versionId = version;
      const { url } = await sigV4({ ...creds, method: 'GET', path: objPath(key), query, now,
        presignSeconds: Math.min(Math.max(Math.floor(expiresSeconds), 1), 300) });
      return url;
    },
  };
}

// File-type checks shared by the Edge Function and tests.
export const BRIPTA_DOCUMENT_KINDS = ['client_passport', 'client_id_front', 'client_id_back', 'guarantor_passport', 'guarantor_id_front', 'guarantor_id_back'];
export const BRIPTA_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024;

export function sniffDocumentType(bytes) {
  const b = new Uint8Array(bytes.slice ? bytes.slice(0, 12) : bytes);
  if (b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) return 'image/jpeg';
  if (b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47) return 'image/png';
  if (String.fromCharCode(...b.slice(0, 4)) === 'RIFF' && String.fromCharCode(...b.slice(8, 12)) === 'WEBP') return 'image/webp';
  if (String.fromCharCode(...b.slice(0, 5)) === '%PDF-') return 'application/pdf';
  return null;
}

export function validateDocument(kind, bytes, declaredType) {
  if (!BRIPTA_DOCUMENT_KINDS.includes(kind)) return 'Unknown document type';
  const size = bytes?.byteLength || 0;
  if (!size || size > BRIPTA_MAX_DOCUMENT_BYTES) return 'Maximum file size is 8 MB.';
  const sniffed = sniffDocumentType(bytes);
  if (!sniffed || (declaredType && declaredType !== sniffed)) return 'Use a JPG, PNG, WEBP or PDF file.';
  if (kind.endsWith('passport') && sniffed === 'application/pdf') return 'Passport photo must be a JPG, PNG or WEBP image.';
  return null;
}
