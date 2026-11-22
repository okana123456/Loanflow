// Minimal Google Cloud Storage client for Bripta documents.
// Plain ESM using fetch + WebCrypto so the same code runs in Supabase Edge (Deno) and Node 22.
// Never logs object bytes, keys, tokens or signed URLs.

const API = 'https://storage.googleapis.com';
export const BRIPTA_GCS_KEY_PATTERN = /^bripta\/docs\/v1\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

const enc = new TextEncoder();
const hex = buf => Array.from(new Uint8Array(buf), b => b.toString(16).padStart(2, '0')).join('');
const b64url = bytes => {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

export async function sha256Hex(bytes) {
  return hex(await crypto.subtle.digest('SHA-256', bytes));
}

// CRC32C (Castagnoli), base64 big-endian as GCS expects; GCS rejects an upload whose bytes do not match.
const CRC32C_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();
export function crc32cBase64(bytes) {
  let c = 0xffffffff;
  for (const b of new Uint8Array(bytes)) c = CRC32C_TABLE[(c ^ b) & 0xff] ^ (c >>> 8);
  c = (c ^ 0xffffffff) >>> 0;
  return btoa(String.fromCharCode((c >>> 24) & 255, (c >>> 16) & 255, (c >>> 8) & 255, c & 255));
}

// RFC 3986 encoding, as required by GCS V4 signing.
export function rfc3986(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
}
const encodeKeyPath = key => key.split('/').map(rfc3986).join('/');

export function newBriptaObjectKey() {
  return `bripta/docs/v1/${crypto.randomUUID()}`;
}

async function importPrivateKey(pem) {
  const body = String(pem).replace(/-----(BEGIN|END) PRIVATE KEY-----/g, '').replace(/\s+/g, '');
  const der = Uint8Array.from(atob(body), c => c.charCodeAt(0));
  return crypto.subtle.importKey('pkcs8', der, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']);
}

export class GcsError extends Error {
  constructor(step, status) {
    super(`GCS ${step} failed (${status})`);
    this.step = step;
    this.status = status;
  }
}

/**
 * @param {{bucket:string, credentials?:{client_email:string, private_key:string}, accessToken?:string, fetchImpl?:typeof fetch}} opts
 * credentials: service-account JSON (runtime signer). accessToken: short-lived token (e.g. GitHub OIDC workload identity).
 */
export function createGcs({ bucket, credentials, accessToken, fetchImpl = fetch }) {
  if (!/^[a-z0-9][a-z0-9._-]{2,62}$/.test(bucket || '')) throw new Error('Invalid GCS bucket name');
  let cached = accessToken ? { token: accessToken, exp: Infinity } : null;
  let keyPromise = null;
  const signingKey = () => (keyPromise ||= importPrivateKey(credentials.private_key));

  async function token() {
    if (cached && cached.exp - 60_000 > Date.now()) return cached.token;
    if (!credentials) throw new Error('No GCS credentials configured');
    const now = Math.floor(Date.now() / 1000);
    const header = b64url(enc.encode(JSON.stringify({ alg: 'RS256', typ: 'JWT' })));
    const claims = b64url(enc.encode(JSON.stringify({
      iss: credentials.client_email, scope: 'https://www.googleapis.com/auth/devstorage.read_write',
      aud: 'https://oauth2.googleapis.com/token', iat: now, exp: now + 3600,
    })));
    const sig = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', await signingKey(), enc.encode(`${header}.${claims}`));
    const res = await fetchImpl('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        assertion: `${header}.${claims}.${b64url(new Uint8Array(sig))}`,
      }),
    });
    if (!res.ok) throw new GcsError('token', res.status);
    const json = await res.json();
    cached = { token: json.access_token, exp: Date.now() + Number(json.expires_in || 3600) * 1000 };
    return cached.token;
  }

  const objectUrl = key => `${API}/storage/v1/b/${bucket}/o/${encodeURIComponent(key)}`;
  const checkKey = key => { if (!BRIPTA_GCS_KEY_PATTERN.test(key)) throw new Error('Unexpected object key shape'); };

  return {
    bucket,
    /** Create-only upload (ifGenerationMatch=0): never overwrites an existing object. */
    async uploadNew(key, bytes, contentType, sha256) {
      checkKey(key);
      const boundary = `bripta${crypto.randomUUID().replace(/-/g, '')}`;
      const meta = JSON.stringify({
        name: key, contentType, cacheControl: 'private, max-age=0, no-store',
        crc32c: crc32cBase64(bytes), metadata: { sha256 },
      });
      const head = enc.encode(`--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${meta}\r\n--${boundary}\r\nContent-Type: ${contentType}\r\n\r\n`);
      const tail = enc.encode(`\r\n--${boundary}--\r\n`);
      const body = new Uint8Array(head.length + bytes.byteLength + tail.length);
      body.set(head, 0); body.set(new Uint8Array(bytes), head.length); body.set(tail, head.length + bytes.byteLength);
      const res = await fetchImpl(`${API}/upload/storage/v1/b/${bucket}/o?uploadType=multipart&ifGenerationMatch=0`, {
        method: 'POST', body,
        headers: { Authorization: `Bearer ${await token()}`, 'Content-Type': `multipart/related; boundary=${boundary}` },
      });
      if (res.status === 412) return { exists: true };
      if (!res.ok) throw new GcsError('upload', res.status);
      const obj = await res.json();
      return { exists: false, generation: Number(obj.generation), size: Number(obj.size), sha256: obj.metadata?.sha256, crc32c: obj.crc32c };
    },
    async metadata(key) {
      checkKey(key);
      const res = await fetchImpl(objectUrl(key), { headers: { Authorization: `Bearer ${await token()}` } });
      if (res.status === 404) return null;
      if (!res.ok) throw new GcsError('metadata', res.status);
      const obj = await res.json();
      return { generation: Number(obj.generation), size: Number(obj.size), sha256: obj.metadata?.sha256 || null, crc32c: obj.crc32c, contentType: obj.contentType };
    },
    /** Reads a specific generation into memory (used only for checksum verification). */
    async download(key, generation) {
      checkKey(key);
      const res = await fetchImpl(`${objectUrl(key)}?alt=media&generation=${Number(generation)}`, {
        headers: { Authorization: `Bearer ${await token()}` },
      });
      if (!res.ok) throw new GcsError('download', res.status);
      return new Uint8Array(await res.arrayBuffer());
    },
    /** Deletes only the exact generation that was recorded. */
    async deleteGeneration(key, generation) {
      checkKey(key);
      const res = await fetchImpl(`${objectUrl(key)}?ifGenerationMatch=${Number(generation)}`, {
        method: 'DELETE', headers: { Authorization: `Bearer ${await token()}` },
      });
      if (res.status === 404) return 'missing';
      if (!res.ok) throw new GcsError('delete', res.status);
      return 'deleted';
    },
    /** V4 signed GET URL; requires service-account credentials. */
    async signedGetUrl(key, { expiresSeconds = 60, contentType, now = new Date() } = {}) {
      checkKey(key);
      if (!credentials) throw new Error('Signing requires service-account credentials');
      const expires = Math.min(Math.max(Math.floor(expiresSeconds), 1), 300);
      const iso = now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
      const date = iso.slice(0, 8);
      const scope = `${date}/auto/storage/goog4_request`;
      const host = 'storage.googleapis.com';
      const path = `/${bucket}/${encodeKeyPath(key)}`;
      const params = {
        'X-Goog-Algorithm': 'GOOG4-RSA-SHA256',
        'X-Goog-Credential': `${credentials.client_email}/${scope}`,
        'X-Goog-Date': iso,
        'X-Goog-Expires': String(expires),
        'X-Goog-SignedHeaders': 'host',
        'response-content-disposition': 'inline',
      };
      if (contentType) params['response-content-type'] = contentType;
      const query = Object.keys(params).sort().map(k => `${rfc3986(k)}=${rfc3986(params[k])}`).join('&');
      const canonical = ['GET', path, query, `host:${host}`, '', 'host', 'UNSIGNED-PAYLOAD'].join('\n');
      const stringToSign = ['GOOG4-RSA-SHA256', iso, scope, await sha256Hex(enc.encode(canonical))].join('\n');
      const sig = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', await signingKey(), enc.encode(stringToSign));
      return `https://${host}${path}?${query}&X-Goog-Signature=${hex(sig)}`;
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
