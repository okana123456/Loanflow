import {generateKeyPairSync,createVerify,createHash} from 'node:crypto';
import {readFileSync,existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import {createGcs,crc32cBase64,newBriptaObjectKey,BRIPTA_GCS_KEY_PATTERN,validateDocument,sniffDocumentType,sha256Hex}
  from '../supabase/functions/bripta-documents/bripta-gcs.mjs';

const {privateKey,publicKey}=generateKeyPairSync('rsa',{modulusLength:2048});
const credentials={client_email:'signer@bripta-test.iam.gserviceaccount.com',private_key:privateKey.export({type:'pkcs8',format:'pem'})};
const key=newBriptaObjectKey();
assert.match(key,BRIPTA_GCS_KEY_PATTERN);
assert.notEqual(newBriptaObjectKey(),key);

// CRC32C standard check value for "123456789" is 0xE3069283.
assert.equal(crc32cBase64(new TextEncoder().encode('123456789')),Buffer.from('e3069283','hex').toString('base64'));
assert.equal(await sha256Hex(new TextEncoder().encode('abc')),createHash('sha256').update('abc').digest('hex'));

// V4 signed URL: reconstruct the string-to-sign and verify the RSA signature.
const gcs=createGcs({bucket:'bripta-docs-test',credentials,fetchImpl:()=>{throw new Error('no network');}});
const now=new Date('2026-11-22T10:11:12.345Z');
const signed=new URL(await gcs.signedGetUrl(key,{expiresSeconds:60,contentType:'image/jpeg',now}));
assert.equal(signed.host,'storage.googleapis.com');
assert.equal(signed.pathname,`/bripta-docs-test/${key}`);
assert.equal(signed.searchParams.get('X-Goog-Expires'),'60');
assert.equal(signed.searchParams.get('X-Goog-Date'),'20261122T101112Z');
assert.equal(signed.searchParams.get('X-Goog-SignedHeaders'),'host');
const sig=signed.searchParams.get('X-Goog-Signature');
const query=signed.search.slice(1).split('&').filter(p=>!p.startsWith('X-Goog-Signature=')).join('&');
const canonical=['GET',signed.pathname,query,'host:storage.googleapis.com','','host','UNSIGNED-PAYLOAD'].join('\n');
const stringToSign=['GOOG4-RSA-SHA256','20261122T101112Z','20261122/auto/storage/goog4_request',createHash('sha256').update(canonical).digest('hex')].join('\n');
assert.ok(createVerify('RSA-SHA256').update(stringToSign).verify(publicKey,Buffer.from(sig,'hex')));
assert.equal(new URL(await gcs.signedGetUrl(key,{expiresSeconds:86400,now})).searchParams.get('X-Goog-Expires'),'300'); // capped
await assert.rejects(gcs.signedGetUrl('BIZ-B3F5E5D9/client/name.jpg'),/Unexpected object key/);

// Token exchange, create-only upload with integrity metadata, exact-generation delete.
const calls=[];
const fake=async(url,init={})=>{
  calls.push({url:String(url),init});
  if(String(url).startsWith('https://oauth2.googleapis.com/token')){
    const assertion=new URLSearchParams(init.body).get('assertion').split('.');
    assert.ok(createVerify('RSA-SHA256').update(`${assertion[0]}.${assertion[1]}`).verify(publicKey,Buffer.from(assertion[2],'base64url')));
    return new Response(JSON.stringify({access_token:'tok',expires_in:3600}));
  }
  if(String(url).includes('/upload/')){
    const body=Buffer.from(init.body).toString('latin1');
    const meta=JSON.parse(body.split('\r\n\r\n')[1].split('\r\n')[0]);
    return new Response(JSON.stringify({generation:'42',size:String(3),metadata:meta.metadata,crc32c:meta.crc32c}));
  }
  if(init.method==='DELETE')return new Response(null,{status:204});
  return new Response('{}',{status:404});
};
const client=createGcs({bucket:'bripta-docs-test',credentials,fetchImpl:fake});
const up=await client.uploadNew(key,new Uint8Array([1,2,3]),'image/jpeg','a'.repeat(64));
assert.deepEqual({g:up.generation,s:up.size,sha:up.sha256},{g:42,s:3,sha:'a'.repeat(64)});
const upload=calls.find(c=>c.url.includes('/upload/'));
assert.match(upload.url,/ifGenerationMatch=0/);
assert.equal(upload.init.headers.Authorization,'Bearer tok');
assert.match(Buffer.from(upload.init.body).toString('latin1'),/"crc32c":"[A-Za-z0-9+/=]{8}"/);
assert.equal(calls.filter(c=>c.url.includes('oauth2')).length,1);
await client.deleteGeneration(key,42);
assert.match(calls.at(-1).url,/ifGenerationMatch=42$/);
assert.equal(await client.metadata(key),null);
const conflict=createGcs({bucket:'bripta-docs-test',accessToken:'t',fetchImpl:async()=>new Response('',{status:412})});
assert.deepEqual(await conflict.uploadNew(key,new Uint8Array([1]),'image/jpeg','b'.repeat(64)),{exists:true});
assert.throws(()=>createGcs({bucket:'Bad Bucket'}),/Invalid GCS bucket/);

// File type and size checks.
const jpeg=new Uint8Array([0xff,0xd8,0xff,0xe0,0,0,0,0,0,0,0,0]);
const pdf=new TextEncoder().encode('%PDF-1.7 xxxxx');
assert.equal(sniffDocumentType(jpeg),'image/jpeg');
assert.equal(validateDocument('client_passport',jpeg,'image/jpeg'),null);
assert.match(validateDocument('client_passport',pdf,'application/pdf'),/Passport/);
assert.equal(validateDocument('client_id_front',pdf,'application/pdf'),null);
assert.match(validateDocument('client_id_front',jpeg,'application/pdf'),/JPG/);
assert.match(validateDocument('client_id_front',new TextEncoder().encode('<html>'),'image/jpeg'),/JPG/);
assert.match(validateDocument('selfie',jpeg,'image/jpeg'),/Unknown/);
assert.match(validateDocument('client_id_front',new Uint8Array(8*1024*1024+1).fill(0xff),'image/jpeg'),/8 MB/);

// Static safety checks on what ships.
const read=p=>readFileSync(new URL('../'+p,import.meta.url),'utf8');
const edge=read('supabase/functions/bripta-documents/index.ts');
const browser=read('bripta-client-documents.js')+read('index.html');
const job=read('scripts/bripta-gcs-migrate-documents.mjs');
const workflow=read('.github/workflows/bripta-gcs-documents.yml');
assert.doesNotMatch(browser,/private_key|BRIPTA_GCS_SIGNER_KEY|storage\.googleapis\.com|service_role/i);
assert.match(browser,/action:'sign'/);assert.match(browser,/action:'delete_client'/);
for(const src of [edge,job])for(const line of src.split('\n').filter(l=>/console\.(log|error|warn)/.test(l)))
  assert.doesNotMatch(line,/url|signed|object_path|supabase_path|gcs_object_key|token|email|name/i,line.trim());
assert.match(edge,/bripta_document_lookup/);assert.match(edge,/deleteGeneration\(prev\.previous_gcs_key/);
assert.match(job,/sha256Hex\(back\)/);assert.match(job,/bripta_gcs_migration_rollback/);
assert.doesNotMatch(job,/writeFile|createWriteStream/);
assert.match(workflow,/id-token: write/);assert.doesNotMatch(workflow,/upload-artifact|credentials_json/);
assert.equal(existsSync(new URL('../.github/workflows/bripta-cloud-optimize-documents.yml',import.meta.url)),false);
console.log('PASS: Bripta GCS client signing, upload integrity, file checks and shipping safety checks.');
