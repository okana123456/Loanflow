import {readFileSync,existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import {sigV4,createB2,newBriptaObjectKey,BRIPTA_OBJECT_KEY_PATTERN,validateDocument,sniffDocumentType,EMPTY_SHA256}
  from '../supabase/functions/bripta-documents/bripta-b2.mjs';

// AWS Signature V4 published S3 examples (examplebucket, 24 May 2013).
const aws={region:'us-east-1',accessKeyId:'AKIAIOSFODNN7EXAMPLE',secretAccessKey:'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
  host:'examplebucket.s3.amazonaws.com',now:new Date('2013-05-24T00:00:00Z')};
const get=await sigV4({...aws,method:'GET',path:'/test.txt',headers:{range:'bytes=0-9'},payloadHash:EMPTY_SHA256});
assert.match(get.headers.authorization,/Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41$/);
assert.match(get.headers.authorization,/SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,/);
const put=await sigV4({...aws,method:'PUT',path:'/test$file.text',payloadHash:'44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072',
  headers:{date:'Fri, 24 May 2013 00:00:00 GMT','x-amz-storage-class':'REDUCED_REDUNDANCY'}});
assert.match(put.headers.authorization,/Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd$/);
const pre=await sigV4({...aws,method:'GET',path:'/test.txt',presignSeconds:86400});
assert.ok(pre.url.endsWith('X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404'),pre.url.slice(-80));

// Bripta client against a fake B2 endpoint.
const key=newBriptaObjectKey();
assert.match(key,BRIPTA_OBJECT_KEY_PATTERN);assert.notEqual(newBriptaObjectKey(),key);
const calls=[];
const fake=async(url,init={})=>{
  calls.push({url:String(url),init});
  const h=new Headers({'x-amz-version-id':'4_zVERSION','content-length':'3','x-amz-meta-sha256':'a'.repeat(64),'content-type':'image/jpeg'});
  if(init.method==='HEAD'&&String(url).includes('missing'))return new Response(null,{status:404});
  if(init.method==='GET')return new Response(new Uint8Array([1,2,3]),{headers:h});
  return new Response(init.method==='HEAD'?null:'',{status:200,headers:h});
};
const opts={endpoint:'s3.us-west-004.backblazeb2.com',bucket:'bripta-docs-02bf34dc',keyId:'004keyid',applicationKey:'secret-app-key',fetchImpl:fake};
const b2=createB2(opts);
const up=await b2.putObject(key,new Uint8Array([1,2,3]),'image/jpeg','a'.repeat(64));
assert.equal(up.version,'4_zVERSION');
const putCall=calls.at(-1);
assert.equal(putCall.url,`https://s3.us-west-004.backblazeb2.com/bripta-docs-02bf34dc/${key}`);
assert.equal(putCall.init.headers['x-amz-content-sha256'],'a'.repeat(64)); // B2 rejects bytes that do not match
assert.match(putCall.init.headers.authorization,/Credential=004keyid\/\d{8}\/us-west-004\/s3\/aws4_request/);
assert.equal(putCall.init.headers['x-amz-meta-sha256'],'a'.repeat(64));
assert.deepEqual(await b2.head(key),{size:3,sha256:'a'.repeat(64),version:'4_zVERSION',contentType:'image/jpeg'});
assert.deepEqual([...await b2.download(key,'4_zVERSION')],[1,2,3]);
assert.match(calls.at(-1).url,/\?versionId=4_zVERSION$/);
await b2.deleteVersion(key,'4_zVERSION');
assert.equal(calls.at(-1).init.method,'DELETE');assert.match(calls.at(-1).url,/versionId=4_zVERSION/);
await assert.rejects(b2.deleteVersion(key,null),/version is required/);
const link=new URL(await b2.signedGetUrl(key,{expiresSeconds:86400,contentType:'image/jpeg'}));
assert.equal(link.searchParams.get('X-Amz-Expires'),'300');
assert.equal(link.searchParams.get('response-content-type'),'image/jpeg');
assert.equal(new URL(await b2.signedGetUrl(key)).searchParams.get('X-Amz-Expires'),'60');
await assert.rejects(b2.signedGetUrl('BIZ-B3F5E5D9/client/name.jpg'),/Unexpected object key/);
assert.throws(()=>createB2({...opts,endpoint:'evil.example.com'}),/endpoint/);
assert.throws(()=>createB2({...opts,bucket:'Bad Bucket'}),/bucket/);
assert.throws(()=>createB2({...opts,applicationKey:''}),/key is missing/);

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
const job=read('scripts/bripta-b2-migrate-documents.mjs');
const workflow=read('.github/workflows/bripta-b2-documents.yml');
assert.doesNotMatch(browser,/applicationKey|BRIPTA_B2_|backblazeb2\.com|service_role/i);
assert.match(browser,/action:'sign'/);assert.match(browser,/action:'delete_client'/);
for(const src of [edge,job])for(const line of src.split('\n').filter(l=>/console\.(log|error|warn)/.test(l)))
  assert.doesNotMatch(line,/url|signed|object_path|supabase_path|ext_object_key|token|email|name|key\b/i,line.trim());
assert.match(edge,/bripta_document_lookup/);assert.match(edge,/deleteVersion\(prev\.previous_ext_key/);
assert.match(job,/sha256Hex\(back\)/);assert.match(job,/bripta_b2_migration_rollback/);
assert.doesNotMatch(job,/writeFile|createWriteStream/);
assert.doesNotMatch(workflow,/upload-artifact/);assert.match(workflow,/secrets\.BRIPTA_B2_MIGRATOR_APP_KEY/);
assert.equal(existsSync(new URL('../.github/workflows/bripta-cloud-optimize-documents.yml',import.meta.url)),false);
console.log('PASS: Backblaze B2 SigV4 (AWS test vectors), upload integrity, version-exact deletes, file checks and shipping safety checks.');
