import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
const edge=readFileSync(new URL('../supabase/functions/bripta-document-compression/index.ts',import.meta.url),'utf8');
const ui=readFileSync(new URL('../bripta-storage-cleanup.js',import.meta.url),'utf8');
const sql=readFileSync(new URL('../bripta-cloud-document-compression-20261121.sql',import.meta.url),'utf8');
assert.match(edge,/bripta_document_compression_candidates/);
assert.match(edge,/bripta_commit_document_compression/);
assert.match(edge,/bripta_pending_old_document_cleanup/);
assert.match(edge,/compressed\.byteLength<=950000/);
assert.match(edge,/bucket\.remove\(\[oldPath\]\)/);
assert.match(ui,/hasRole\('admin'\)/);
assert.match(ui,/No external backup will be kept/);
assert.match(sql,/auth\.uid\(\)/);
assert.match(sql,/s\.business_id='BIZ-B3F5E5D9'/);
const calls=[];let status='';
const context=vm.createContext({currentUser:{business_id:'BIZ-B3F5E5D9'},hasRole:role=>role==='admin',
  window:{confirm:()=>true},openModal:()=>{},document:{getElementById:id=>id==='briptaStorageStatus'?{set textContent(value){status=value;}}:null},
  supabaseClient:{rpc:async name=>({data:name==='bripta_pending_document_cleanup_ids'?[]:[{object_id:'92cf7113-b8ed-487b-b7b7-9fffb9258121',rank_no:1}]}),
    functions:{invoke:async(_name,{body})=>{calls.push(body.action);return {data:body.action==='stage'?{
      ok:true,object_id:body.object_id,original_bytes:5000000,compressed_bytes:900000,stage_path:'stage'
    }:{ok:true,saved_bytes:4100000}};}}}});
vm.runInContext(ui,context);
await context.runBriptaStorageBatch(1);
assert.deepEqual(calls,['stage','commit']);
assert.match(status,/1 replaced/);
console.log('PASS: Bripta cloud compression uses an audited manifest, admin approval and one-image verification.');
