import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';

const db=new PGlite();
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const biz='BIZ-B3F5E5D9';
const sqlFile=name=>readFileSync(new URL('../'+name,import.meta.url),'utf8');
const hash=c=>c.repeat(64);
const path=(client,kind,c,ext='jpg')=>`${biz}/${id(client)}/${kind}/${hash(c)}.${ext}`;
const key=n=>`bripta/docs/v1/${id(n)}`;
const rejects=async(promise,pattern)=>{await assert.rejects(promise,pattern);};

await db.exec(`create role authenticated;create role anon;create role service_role bypassrls;
  create schema auth;create schema storage;
  create function auth.jwt() returns jsonb language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb$$;
  create function auth.uid() returns uuid language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claim.sub',true),''),auth.jwt()->>'sub')::uuid$$;
  create function auth.role() returns text language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claim.role',true),''),auth.jwt()->>'role')$$;
  grant usage on schema public,auth,storage to authenticated,anon,service_role;
  create table loan_staff(id uuid primary key,auth_user_id uuid,email text,name text,role text,business_id text,branch_id uuid,is_active boolean);
  create table loan_clients(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid);
  create table loans(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,client_id uuid);
  create table storage.buckets(id text primary key,name text,public boolean,file_size_limit bigint,allowed_mime_types text[]);
  create table storage.objects(id uuid primary key default gen_random_uuid(),bucket_id text,name text,metadata jsonb,created_at timestamptz default now());
  alter table storage.objects enable row level security;
  grant all on storage.objects to authenticated,anon,service_role;
  create policy old_broad_storage on storage.objects for all to authenticated,anon using(true) with check(true);
  create table bripta_domain_audit(business_id text,branch_id uuid,actor_staff_id uuid,action text,entity_type text,entity_id text,old_value jsonb,new_value jsonb);
  grant all on all tables in schema public to service_role;
  alter default privileges in schema public grant all on tables to service_role;
  alter default privileges in schema public grant all on sequences to service_role;`);
for(const[n,role,business,branch]of [[11,'admin',biz,1],[12,'loan_officer',biz,1],[13,'branch_manager',biz,2],[14,'cashier',biz,1],[15,'admin','OTHER',9],[16,'loan_officer',biz,2]])
  await db.query('insert into loan_staff values($1,$1,$2,$2,$3,$4,$5,true)',[id(n),`u${n}@test.invalid`,role,business,id(branch)]);
for(const[n,branch,owner,loan]of [[21,1,12,true],[22,2,16,true],[24,1,12,false]]){
  await db.query('insert into loan_clients values($1,$2,$3,$4)',[id(n),biz,id(branch),id(owner)]);
  if(loan)await db.query('insert into loans values($1,$2,$3,$4,$5)',[id(n+10),biz,id(branch),id(owner),id(n)]);
}
const login=async n=>{
  await db.exec('reset role');
  await db.query("select set_config('request.jwt.claims',$1,false)",[JSON.stringify(n==='service'?{role:'service_role'}:n?{sub:id(n),email:`u${n}@test.invalid`,role:'authenticated'}:{})]);
  await db.exec(n==='service'?'set role service_role':n?'set role authenticated':'set role anon');
};
const owner=async()=>{await db.exec('reset role');await db.query("select set_config('request.jwt.claims','',false)");};
const putObject=async(p,size=1000)=>{await owner();await db.query("insert into storage.objects(bucket_id,name,metadata) values('bripta-client-documents',$1,$2)",[p,{size:String(size),mimetype:'image/jpeg'}]);};
const one=async(q,p=[])=>(await db.query(q,p)).rows[0];

await db.exec(sqlFile('bripta-client-documents-20261005.sql'));
// Legacy documents uploaded by staff before the migration.
for(const[client,kind,c]of [[21,'client_passport','a'],[21,'client_id_front','b'],[22,'client_passport','c'],[24,'client_passport','d']]){
  await putObject(path(client,kind,c));
  await login(client===22?16:12);
  await db.query('insert into bripta_client_documents(client_id,kind,business_id,object_path,file_name,mime_type,file_size) values($1,$2,$3,$4,$5,$6,1000)',[id(client),kind,biz,path(client,kind,c),'x.jpg','image/jpeg']);
}
await putObject(`${biz}/${id(21)}/client_id_back/${hash('e')}.jpg`,777); // previously unlinked
await owner();
await db.query("insert into storage.objects(id,bucket_id,name,metadata) values('5afe3fad-e5e2-409c-88ba-6d9c5fe36aa0','bripta-client-documents',$1,$2)",
  [`${biz}/${id(21)}/guarantor_passport/${hash('d')}.jpg`,{size:'195473',mimetype:'image/jpeg'}]); // failed optimizer's staged copy
await owner();
// Read-only preflight runs against the pre-migration schema (incl. the failed optimizer's tables).
await db.exec(sqlFile('bripta-cloud-document-optimizer-20261121.sql'));
await db.exec(`create table bripta_document_compression_audit(object_id uuid,client_id uuid,kind text,old_path text,new_path text,old_deleted_at timestamptz);`);
const preflight=(await db.query(sqlFile('bripta-gcs-preflight-audit-20261122.sql'))).rows;
const totals=preflight.find(r=>r.section==='1 totals').details;
assert.equal(totals.linked_files,4);assert.equal(totals.unlinked_files,2);assert.equal(totals.broken_links,0);
assert.ok(preflight.some(r=>r.section==='7 live function'&&r.kind==='bripta_can_access_client_documents'));
await db.exec(sqlFile('bripta-gcs-documents-20261122.sql'));
await db.exec(sqlFile('bripta-gcs-documents-20261122.sql')); // idempotent
assert.deepEqual((await db.query('select linked_at_baseline,count(*)::int n from bripta_document_storage_baseline group by 1 order by 1')).rows,
  [{linked_at_baseline:false,n:2},{linked_at_baseline:true,n:4}]);
assert.equal((await one("select count(*)::int n from bripta_client_documents where storage_provider='supabase'")).n,4);

// Legacy staff uploads keep working and stay on Supabase.
await putObject(path(21,'guarantor_passport','f'));
await login(12);
await db.query('insert into bripta_client_documents(client_id,kind,business_id,object_path,file_name,mime_type,file_size) values($1,$2,$3,$4,$5,$6,1000)',[id(21),'guarantor_passport',biz,path(21,'guarantor_passport','f'),'g.jpg','image/jpeg']);
// A browser cannot point a record at Google Cloud: the trigger forces it back to Supabase.
await db.query("update bripta_client_documents set storage_provider='gcs',gcs_bucket='b',gcs_object_key=$1,gcs_generation=1,sha256=$2 where client_id=$3 and kind='client_passport'",[key(1),hash('a'),id(21)]);
assert.deepEqual(await one("select storage_provider,gcs_object_key from bripta_client_documents where client_id=$1 and kind='client_passport'",[id(21)]),{storage_provider:'supabase',gcs_object_key:null});

// Server RPCs are service-role only.
await login(12);
await rejects(db.query("select * from bripta_document_lookup($1,$2,$3,'client_passport')",[id(12),'u12@test.invalid',id(21)]),/permission denied/);
await rejects(db.query('select * from bripta_gcs_migration_claim(5,$1)',['bripta-docs-test']),/permission denied/);
await login(11);
await rejects(db.query('select bripta_document_approve_cleanup($1,$2)',['previously_unlinked','x']),/permission denied/);

// Access rules: the LIVE bripta_can_access_client_documents decides, as the named staff member.
await login('service');
const lookup=(staff,client,kind='client_passport')=>db.query('select * from bripta_document_lookup($1,$2,$3,$4)',[id(staff),`u${staff}@test.invalid`,id(client),kind]);
assert.equal((await lookup(12,21)).rows.length,1);  // assigned officer, same branch
assert.equal((await lookup(11,22)).rows.length,1);  // admin
assert.equal((await lookup(13,22)).rows.length,1);  // branch manager of branch 2
for(const[staff,client]of [[16,21],[14,21],[15,21],[13,21],[99,21]])
  await rejects(lookup(staff,client),/Not authorized/);
assert.equal((await one('select auth.role() r')).r,'service_role'); // claims restored after impersonation

// Migration state machine.
const claimed=(await db.query('select * from bripta_gcs_migration_claim(50,$1)',['bripta-docs-test'])).rows;
assert.equal(claimed.length,5);
assert.ok(claimed.every(r=>/^bripta\/docs\/v1\/[0-9a-f-]{36}$/.test(r.gcs_object_key)&&!r.gcs_object_key.includes(id(21))));
assert.equal((await db.query('select * from bripta_gcs_migration_claim(50,$1)',['bripta-docs-test'])).rows.length,5); // no duplicates
const row=claimed.find(r=>r.client_id===id(21)&&r.kind==='client_passport');
await rejects(db.query('select bripta_gcs_migration_mark_copied($1,$2,7,999)',[row.id,hash('a')]),/does not match/);
assert.equal((await db.query('select bripta_gcs_migration_switch($1) s',[row.id])).rows[0].s,'not_copied');
assert.equal((await one('select bripta_gcs_migration_mark_copied($1,$2,7,1000) ok',[row.id,hash('a')])).ok,true);
assert.equal((await one('select bripta_gcs_migration_switch($1) s',[row.id])).s,'switched');
assert.equal((await one('select bripta_gcs_migration_switch($1) s',[row.id])).s,'already_switched');
let doc=await one("select * from bripta_client_documents where client_id=$1 and kind='client_passport'",[id(21)]);
assert.equal(doc.storage_provider,'gcs');assert.equal(doc.object_path,path(21,'client_passport','a'));assert.equal(Number(doc.gcs_generation),7);
assert.equal((await one("select action from bripta_domain_audit where entity_id=$1 order by ctid desc limit 1",[`${id(21)}/client_passport`])).action,'document_storage_migrated');
assert.equal((await one("select bripta_gcs_migration_record($1,'verified',null) ok",[row.id])).ok,true);
assert.equal((await one('select gcs_object_key from bripta_gcs_migration_target($1)',[row.id])).gcs_object_key,row.gcs_object_key);
// Staff who may see the client still get the record; others still cannot.
assert.equal((await lookup(12,21)).rows[0].storage_provider,'gcs');
await rejects(lookup(16,21),/Not authorized/);

// A document replaced after claim is skipped, never overwritten.
const front=claimed.find(r=>r.client_id===id(21)&&r.kind==='client_id_front');
await db.query('select bripta_gcs_migration_mark_copied($1,$2,8,1000)',[front.id,hash('b')]);
await putObject(path(21,'client_id_front','1'));
await login(12);
await db.query("update bripta_client_documents set object_path=$1 where client_id=$2 and kind='client_id_front'",[path(21,'client_id_front','1'),id(21)]);
await login('service');
assert.equal((await one('select bripta_gcs_migration_switch($1) s',[front.id])).s,'document_changed');

// Failure + rollback: link returns to the Supabase original.
const other=claimed.find(r=>r.client_id===id(22));
await db.query('select bripta_gcs_migration_mark_copied($1,$2,9,1000)',[other.id,hash('c')]);
await db.query('select bripta_gcs_migration_switch($1)',[other.id]);
assert.equal((await one('select bripta_gcs_migration_rollback($1) ok',[other.id])).ok,true);
assert.equal((await one("select storage_provider from bripta_client_documents where client_id=$1",[id(22)])).storage_provider,'supabase');
await db.query("select bripta_gcs_migration_record($1,'failed','app_verify_502')",[other.id]);
assert.ok((await db.query('select * from bripta_gcs_migration_claim(50,$1)',['bripta-docs-test'])).rows.some(r=>r.id===other.id&&r.state==='failed'));

// New uploads through the server function go to GCS with optimistic concurrency.
const commit=(staff,client,kind,expected,n,c)=>db.query('select * from bripta_document_gcs_commit($1,$2,$3,$4,$5,$6,$7,$8,$9,500,$10,$11)',
  [id(staff),`u${staff}@test.invalid`,id(client),kind,expected,'bripta-docs-test',key(n),n,hash(c),'image/jpeg','scan.jpg']);
await rejects(commit(16,21,'client_id_back','',101,'2'),/Not authorized/);
assert.equal((await commit(12,21,'client_id_back','',101,'2')).rows[0].previous_gcs_key,null);
doc=await one("select * from bripta_client_documents where client_id=$1 and kind='client_id_back'",[id(21)]);
assert.equal(doc.storage_provider,'gcs');assert.equal(doc.object_path,null);assert.equal(doc.uploaded_by,id(12));
await rejects(commit(12,21,'client_id_back','',102,'3'),/changed while uploading/);
assert.equal((await commit(12,21,'client_id_back',key(101),102,'3')).rows[0].previous_gcs_key,key(101));
// Replacing a Supabase original queues it for approved cleanup instead of deleting it.
await commit(12,21,'guarantor_passport',path(21,'guarantor_passport','f'),103,'4');
assert.equal((await one("select reason from bripta_document_object_queue where object_key=$1",[path(21,'guarantor_passport','f')])).reason,'replaced_by_staff');

// A cached old browser replacing a GCS document is forced back to Supabase and the GCS copy is queued.
await putObject(path(21,'client_id_back','5'));
await login(12);
await db.query("update bripta_client_documents set object_path=$1 where client_id=$2 and kind='client_id_back'",[path(21,'client_id_back','5'),id(21)]);
await login('service');
doc=await one("select * from bripta_client_documents where client_id=$1 and kind='client_id_back'",[id(21)]);
assert.equal(doc.storage_provider,'supabase');assert.equal(doc.gcs_object_key,null);
assert.equal((await one("select reason from bripta_document_object_queue where object_key=$1",[key(102)])).reason,'legacy_replaced_gcs');

// Permanent client deletion: admin only, no loans, documents queued for removal in one transaction.
await rejects(db.query('select * from bripta_delete_client_with_documents($1,$2,$3)',[id(12),'u12@test.invalid',id(24)]),/Only admins/);
await rejects(db.query('select * from bripta_delete_client_with_documents($1,$2,$3)',[id(11),'u11@test.invalid',id(21)]),/existing loans/);
const removed=(await db.query('select * from bripta_delete_client_with_documents($1,$2,$3)',[id(11),'u11@test.invalid',id(24)])).rows;
assert.deepEqual(removed.map(r=>r.provider),['supabase']);
assert.equal((await one('select count(*)::int n from loan_clients where id=$1',[id(24)])).n,0);
assert.equal((await one('select bripta_document_queue_done($1) ok',[removed[0].queue_id])).ok,false); // object not yet removed
await owner();await db.query('delete from storage.objects where name=$1',[removed[0].object_key]);
await login('service');
assert.equal((await one('select bripta_document_queue_done($1) ok',[removed[0].queue_id])).ok,true);

// Cleanup manifest, owner approval by exact digest, and deletion confirmation.
await putObject(`${biz}/${id(21)}/client_id_back/${hash('6')}.jpg`,55); // appears after baseline
await owner();
const cats=Object.fromEntries((await db.query('select category,count(*)::int n from bripta_document_cleanup_manifest group by 1')).rows.map(r=>[r.category,r.n]));
assert.equal(cats.migrated_original,1);assert.equal(cats.previously_unlinked,1);assert.equal(cats.optimizer_staged,1);assert.ok(cats.still_linked_original>=1);assert.ok(cats.uncertain>=1);
await rejects(db.query("select bripta_document_approve_cleanup('migrated_original','wrong')"),/Manifest changed/);
const digest=(await one("select bripta_document_manifest_digest('migrated_original') d")).d;
assert.equal((await one("select bripta_document_approve_cleanup('migrated_original',$1) n",[digest])).n,1);
await login('service');
const next=(await db.query('select * from bripta_document_cleanup_next(20)')).rows;
assert.equal(next.length,1);assert.equal(next[0].gcs_object_key,row.gcs_object_key);assert.equal(next[0].sha256,hash('a'));
assert.equal((await one('select bripta_document_cleanup_mark($1,null) ok',[next[0].object_id])).ok,false); // still present
await owner();await db.query('delete from storage.objects where id=$1',[next[0].object_id]);
await login('service');
assert.equal((await one('select bripta_document_cleanup_mark($1,null) ok',[next[0].object_id])).ok,true);
assert.equal((await lookup(12,21)).rows[0].storage_provider,'gcs'); // still opens from GCS

// Read-only status + manifest queries run cleanly.
await owner();
const status=Object.fromEntries((await db.query(sqlFile('bripta-gcs-status-20261122.sql'))).rows.map(r=>[r.section,r.details]));
assert.equal(status.documents.on_gcs_migrated,1);assert.equal(status['broken links'].supabase_rows_missing_object,0);
assert.equal(status['broken links'].gcs_rows_without_verified_ledger,0);
await db.exec(sqlFile('bripta-gcs-cleanup-manifest-20261122.sql'));
// Phase 3: browsers can no longer add files to the Supabase document bucket.
await db.exec(sqlFile('bripta-gcs-stop-supabase-uploads-20261122.sql'));
await db.exec('drop policy old_broad_storage on storage.objects');
await db.exec('create policy reads on storage.objects for select to authenticated using(true)');
await login(12);
await rejects(db.query("insert into storage.objects(bucket_id,name,metadata) values('bripta-client-documents',$1,'{}')",[path(21,'client_id_back','7')]),/row-level security|permission/);
await owner();

// Rollback script: migrated documents whose original is gone stay on GCS; others revert.
await db.exec(sqlFile('bripta-gcs-documents-rollback-20261122.sql'));
assert.equal((await one("select storage_provider from bripta_client_documents where client_id=$1 and kind='client_passport'",[id(21)])).storage_provider,'gcs');
assert.equal((await one("select count(*)::int n from bripta_gcs_document_migration where state in ('claimed','copied','failed')")).n,0);
console.log('PASS: Bripta GCS document schema, access checks, migration state machine, cleanup approval and rollback.');
