import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const db=new PGlite();
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const biz='BIZ-B3F5E5D9';
const migration=name=>readFileSync(new URL('../'+name,import.meta.url),'utf8');
try{
  await db.exec(`create role authenticated;create role anon;create role service_role;
    create schema auth;create schema storage;
    create function auth.jwt() returns jsonb language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb$$;
    create function auth.uid() returns uuid language sql stable as $$select (auth.jwt()->>'sub')::uuid$$;
    grant usage on schema public,auth,storage to authenticated,anon;
    create table loan_staff(id uuid primary key,auth_user_id uuid,email text,name text,role text,business_id text,branch_id uuid,is_active boolean);
    create table bripta_branches(id uuid primary key,business_id text,is_head_office boolean);
    create table loan_settings(business_id text,mpesa_shortcode text);
    create table loan_clients(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid);
    create table loans(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,client_id uuid,outstanding_balance numeric);
    create table loan_repayments(id uuid primary key,business_id text,loan_id uuid,payment_reference text,receipt_no text,amount numeric);
    create table mpesa_callback_queue(id uuid primary key,business_short_code text,branch_id uuid,confirmed boolean,dismissed boolean,
      trans_id text,trans_amount numeric,created_at timestamptz default now());
    create table unmatched_payments(id uuid primary key,business_id text,branch_id uuid,resolved boolean,dismissed boolean,
      mpesa_reference text,amount numeric,created_at timestamptz default now());
    create table storage.buckets(id text primary key,name text,public boolean,file_size_limit bigint,allowed_mime_types text[]);
    create table storage.objects(id uuid primary key default gen_random_uuid(),bucket_id text,name text);
    alter table storage.objects enable row level security;
    grant all on storage.objects to authenticated,anon;
    create policy old_broad_storage on storage.objects for all to authenticated,anon using(true) with check(true);
    create table bripta_domain_audit(business_id text,branch_id uuid,actor_staff_id uuid,action text,entity_type text,entity_id text,old_value jsonb,new_value jsonb);
  `);
  for(const[n,role,business,branch]of [[11,'admin',biz,1],[12,'loan_officer',biz,1],[13,'branch_manager',biz,2],[14,'cashier',biz,1],[15,'admin','OTHER',9],[16,'loan_officer',biz,2],[17,'loan_officer',biz,1]]){
    await db.query('insert into loan_staff values($1,$1,$2,$2,$3,$4,$5,true)',[id(n),`u${n}@test.invalid`,role,business,id(branch)]);
  }
  for(const[n,business,head]of [[1,biz,true],[2,biz,false],[9,'OTHER',true]])await db.query('insert into bripta_branches values($1,$2,$3)',[id(n),business,head]);
  await db.exec(`insert into loan_settings values('${biz}','4044341'),('OTHER','5555');`);
  for(const[n,business,branch,owner]of [[21,biz,1,12],[22,biz,2,16],[23,'OTHER',9,15]]){
    await db.query('insert into loan_clients values($1,$2,$3,$4)',[id(n),business,id(branch),id(owner)]);
    await db.query('insert into loans values($1,$2,$3,$4,$5,1000)',[id(n+10),business,id(branch),id(owner),id(n)]);
  }
  for(const[n,code,branch,confirmed,dismissed,ref]of [
    [41,biz,null,null,null,'LEGACY-NULL'],[42,'4044341',9,false,false,'NUMERIC-PAYBILL'],
    [43,biz,2,false,false,'OTHER-BRANCH'],[44,biz,1,true,false,'CONFIRMED'],
    [45,biz,1,false,true,'DISMISSED'],[46,'OTHER',9,false,false,'OTHER-BIZ'],
    [47,biz,1,false,false,'ALREADY-PAID'],[48,biz,1,false,false,'RESOLVED-MANUAL']
  ])await db.query('insert into mpesa_callback_queue(id,business_short_code,branch_id,confirmed,dismissed,trans_id,trans_amount) values($1,$2,$3,$4,$5,$6,100)',[id(n),code,branch?id(branch):null,confirmed,dismissed,ref]);
  await db.query('insert into loan_repayments values($1,$2,$3,$4,$4,100)',[id(71),biz,id(31),'ALREADY-PAID']);
  await db.query('insert into unmatched_payments(id,business_id,branch_id,resolved,mpesa_reference,amount) values($1,$2,$3,true,$4,100)',[id(72),biz,id(1),'RESOLVED-MANUAL']);
  await db.query('insert into unmatched_payments(id,business_id,branch_id,resolved,mpesa_reference,amount) values($1,$2,null,null,$3,200)',[id(73),biz,'MANUAL']);
  const before=(await db.query('select sum(trans_amount)::numeric v from mpesa_callback_queue union all select sum(amount) from loan_repayments union all select sum(outstanding_balance) from loans')).rows;
  for(const file of ['bripta-suspense-reader-20261005.sql','bripta-client-documents-20261005.sql']){
    await db.exec(migration(file));await db.exec(migration(file));
  }
  assert.deepEqual((await db.query('select sum(trans_amount)::numeric v from mpesa_callback_queue union all select sum(amount) from loan_repayments union all select sum(outstanding_balance) from loans')).rows,before);
  assert.equal((await db.query('select branch_id from mpesa_callback_queue where id=$1',[id(42)])).rows[0].branch_id,id(1));
  await db.query('insert into mpesa_callback_queue(id,business_short_code,trans_id,trans_amount) values($1,$2,$3,55)',[id(49),'4044341','FUTURE']);
  assert.equal((await db.query('select branch_id from mpesa_callback_queue where id=$1',[id(49)])).rows[0].branch_id,id(1));
  const login=async n=>{await db.exec('reset role');await db.query("select set_config('request.jwt.claims',$1,false)",[JSON.stringify(n?{sub:id(n),email:`u${n}@test.invalid`}:{})]);await db.exec(n?'set role authenticated':'set role anon');};
  const suspense=async(source='queue',branch=null)=>(await db.query('select bripta_suspense_page($1,$2) result',[source,branch])).rows[0].result.rows;
  await login(11);
  assert.deepEqual((await suspense()).map(x=>x.trans_id).sort(),['FUTURE','LEGACY-NULL','NUMERIC-PAYBILL','OTHER-BRANCH']);
  await login(12);
  assert.equal((await suspense()).length,3);
  assert.equal((await suspense('manual')).length,1);
  await assert.rejects(()=>suspense('queue',id(2)),/Branch access/);
  await login(13);assert.deepEqual((await suspense()).map(x=>x.trans_id),['OTHER-BRANCH']);
  await login(15);await assert.rejects(()=>suspense(),/not permitted/);
  await login(null);await assert.rejects(()=>suspense(),/permission denied/);
  console.log('PASS: legacy null flags, numeric Paybill, branch repair, future callbacks, settled exclusions and role isolation.');

  const kinds=['client_passport','client_id_front','client_id_back','guarantor_passport','guarantor_id_front','guarantor_id_back'];
  await login(12);
  for(const kind of kinds){
    const path=`${biz}/${id(21)}/${kind}/${id(80)}.jpg`;
    await db.query('insert into storage.objects(bucket_id,name) values($1,$2)',['bripta-client-documents',path]);
    await db.query('insert into bripta_client_documents(client_id,kind,object_path,file_name,mime_type,file_size) values($1,$2,$3,$4,$5,100)',[id(21),kind,path,'photo.jpg','image/jpeg']);
  }
  assert.equal((await db.query('select count(*)::int n from bripta_client_documents')).rows[0].n,6);
  await assert.rejects(()=>db.query('insert into storage.objects(bucket_id,name) values($1,$2)',['bripta-client-documents',`${biz}/${id(22)}/client_id_front/${id(81)}.jpg`]),/row-level security/);
  await assert.rejects(()=>db.query('update bripta_client_documents set object_path=$1 where kind=$2',[`${biz}/${id(21)}/client_id_back/${id(80)}.jpg`,'client_id_front']),/authorized client folder/);
  for(const user of [14,15,16,17]){
    await login(user);
    assert.equal((await db.query('select count(*)::int n from bripta_client_documents')).rows[0].n,0);
    assert.equal((await db.query("select count(*)::int n from storage.objects where bucket_id='bripta-client-documents'")).rows[0].n,0);
  }
  await login(null);
  assert.equal((await db.query("select count(*)::int n from storage.objects where bucket_id='bripta-client-documents'")).rows[0].n,0,'private boundary overrides old broad storage policy');
  await login(11);
  assert.equal((await db.query('select count(*)::int n from bripta_client_documents')).rows[0].n,6);
  await db.exec('reset role');
  assert.equal((await db.query('select count(*)::int n from bripta_domain_audit')).rows[0].n,6);
  assert.equal((await db.query("select public from storage.buckets where id='bripta-client-documents'")).rows[0].public,false);
  console.log('PASS: six private documents, own-portfolio access, other branches/businesses/cashiers/anonymous blocked, audit trail and idempotent SQL.');
}finally{await db.close();}
