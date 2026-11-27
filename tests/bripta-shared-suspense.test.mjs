import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';

const db=new PGlite();
const migori='00000000-0000-4000-8000-000000000001';
const mbita='00000000-0000-4000-8000-000000000002';
try{
  await db.exec(`
    create role anon; create role authenticated;
    create schema auth;
    create function auth.uid() returns uuid language sql stable as $$
      select nullif(current_setting('app.user_id',true),'')::uuid $$;
    create function auth.jwt() returns jsonb language sql stable as $$
      select '{}'::jsonb $$;
    create table loan_staff(id uuid primary key,business_id text,is_active boolean,
      auth_user_id uuid,role text,email text,branch_id uuid);
    create table loan_settings(business_id text,mpesa_shortcode text);
    create table mpesa_callback_queue(id uuid primary key,business_short_code text,
      confirmed boolean,dismissed boolean,branch_id uuid,trans_id text,
      trans_amount numeric,created_at timestamptz);
    create table unmatched_payments(id uuid primary key,business_id text,resolved boolean,
      dismissed boolean,branch_id uuid,mpesa_reference text,amount numeric);
    create table loan_repayments(id uuid primary key,business_id text,
      payment_reference text,receipt_no text);
    create function bripta_is_suspense_business(code text) returns boolean
      language sql stable as $$ select code in ('BIZ-B3F5E5D9','123456') $$;
    insert into loan_staff values
      ('10000000-0000-4000-8000-000000000001','BIZ-B3F5E5D9',true,
       '20000000-0000-4000-8000-000000000001','branch_manager','mbita@example.test','${mbita}'),
      ('10000000-0000-4000-8000-000000000002','BIZ-OTHER',true,
       '20000000-0000-4000-8000-000000000002','admin','other@example.test','${migori}');
    insert into loan_settings values ('BIZ-B3F5E5D9','123456');
    insert into mpesa_callback_queue values
      ('30000000-0000-4000-8000-000000000001','BIZ-B3F5E5D9',false,false,'${migori}','MIGORI',100,now()),
      ('30000000-0000-4000-8000-000000000002','123456',false,false,'${mbita}','MBITA',200,now()),
      ('30000000-0000-4000-8000-000000000003','BIZ-OTHER',false,false,'${migori}','OTHER',300,now()),
      ('30000000-0000-4000-8000-000000000004','123456',true,false,'${mbita}','PAID',400,now());
    insert into unmatched_payments values
      ('40000000-0000-4000-8000-000000000001','BIZ-B3F5E5D9',false,false,'${migori}','MANUAL-M',50),
      ('40000000-0000-4000-8000-000000000002','BIZ-B3F5E5D9',false,false,'${mbita}','MANUAL-B',75),
      ('40000000-0000-4000-8000-000000000003','BIZ-OTHER',false,false,'${migori}','OTHER-M',90);
  `);
  await db.exec(readFileSync(new URL('../bripta-shared-suspense-reader-20261127.sql',import.meta.url),'utf8'));
  await db.exec("select set_config('app.user_id','20000000-0000-4000-8000-000000000001',false)");
  for(const source of ['queue','manual']){
    const result=await db.query(`select bripta_suspense_page('${source}','${mbita}',null,500) as page`);
    const rows=result.rows[0].page.rows;
    assert.equal(rows.length,2,`${source} includes both Bripta branches`);
    assert.deepEqual(new Set(rows.map(row=>row.branch_id)),new Set([migori,mbita]));
  }
  await db.exec("select set_config('app.user_id','20000000-0000-4000-8000-000000000002',false)");
  await assert.rejects(db.query("select bripta_suspense_page('queue',null,null,500)"),/Suspense access is not permitted/);
  console.log('PASS: one Bripta Suspense register shows both branches, excludes confirmed and other-business records, and rejects outsider staff.');
}finally{await db.close();}
