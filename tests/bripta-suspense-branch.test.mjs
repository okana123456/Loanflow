import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';

const db=new PGlite();
const sql=readFileSync(new URL('../bripta-fix-suspense-branch-20261002.sql',import.meta.url),'utf8');
const head='00000000-0000-4000-8000-000000000001';
const system='00000000-0000-4000-8000-000000000099';
try{
  await db.exec(`
    create table bripta_branches(id uuid primary key,business_id text,name text,is_head_office boolean);
    create table mpesa_callback_queue(id uuid primary key,business_short_code text,branch_id uuid,
      confirmed boolean,dismissed boolean,trans_id text,trans_amount numeric,repayment_id uuid,created_at timestamptz);
    insert into bripta_branches values('${head}','BIZ-B3F5E5D9','Migori',true),
      ('${system}','SYSTEM','System',true);
    create function bripta_assign_branch() returns trigger language plpgsql as $$
      begin if new.branch_id is null then new.branch_id:='${system}'; end if; return new; end $$;
    create trigger bripta_assign_branch_trg before insert on mpesa_callback_queue
      for each row execute function bripta_assign_branch();
    insert into mpesa_callback_queue values
      ('00000000-0000-4000-8000-000000000010','BIZ-B3F5E5D9',null,false,false,'PENDING',100,null,now()),
      ('00000000-0000-4000-8000-000000000011','BIZ-B3F5E5D9',null,true,false,'POSTED',200,null,now()),
      ('00000000-0000-4000-8000-000000000012','OTHER',null,false,false,'OTHER',300,null,now());
  `);
  await db.exec(sql);
  const rows=(await db.query('select trans_id,branch_id from mpesa_callback_queue order by trans_id')).rows;
  assert.equal(rows.find(x=>x.trans_id==='PENDING').branch_id,head);
  assert.equal(rows.find(x=>x.trans_id==='POSTED').branch_id,system);
  assert.equal(rows.find(x=>x.trans_id==='OTHER').branch_id,system);
  await db.exec(`insert into mpesa_callback_queue values
    ('00000000-0000-4000-8000-000000000013','BIZ-B3F5E5D9',null,false,false,'FUTURE',400,null,now()),
    ('00000000-0000-4000-8000-000000000014','OTHER',null,false,false,'OTHER-FUTURE',500,null,now());`);
  assert.equal((await db.query("select branch_id from mpesa_callback_queue where trans_id='FUTURE'")).rows[0].branch_id,head);
  assert.equal((await db.query("select branch_id from mpesa_callback_queue where trans_id='OTHER-FUTURE'")).rows[0].branch_id,system);
  await db.exec(sql);
  assert.equal((await db.query("select branch_id from mpesa_callback_queue where trans_id='PENDING'")).rows[0].branch_id,head);
  console.log('PASS: pending Bripta callback backfill, future branch assignment, other businesses and confirmed history untouched, safe rerun.');
}finally{await db.close();}
