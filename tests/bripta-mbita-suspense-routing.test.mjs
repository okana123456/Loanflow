import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';

const db=new PGlite();
const biz='BIZ-B3F5E5D9';
const migori='00000000-0000-4000-8000-000000000001';
const mbita='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
try{
  await db.exec(`create table bripta_branches(id uuid primary key,business_id text,name text,is_head_office boolean,is_active boolean);
    create table loan_settings(business_id text,mpesa_shortcode text);
    create table loan_staff(id uuid primary key,business_id text,branch_id uuid,name text,role text,is_active boolean);
    create table loan_clients(id uuid primary key,business_id text,branch_id uuid,phone text);
    create table loan_repayments(id uuid primary key,business_id text,payment_reference text,receipt_no text);
    create table mpesa_callback_queue(id uuid primary key,business_short_code text,branch_id uuid,
      bill_ref_number text,confirmed boolean,dismissed boolean,trans_id text,trans_amount numeric);
    create table unmatched_payments(id uuid primary key,business_id text,branch_id uuid,account_number text,
      resolved boolean,dismissed boolean,mpesa_reference text,amount numeric);
    create table bripta_accounting_entries(business_id text,branch_id uuid,source_table text,source_id text,
      debit numeric,credit numeric);
    insert into bripta_branches values
      ('${migori}','${biz}','Migori',true,true),('${mbita}','${biz}','MBITA  BRANCH',false,true),
      ('${id(9)}','OTHER','Other',true,true);
    insert into loan_settings values('${biz}','4044341'),('OTHER','5555');
    insert into loan_staff values
      ('${id(41)}','${biz}','${migori}','Brian Ochieng','loan_officer',true),
      ('${id(42)}','${biz}','${migori}','Faith','loan_officer',true),
      ('${id(43)}','${biz}','${migori}','Juma','loan_officer',true),
      ('${id(44)}','${biz}','${migori}','Phelix','loan_officer',true),
      ('${id(45)}','${biz}','${mbita}','George','loan_officer',true),
      ('${id(46)}','${biz}','${mbita}','James','loan_officer',true),
      ('${id(47)}','${biz}','${migori}','Admin','admin',true);
    create function bripta_is_suspense_business(p_code text) returns boolean language sql stable as $$
      select p_code='${biz}' or p_code='4044341' $$;
    insert into loan_clients values
      ('${id(11)}','${biz}','${mbita}','0712345678'),
      ('${id(12)}','${biz}','${migori}','0712349999'),
      ('${id(13)}','${biz}','${migori}','0700000000'),
      ('${id(14)}','${biz}','${mbita}','0700000000');
    insert into mpesa_callback_queue values
      ('${id(21)}','${biz}','${migori}','0712345678',false,false,'MBITA-OLD',150),
      ('${id(22)}','${biz}','${migori}','0712349999',false,false,'MIGORI-OLD',200),
      ('${id(23)}','${biz}','${migori}','0700000000',false,false,'AMBIGUOUS',50),
      ('${id(24)}','${biz}','${migori}','unknown',false,false,'UNKNOWN',40),
      ('${id(25)}','${biz}','${migori}','0712345678',true,false,'POSTED',100),
      ('${id(26)}','OTHER','${id(9)}','0712345678',false,false,'OTHER',300);
    insert into unmatched_payments values
      ('${id(31)}','${biz}','${migori}','0712345678',false,false,'MANUAL',75);
    insert into bripta_accounting_entries values
      ('${biz}','${migori}','unmatched_payments','${id(31)}',75,0),
      ('${biz}','${migori}','unmatched_payments','${id(31)}',0,75);
  `);
  const before=(await db.query(`select
    (select sum(trans_amount) from mpesa_callback_queue) callback_total,
    (select sum(amount) from unmatched_payments) manual_total,
    (select sum(debit-credit) from bripta_accounting_entries) journal_net`)).rows[0];
  const sql=readFileSync(new URL('../bripta-mbita-suspense-routing-20261027.sql',import.meta.url),'utf8');
  await db.exec(sql);await db.exec(sql);
  const rows=(await db.query('select trans_id,branch_id from mpesa_callback_queue')).rows;
  for(const ref of ['MBITA-OLD'])assert.equal(rows.find(r=>r.trans_id===ref).branch_id,mbita);
  for(const ref of ['MIGORI-OLD','AMBIGUOUS','UNKNOWN','POSTED'])
    assert.equal(rows.find(r=>r.trans_id===ref).branch_id,migori);
  assert.equal(rows.find(r=>r.trans_id==='OTHER').branch_id,id(9));
  await db.query(`insert into mpesa_callback_queue values($1,$2,$3,$4,false,false,$5,100)`,
    [id(27),biz,migori,'0712345678','MBITA-FUTURE']);
  assert.equal((await db.query('select branch_id from mpesa_callback_queue where id=$1',[id(27)])).rows[0].branch_id,mbita);
  assert.equal((await db.query('select branch_id from unmatched_payments where id=$1',[id(31)])).rows[0].branch_id,mbita);
  assert.equal((await db.query('select count(*)::int n from bripta_accounting_entries where branch_id=$1',[mbita])).rows[0].n,2);
  const after=(await db.query(`select
    (select sum(trans_amount)-100 from mpesa_callback_queue) callback_total,
    (select sum(amount) from unmatched_payments) manual_total,
    (select sum(debit-credit) from bripta_accounting_entries) journal_net`)).rows[0];
  assert.deepEqual(after,before);
  const check=await db.exec(readFileSync(new URL('../bripta-branch-accounting-suspense-check-20261027.sql',import.meta.url),'utf8'));
  const checks=check.at(-1).rows;
  assert.equal(Number(checks.find(r=>r.branch==='Migori'&&r.check_name==='current_loan_officers').result),4);
  assert.equal(Number(checks.find(r=>r.branch==='MBITA  BRANCH'&&r.check_name==='current_loan_officers').result),2);
  console.log('PASS: only unique Mbita phone accounts move, future receipts route correctly, totals unchanged, safe rerun.');
}finally{await db.close();}
