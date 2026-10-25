import assert from 'node:assert/strict';
import fs from 'node:fs';
import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';

const db=new PGlite();
const migori='00000000-0000-4000-8000-000000000001';
const mbita='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338';
const george='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99';
const business='BIZ-B3F5E5D9';
const ids=[
  '5f44fc32-41f7-48c3-8eec-b71c6584c294','f8106a47-bba1-4585-8dff-5f7bd7fff99d',
  '33e3a535-7922-4624-82bf-7364642ed71b','0e9de4dc-fc33-41ea-bde6-99dfafb2a31b',
  '9bcf0987-10a0-4c24-b021-82de4c751d64','1252fba7-0ff8-42ba-8eed-238aa0214843',
  '3fd2c264-da0b-49b5-ac80-b74e09613d61','c038c0bf-1ce5-423c-9ec3-f82e259e123b',
  '3fce500c-5fb4-4d08-8aca-d73f81a7b1f3','76e1912c-85ec-4339-a783-270016b991ea'
];
const uuid=i=>`00000000-0000-4000-8000-${String(i).padStart(12,'0')}`;
await db.exec(`
create table bripta_branches(id uuid,business_id text,name text);
create table loan_staff(id uuid,business_id text,branch_id uuid);
create table bripta_staff_permissions(staff_id uuid,business_id text,branch_id uuid);
create table loan_clients(id uuid,business_id text,branch_id uuid,loan_officer_id uuid,updated_at timestamptz);
create table loans(id uuid,client_id uuid,application_id uuid,loan_no text,business_id text,loan_officer_id uuid,branch_id uuid,created_at timestamptz,updated_at timestamptz,total_paid numeric,outstanding_balance numeric,status text,arrears_amount numeric,overdue_days int);
create table loan_applications(id uuid,client_id uuid,business_id text,branch_id uuid,status text,updated_at timestamptz);
create table loan_schedules(id uuid,loan_id uuid,business_id text,branch_id uuid);
create table loan_repayments(id uuid,loan_id uuid,business_id text,branch_id uuid,amount numeric);
create function test_balance_recalculation() returns trigger language plpgsql as $$
begin update loans set total_paid=999 where id=new.loan_id; return new; end $$;
create trigger bripta_sync_loan_balance_trigger after update on loan_repayments
for each row execute function test_balance_recalculation();
create table loan_penalties(id uuid,loan_id uuid,business_id text,branch_id uuid);
create table bripta_charges(id uuid,loan_id uuid,business_id text,branch_id uuid);
create table bripta_excess_ledger(id uuid,loan_id uuid,business_id text,branch_id uuid);
create table bripta_excess_allocations(id uuid,excess_id uuid,business_id text,branch_id uuid);
create table journal_entries(id uuid,loan_id uuid,business_id text,branch_id uuid);
create table bripta_accounting_entries(id uuid,loan_id uuid,business_id text,branch_id uuid,source_table text,source_id text,debit numeric,credit numeric);
create table mpesa_callback_queue(id uuid,loan_id uuid,repayment_id uuid,branch_id uuid);
create table bripta_domain_audit(business_id text,branch_id uuid,actor_staff_id uuid,action text,entity_type text,new_value jsonb);
insert into bripta_branches values ('${migori}','${business}','Migori'),('${mbita}','${business}','MBITA BRANCH');
insert into loan_staff values ('${george}','${business}','${migori}');
insert into bripta_staff_permissions values ('${george}','${business}','${migori}');
`);
for(let i=0;i<10;i++){
  const client=uuid(i+20),app=uuid(i+40);
  await db.query(`insert into loan_clients values ($1,$2,$3,$4,now())`,[client,business,migori,george]);
  await db.query(`insert into loan_applications values ($1,$2,$3,$4,'disbursed',now())`,[app,client,business,migori]);
  await db.query(`insert into loans values ($1,$2,$3,$4,$5,$6,$7,'2026-10-20',now(),100,900,'active',0,0)`,[ids[i],client,app,String(100000+i),business,george,migori]);
}
const oldLoan=uuid(100),selectedRepayment=uuid(101),oldRepayment=uuid(102);
await db.query(`insert into loans values ($1,$2,null,'OLD',$3,$4,$5,'2026-10-01',now(),50,950,'active',0,0)`,[oldLoan,uuid(20),business,george,migori]);
await db.query(`insert into loan_repayments values ($1,$2,$3,$4,50),($5,$6,$3,$4,25)`,[selectedRepayment,ids[0],business,migori,oldRepayment,oldLoan]);
await db.query(`insert into bripta_accounting_entries values ($1,$2,$3,$4,'loan_repayments',$5,50,0),($6,$7,$3,$4,'loan_repayments',$8,25,0)`,[uuid(103),ids[0],business,migori,selectedRepayment,uuid(104),oldLoan,oldRepayment]);
await db.query(`insert into mpesa_callback_queue values ($1,$2,$3,$4)`,[uuid(105),ids[0],selectedRepayment,migori]);
const sql=fs.readFileSync(new URL('../bripta-george-mbita-exact-ten-repair-20261025.sql',import.meta.url),'utf8');
await db.exec(sql);
await db.exec(sql);
const get=async(table,id)=>(await db.query(`select branch_id from ${table} where id=$1`,[id])).rows[0].branch_id;
for(const id of ids)assert.equal(await get('loans',id),mbita);
assert.equal(await get('loan_staff',george),mbita);
assert.equal(await get('loans',oldLoan),migori);
assert.equal(await get('loan_repayments',selectedRepayment),mbita);
assert.equal(await get('loan_repayments',oldRepayment),migori);
assert.equal(await get('bripta_accounting_entries',uuid(103)),mbita);
assert.equal(await get('bripta_accounting_entries',uuid(104)),migori);
assert.equal(await get('mpesa_callback_queue',uuid(105)),mbita);
assert.equal(Number((await db.query('select sum(amount) as total from loan_repayments')).rows[0].total),75);
assert.equal(Number((await db.query('select sum(debit) as total from bripta_accounting_entries')).rows[0].total),75);
assert.equal((await db.query("select count(*) as count from bripta_domain_audit where action='george_mbita_exact_ten_repair_20261025'")).rows[0].count,1);
assert.equal((await db.query("select tgenabled from pg_trigger where tgname='bripta_sync_loan_balance_trigger'")).rows[0].tgenabled,'O');
await db.close();
console.log('PASS: exact ten repair is idempotent; Mbita branches correct, old Migori loan retained, repayment and ledger amounts unchanged.');
