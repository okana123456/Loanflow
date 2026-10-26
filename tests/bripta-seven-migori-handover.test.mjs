import assert from 'node:assert/strict';
import fs from 'node:fs';
import { PGlite } from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';

const db = new PGlite();
const migori = '00000000-0000-4000-8000-000000000001';
const mbita = '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338';
const george = '74b5e8ca-fabf-4890-b17b-5f6fc80e0d99';
const admin = '00000000-0000-4000-8000-000000000004';
const target = '00000000-0000-4000-8000-000000000005';
const biz = 'BIZ-B3F5E5D9';
const uuid = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const reviewed = [
  ['0e9de4dc-fc33-41ea-bde6-99dfafb2a31b', '30983310'],
  ['9bcf0987-10a0-4c24-b021-82de4c751d64', '11557687'],
  ['1252fba7-0ff8-42ba-8eed-238aa0214843', '31358107'],
  ['3fd2c264-da0b-49b5-ac80-b74e09613d61', '29912940'],
  ['c038c0bf-1ce5-423c-9ec3-f82e259e123b', '32159891'],
  ['3fce500c-5fb4-4d08-8aca-d73f81a7b1f3', '322429663'],
  ['76e1912c-85ec-4339-a783-270016b991ea', '6896810'],
  ['33e3a535-7922-4624-82bf-7364642ed71b', '27365745'],
  ['f8106a47-bba1-4585-8dff-5f7bd7fff99d', '13657265'],
  ['5f44fc32-41f7-48c3-8eec-b71c6584c294', '13657272'],
  [uuid(990), '30179891'], [uuid(991), '36439143'],
];

await db.exec(`
create role anon; create role authenticated;
create schema auth;
create function auth.uid() returns uuid language sql stable as $$ select '${admin}'::uuid $$;
create function public.bripta_has_permission(text) returns boolean language sql stable as $$ select true $$;
create table bripta_branches(id uuid primary key,business_id text,name text,is_active boolean);
create table loan_staff(id uuid primary key,auth_user_id uuid,business_id text,branch_id uuid,role text,is_active boolean,name text);
create table loan_clients(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,id_number text,updated_at timestamptz);
create table loans(id uuid primary key,client_id uuid,application_id uuid,loan_no text,business_id text,loan_officer_id uuid,branch_id uuid,created_at timestamptz,updated_at timestamptz,total_paid numeric,outstanding_balance numeric,total_payable numeric,status text,arrears_amount numeric,overdue_days int);
create table loan_applications(id uuid primary key,client_id uuid,business_id text,branch_id uuid,loan_officer_id uuid,status text,updated_at timestamptz);
create table loan_schedules(id uuid primary key,loan_id uuid,business_id text,branch_id uuid,amount_paid numeric);
create table loan_repayments(id uuid primary key,loan_id uuid,business_id text,branch_id uuid,amount numeric);
create function test_balance_recalculation() returns trigger language plpgsql as $$ begin update loans set total_paid=999 where id=new.loan_id; return new; end $$;
create trigger bripta_sync_loan_balance_trigger after update on loan_repayments for each row execute function test_balance_recalculation();
create table bripta_accounting_entries(id uuid,loan_id uuid,business_id text,branch_id uuid,source_table text,source_id text,debit numeric,credit numeric);
create table mpesa_callback_queue(id uuid,loan_id uuid,repayment_id uuid,branch_id uuid);
create table bripta_domain_audit(business_id text,branch_id uuid,actor_staff_id uuid,action text,entity_type text,new_value jsonb);
insert into bripta_branches values ('${migori}','${biz}','Migori',true),('${mbita}','${biz}','MBITA  BRANCH',true);
insert into loan_staff values ('${admin}','${admin}','${biz}','${migori}','admin',true,'Admin'),('${george}',null,'${biz}','${mbita}','loan_officer',true,'George Majwa'),('${target}',null,'${biz}','${migori}','loan_officer',true,'New Migori Officer');
`);
for (let i = 0; i < reviewed.length; i++) {
  const [loanId, idNumber] = reviewed[i];
  const clientId = uuid(100 + i), appId = uuid(200 + i);
  await db.query('insert into loan_clients values ($1,$2,$3,$4,$5,now())',
    [clientId,biz,mbita,i === 4 ? null : george,idNumber]);
  await db.query("insert into loan_applications values ($1,$2,$3,$4,$5,'disbursed',now())",
    [appId,clientId,biz,mbita,george]);
  await db.query("insert into loans values ($1,$2,$3,$4,$5,$6,$7,now(),now(),100,900,1000,'active',0,0)",
    [loanId,clientId,appId,String(800000+i),biz,george,mbita]);
  await db.query('insert into loan_schedules values ($1,$2,$3,$4,0)',[uuid(300+i),loanId,biz,mbita]);
  await db.query('insert into loan_repayments values ($1,$2,$3,$4,100)',[uuid(400+i),loanId,biz,mbita]);
  await db.query("insert into bripta_accounting_entries values ($1,$2,$3,$4,'loans',$5,100,0)",
    [uuid(500+i),loanId,biz,mbita,loanId]);
  await db.query('insert into mpesa_callback_queue values ($1,$2,$3,$4)',[uuid(600+i),loanId,uuid(400+i),mbita]);
}
const correction=fs.readFileSync(new URL('../bripta-return-seven-george-loans-to-migori-20261026.sql',import.meta.url),'utf8');
await db.exec(correction);
await db.exec(correction);
for(let i=0;i<reviewed.length;i++){
  const [loanId]=reviewed[i], expected=i<7?migori:mbita;
  for(const [table,id] of [['loans',loanId],['loan_clients',uuid(100+i)],['loan_repayments',uuid(400+i)],['loan_schedules',uuid(300+i)],['bripta_accounting_entries',uuid(500+i)],['mpesa_callback_queue',uuid(600+i)]]){
    assert.equal((await db.query(`select branch_id from ${table} where id=$1`,[id])).rows[0].branch_id,expected,`${table} ${i}`);
  }
}
assert.equal((await db.query('select loan_officer_id from loan_clients where id=$1',[uuid(104)])).rows[0].loan_officer_id,george);
assert.equal(Number((await db.query('select sum(total_paid) as total from loans')).rows[0].total),1200);
assert.equal(Number((await db.query('select sum(amount) as total from loan_repayments')).rows[0].total),1200);
assert.equal((await db.query("select count(*) as n from bripta_domain_audit where action='george_seven_migori_correction_20261026'")).rows[0].n,1);
assert.equal((await db.query("select tgenabled from pg_trigger where tgname='bripta_sync_loan_balance_trigger'")).rows[0].tgenabled,'O');
console.log('PASS: seven selected loans returned to Migori; five Mbita loans, amounts and triggers preserved; rerun idempotent.');

const handover=fs.readFileSync(new URL('../bripta-branch-specific-portfolio-handover-20261026.sql',import.meta.url),'utf8');
await db.exec(handover);
await db.exec(handover);
await assert.rejects(
  db.query('select public.bripta_handover_branch_portfolio($1,$2,$3)',[george,target,mbita]),
  /same-branch target officer is unavailable/
);
const result=(await db.query('select public.bripta_handover_branch_portfolio($1,$2,$3) as result',[george,target,migori])).rows[0].result;
assert.equal(result.clients_moved,7);
assert.equal(result.active_loans_moved,7);
assert.equal((await db.query('select count(*) as n from loan_clients where branch_id=$1 and loan_officer_id=$2',[migori,target])).rows[0].n,7);
assert.equal((await db.query('select count(*) as n from loan_clients where branch_id=$1 and loan_officer_id=$2',[mbita,george])).rows[0].n,5);
assert.equal((await db.query('select count(*) as n from loans where branch_id=$1 and loan_officer_id=$2',[mbita,george])).rows[0].n,5);
assert.equal(Number((await db.query('select sum(total_paid) as total from loans')).rows[0].total),1200);
await assert.rejects(
  db.query('select public.bripta_handover_branch_portfolio($1,$2,$3)',[george,target,migori]),
  /No clients are assigned/
);
await db.close();
console.log('PASS: later Migori handover transfers seven clients and active loans only; Mbita and financial values remain unchanged.');
