import assert from 'node:assert/strict';
import fs from 'node:fs';
import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';

const db=new PGlite();
const migori='00000000-0000-4000-8000-000000000001';
const mbita='00000000-0000-4000-8000-000000000002';
const george='00000000-0000-4000-8000-000000000003';
const admin='00000000-0000-4000-8000-000000000004';
const client='00000000-0000-4000-8000-000000000005';
const loan='00000000-0000-4000-8000-000000000006';
await db.exec(`
create role anon;
create schema auth;
create function auth.uid() returns uuid language sql stable as $$select '${admin}'::uuid$$;
create function public.bripta_has_permission(text) returns boolean language sql stable as $$select true$$;
create function public.bripta_can_access_branch(uuid) returns boolean language sql stable as $$select true$$;
create table loan_staff(id uuid primary key,auth_user_id uuid,business_id text,branch_id uuid,role text,is_active boolean);
create table loan_clients(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,created_by uuid,updated_at timestamptz);
create table loan_applications(id uuid primary key,business_id text,branch_id uuid,client_id uuid,loan_officer_id uuid,status text,updated_at timestamptz);
create table loans(id uuid primary key,business_id text,branch_id uuid,client_id uuid,loan_officer_id uuid,amount numeric);
create table loan_schedules(id uuid primary key,business_id text,branch_id uuid,loan_id uuid,total_due numeric);
create table loan_repayments(id uuid primary key,business_id text,branch_id uuid,loan_id uuid,amount numeric);
create table bripta_charges(id uuid primary key,business_id text,branch_id uuid,client_id uuid,loan_id uuid,amount numeric);
create table bripta_domain_audit(business_id text,branch_id uuid,actor_staff_id uuid,action text,entity_type text,new_value jsonb);
insert into loan_staff values ('${admin}','${admin}','BIZ-B3F5E5D9','${migori}','admin',true),
  ('${george}',null,'BIZ-B3F5E5D9','${mbita}','loan_officer',true);
insert into loan_clients values ('${client}','BIZ-B3F5E5D9','${migori}','${george}','${admin}',now());
insert into loans values ('${loan}','BIZ-B3F5E5D9','${migori}','${client}','${george}',5000);
`);
const migration=fs.readFileSync(new URL('../bripta-branch-origin-guard-20261025.sql',import.meta.url),'utf8');
await db.exec(migration);
await db.exec(migration); // idempotent
const otherClient='00000000-0000-4000-8000-000000000007';
const otherLoan='00000000-0000-4000-8000-000000000008';
await db.exec(`
insert into loan_clients values ('${otherClient}','BIZ-B3F5E5D9','${mbita}','${george}','${admin}',now());
insert into loan_applications values ('00000000-0000-4000-8000-000000000009','BIZ-B3F5E5D9','${migori}','${otherClient}','${george}','submitted',now());
insert into loans values ('${otherLoan}','BIZ-B3F5E5D9','${migori}','${otherClient}','${george}',7000);
insert into loan_schedules values ('00000000-0000-4000-8000-000000000010','BIZ-B3F5E5D9','${migori}','${otherLoan}',7000);
insert into loan_repayments values ('00000000-0000-4000-8000-000000000011','BIZ-B3F5E5D9','${migori}','${otherLoan}',100);
insert into bripta_charges values ('00000000-0000-4000-8000-000000000012','BIZ-B3F5E5D9','${migori}','${otherClient}','${otherLoan}',25);
insert into loan_repayments values ('00000000-0000-4000-8000-000000000013','BIZ-B3F5E5D9','${mbita}','${loan}',50);
`);
const branch=async(table,id)=>(await db.query(`select branch_id from ${table} where id=$1`,[id])).rows[0].branch_id;
for(const [table,id] of [
  ['loan_applications','00000000-0000-4000-8000-000000000009'],
  ['loans',otherLoan],['loan_schedules','00000000-0000-4000-8000-000000000010'],
  ['loan_repayments','00000000-0000-4000-8000-000000000011'],
  ['bripta_charges','00000000-0000-4000-8000-000000000012']
])assert.equal(await branch(table,id),mbita,`${table} inherits the Mbita origin`);
assert.equal(await branch('loans',loan),migori,'old Migori loan stays in Migori');
assert.equal(await branch('loan_repayments','00000000-0000-4000-8000-000000000013'),migori,'old loan repayment stays in Migori');
assert.deepEqual((await db.query('select amount from loans order by amount')).rows.map(r=>Number(r.amount)),[5000,7000]);
const target='00000000-0000-4000-8000-000000000014';
const pending='00000000-0000-4000-8000-000000000015';
const finished='00000000-0000-4000-8000-000000000016';
await db.exec(`
insert into loan_staff values ('${target}',null,'BIZ-B3F5E5D9','${mbita}','loan_officer',true);
insert into loan_applications values ('${pending}','BIZ-B3F5E5D9','${migori}','${client}','${george}','submitted',now());
insert into loan_applications values ('${finished}','BIZ-B3F5E5D9','${migori}','${client}','${george}','disbursed',now());
`);
await db.query('select public.bripta_transfer_portfolio($1,$2,$3,false)',[george,target,[client]]);
assert.equal(await branch('loan_clients',client),mbita,'selected client follows target officer');
assert.equal(await branch('loan_applications',pending),mbita,'pending application follows client');
assert.equal(await branch('loan_applications',finished),migori,'disbursed application remains historical');
assert.equal(await branch('loans',loan),migori,'transfer preserves old loan branch');
await db.close();
console.log('PASS: migration is idempotent; new records inherit parent branch; transfers preserve old Migori history and all amounts.');
