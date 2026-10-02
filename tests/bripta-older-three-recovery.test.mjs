// Run with Node after installing @electric-sql/pglite in .recovery-test-runtime.
// Uses disposable, synthetic PostgreSQL databases; no Supabase connection.
import { PGlite } from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';

const root = new URL('../', import.meta.url);
const sql = readFileSync(new URL('bripta-restore-older-three-payments-20261002.sql', root), 'utf8');
const verification = readFileSync(new URL('bripta-verify-phone-payment-recovery-20261002.sql', root), 'utf8');
const laterReview = readFileSync(new URL('bripta-recovery-later-repayment-review-20261002.sql', root), 'utf8');
const scheduleReview = readFileSync(new URL('bripta-recovery-schedule-review-20261002.sql', root), 'utf8');
const manifest = [...sql.matchAll(/\('([A-Z0-9]+)','([0-9a-f-]+)','([0-9a-f-]+)','([0-9a-f-]+)','([0-9]+)','([0-9]+)'\)/g)]
  .map(([,ref,queue,client,loan,amount,phone]) => ({ref,queue,client,loan,amount:Number(amount),phone}));
assert.equal(manifest.length,3);
assert.equal(manifest.reduce((s,m)=>s+m.amount,0),2100);
const biz='BIZ-B3F5E5D9',branch='00000000-0000-4000-8000-000000000001';
const prepareSource=readFileSync(new URL('bripta-payment-capture-and-npl-setup-20260814.sql',root),'utf8');
const prepare=prepareSource.slice(prepareSource.indexOf('create or replace function public.bripta_prepare_future_repayment()'),prepareSource.indexOf('-- Atomic callback operation'));
const smsSource=readFileSync(new URL('bripta-talksasa-sms-wallet.sql',root),'utf8');
const sms=smsSource.slice(smsSource.indexOf('create or replace function public.bripta_queue_repayment_sms()'),smsSource.indexOf('-- Atomically reserves SMS credits'));

async function fixture() {
  const db=new PGlite();
  await db.exec(`
    create role anon; create role authenticated; create role service_role;
    create table loan_clients(id uuid primary key,business_id text,full_name text,phone text,account_credit numeric default 0);
    create table bripta_branches(id uuid primary key,business_id text);
    create table loans(id uuid primary key,business_id text,client_id uuid,branch_id uuid,loan_no text,
      status text default 'active',outstanding_balance numeric default 10000,total_paid numeric default 0,
      total_payable numeric default 10000,total_interest numeric default 1000,disbursed_amount numeric default 9000,
      arrears_amount numeric default 0,overdue_days integer default 0);
    create table loan_schedules(id uuid primary key default gen_random_uuid(),loan_id uuid,due_date date,
      installment_no integer,total_due numeric,total_paid numeric default 0,status text default 'pending',paid_at timestamptz);
    create table loan_repayments(id uuid primary key default gen_random_uuid(),business_id text,branch_id uuid,
      loan_id uuid references loans(id),receipt_no text,payment_reference text,amount numeric,payment_method text,
      payment_date timestamptz,created_at timestamptz default now(),principal_portion numeric,interest_portion numeric,
      penalty_portion numeric,registration_fee_portion numeric,processing_fee_portion numeric,loan_portion numeric,
      credit_portion numeric,mpesa_confirmed boolean,sender_name text,sender_phone text,notes text);
    create table mpesa_callback_queue(id uuid primary key,business_short_code text,trans_id text,trans_amount numeric,
      created_at timestamptz,trans_time text,raw_payload jsonb,bill_ref_number text,first_name text,middle_name text,
      last_name text,msisdn text,confirmed boolean default false,loan_id uuid,repayment_id uuid,branch_id uuid,dismissed boolean default false);
    create table unmatched_payments(id uuid default gen_random_uuid(),business_id text,mpesa_reference text,resolved boolean,resolved_at timestamptz);
    create table bripta_sms_outbox(id uuid default gen_random_uuid(),business_id text,repayment_id uuid unique,
      loan_id uuid,client_id uuid,client_name text,message_type text,status text default 'queued',queued_at timestamptz default now());
    create table bripta_sms_wallets(business_id text primary key,activated_at timestamptz default '2026-01-01Z',
      credits_purchased integer default 6483,credits_used integer default 3534);
    insert into bripta_sms_wallets(business_id) values('${biz}');
    insert into bripta_branches values('${branch}','${biz}');
    insert into bripta_sms_outbox(business_id,repayment_id,status)
      select '${biz}',gen_random_uuid(),'sent' from generate_series(1,5);
  `);
  const clients=new Set(),loans=new Set();
  for (const [index,m] of manifest.entries()) {
    if (!clients.has(m.client)) {
      await db.query('insert into loan_clients(id,business_id,full_name,phone) values($1,$2,$3,$4)',[m.client,biz,'Test Borrower',m.phone]);
      clients.add(m.client);
    }
    if (!loans.has(m.loan)) {
      await db.query('insert into loans(id,business_id,client_id,branch_id,loan_no) values($1,$2,$3,$4,$5)',[m.loan,biz,m.client,branch,String(index)]);
      await db.query("insert into loan_schedules(loan_id,due_date,installment_no,total_due) values($1,'2026-10-01',1,10000)",[m.loan]);
      loans.add(m.loan);
    }
    const mm=String(10+index).padStart(2,'0');
    await db.query(`insert into mpesa_callback_queue(id,business_short_code,trans_id,trans_amount,bill_ref_number,
      created_at,trans_time,raw_payload,first_name,msisdn) values($1,$2,$3,$4,$5,$6,$7,$8,'Test','hashed-payer')`,
      [m.queue,biz,m.ref,m.amount,m.phone,`2026-10-01T19:${mm}:00Z`,`2026100122${mm}00`,JSON.stringify({TransTime:`2026100122${mm}00`})]);
  }
  await db.exec(`insert into mpesa_callback_queue(id,business_short_code,trans_id,trans_amount,bill_ref_number,created_at)
    values('06ffeb78-4240-4d04-9cbb-8614620e97b2','${biz}','UJ2558DDTO',300,'0757692626','2026-10-02T04:24:17Z');
    insert into loan_clients values(gen_random_uuid(),'ANOTHER_BUSINESS','Other Client','${manifest[0].phone}',0);`);
  await db.exec(prepare);
  await db.exec(sms);
  return db;
}



async function reviewedFixture(){
 const db=await fixture();
 const setup=readFileSync(new URL('bripta-restore-phone-account-payments-20261002.sql',root),'utf8');
 await db.exec(setup.slice(setup.indexOf('alter table public.mpesa_callback_queue'),setup.indexOf('create temporary table bripta_recovery_manifest')));
 const reviewed={
  UJ1JL8EY56:{balance:5100,paid:150,payable:5250,interest:1250,disbursed:4000,transTime:'20261001112450',created:'2026-10-01T08:24:51.885971Z',count:5},
  UJ1BZ8H7SH:{balance:5000,paid:0,payable:5000,interest:1000,disbursed:4000,transTime:'20261001123504',created:'2026-10-01T09:35:06.495297Z',count:4},
  UJ16F8CN12:{balance:4950,paid:300,payable:5250,interest:1250,disbursed:4000,transTime:'20261001130446',created:'2026-10-01T10:04:48.5531Z',count:5}
 };
 for(const m of manifest){
  const x=reviewed[m.ref],step=x.payable/x.count;
  await db.query('update loans set outstanding_balance=$2,total_paid=$3,total_payable=$4,total_interest=$5,disbursed_amount=$6 where id=$1',[m.loan,x.balance,x.paid,x.payable,x.interest,x.disbursed]);
  await db.query('delete from loan_schedules where loan_id=$1',[m.loan]);
  for(let j=0;j<x.count;j++)await db.query("insert into loan_schedules(loan_id,installment_no,total_due,total_paid,due_date,status) values($1,$2,$3,$4,$5,$6)",[m.loan,j+1,step,Math.min(step,Math.max(0,x.paid-j*step)),['2026-10-08','2026-10-15','2026-10-22','2026-10-29','2026-11-05'][j],x.paid>j*step?'partial':'pending']);
  await db.query('update mpesa_callback_queue set trans_time=$2,raw_payload=$3,created_at=$4 where id=$1',[m.queue,x.transTime,JSON.stringify({TransTime:x.transTime}),x.created]);
 }
 const later=[
  {id:'d90e371d-3a2f-4bb4-a47c-cb4f95d4825e',loan:manifest.find(x=>x.ref==='UJ1JL8EY56').loan,amount:150,ref:'UJ2JL8L2LD',date:'2026-10-02 14:08:15+00',created:'2026-10-02 14:08:18.649903+00',notes:'Auto-confirmed via Daraja C2B. Matched by borrower phone account. Allocation: loan KES 150.00. Payer: MERCY'},
  {id:'d4166fe4-9f1f-4975-9660-2ce45218327e',loan:manifest.find(x=>x.ref==='UJ16F8CN12').loan,amount:300,ref:'UJ26F8GSQD',date:'2026-10-02 08:29:14+00',created:'2026-10-02 11:54:28.113371+00',notes:'[BRIPTA_RECOVERY_20261002] [NEXT_THREE] Original M-Pesa callback restored using reviewed borrower PHONE ACCOUNT reference. No new fees.'}
 ];
 for(const x of later){await db.query(`insert into loan_repayments(id,business_id,branch_id,loan_id,amount,loan_portion,payment_reference,receipt_no,payment_method,payment_date,created_at,notes)
 values($1,$2,$3,$4,$5,$5,$6,$6,'M-Pesa',$7,$8,$9)`,[x.id,biz,branch,x.loan,x.amount,x.ref,x.date,x.created,x.notes]);
 await db.query("update bripta_sms_outbox set status='sent' where repayment_id=$1",[x.id]);}
 return db;
}
const db=await reviewedFixture();
try{
 const old=(await db.query("select to_jsonb(r) as row from loan_repayments r where payment_reference in ('UJ2JL8L2LD','UJ26F8GSQD') order by id")).rows;
 await db.exec(sql);
 assert.equal(Number((await db.query("select sum(amount) as amount from bripta_payment_recovery_20261002 where transaction_code in ('UJ1JL8EY56','UJ1BZ8H7SH','UJ16F8CN12')")).rows[0].amount),2100);
 const expected={UJ1JL8EY56:4400,UJ1BZ8H7SH:4300,UJ16F8CN12:4250};
 for(const m of manifest)assert.equal(Number((await db.query('select outstanding_balance from loans where id=$1',[m.loan])).rows[0].outstanding_balance),expected[m.ref]);
 assert.deepEqual((await db.query("select to_jsonb(r) as row from loan_repayments r where payment_reference in ('UJ2JL8L2LD','UJ26F8GSQD') order by id")).rows,old);
 assert.equal((await db.query("select count(*)::int n from bripta_sms_outbox where status='queued'")).rows[0].n,3);
 const snapshot=(await db.query('select to_jsonb(r) as row from loan_repayments r order by id')).rows;
 await db.exec(sql);
 assert.deepEqual((await db.query('select to_jsonb(r) as row from loan_repayments r order by id')).rows,snapshot);
 assert.equal((await db.query("select confirmed from mpesa_callback_queue where trans_id='UJ2558DDTO'")).rows[0].confirmed,false);
 console.log('PASS: three older KES 700 callbacks recovered, later Mercy/Alice payments and SMS unchanged, rerun idempotent, held payment untouched.');
}finally{await db.close();}
for(const change of ["update loans set outstanding_balance=5099 where id='e61ed62b-864d-4d22-9b26-315445f6ec07'","update loan_repayments set amount=151 where payment_reference='UJ2JL8L2LD'","update loan_repayments set amount=301 where payment_reference='UJ26F8GSQD'","update loan_schedules set total_due=1 where loan_id='992a3ee7-1ff1-4c50-938c-f0054236ff76'"]){
 const d=await reviewedFixture();try{await d.exec(change);await assert.rejects(()=>d.exec(sql));await d.exec('rollback');assert.equal((await d.query('select count(*)::int n from loan_repayments')).rows[0].n,2);console.log('PASS: changed baseline, later credit or schedules roll back entire batch.');}finally{await d.close();}
}
