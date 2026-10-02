// Run with Node after installing @electric-sql/pglite in .recovery-test-runtime.
// Uses disposable, synthetic PostgreSQL databases; no Supabase connection.
import { PGlite } from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';

const root = new URL('../', import.meta.url);
const sql = readFileSync(new URL('bripta-restore-phone-account-payments-20261002.sql', root), 'utf8');
const verification = readFileSync(new URL('bripta-verify-phone-payment-recovery-20261002.sql', root), 'utf8');
const laterReview = readFileSync(new URL('bripta-recovery-later-repayment-review-20261002.sql', root), 'utf8');
const scheduleReview = readFileSync(new URL('bripta-recovery-schedule-review-20261002.sql', root), 'utf8');
const manifest = [...sql.matchAll(/\('([A-Z0-9]+)','([0-9a-f-]+)','([0-9a-f-]+)','([0-9a-f-]+)','([0-9]+)','([0-9]+)'\)/g)]
  .map(([,ref,queue,client,loan,amount,phone]) => ({ref,queue,client,loan,amount:Number(amount),phone}));
assert.equal(manifest.length,20);
assert.equal(manifest.reduce((s,m)=>s+m.amount,0),7685);
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

async function verify(db) {
  const [{n,total}]=(await db.query('select count(*)::int n,sum(amount)::float total from loan_repayments')).rows;
  assert.equal(n,20);assert.equal(total,7685);
  const totals=new Map();
  for(const m of manifest)totals.set(m.loan,(totals.get(m.loan)||0)+m.amount);
  for(const row of (await db.query('select id,total_paid::float paid,outstanding_balance::float balance,total_payable::float payable from loans')).rows){
    assert.equal(row.paid,totals.get(row.id));assert.equal(row.balance,10000-totals.get(row.id));assert.equal(row.payable,10000);
  }
  assert.equal((await db.query('select sum(total_paid)::float paid from loan_schedules')).rows[0].paid,7685);
  assert.deepEqual((await db.query('select status,count(*)::int n from bripta_sms_outbox group by status order by status')).rows,[{status:'queued',n:20},{status:'sent',n:5}]);
  assert.equal((await db.query('select credits_used from bripta_sms_wallets')).rows[0].credits_used,3534);
  assert.equal((await db.query("select confirmed from mpesa_callback_queue where trans_id='UJ2558DDTO'")).rows[0].confirmed,false);
  assert.equal((await db.query("select count(*)::int n from loan_repayments where payment_reference='UJ2558DDTO'")).rows[0].n,0);
  assert.equal((await db.query("select to_char(payment_date at time zone 'UTC','YYYY-MM-DD HH24:MI') t from loan_repayments order by payment_date limit 1")).rows[0].t,'2026-10-01 19:10');
  const report=Object.fromEntries((await db.query(verification)).rows.map(r=>[r.check_name,Number(r.result)]));
  assert.equal(report.recovered_payments,20);assert.equal(report.recovered_amount,7685);
  assert.equal(report.recovery_integrity_errors,0);assert.equal(report.held_payment_repayments,0);
  assert.equal(report.held_suspense_amount,300);assert.equal(report.recovery_sms_missing,0);
  assert.equal(report.recovery_sms_queued,20);assert.equal(report.other_pending_callbacks,0);
}

const db=await fixture();
try {
  const reviewRows=(await db.query(laterReview)).rows;
  assert.equal(reviewRows.length,20);
  assert.equal(reviewRows[0].missing_transaction,'UJ1DD89X72');
  assert.equal(reviewRows[0].existing_repayment_id,null);
  console.log('PASS: read-only later-repayment review covers all 20 transactions and prioritizes the reported failure.');
  const scheduleRows=(await db.query(scheduleReview)).rows;
  assert.equal(scheduleRows.length,new Set(manifest.map(m=>m.loan)).size);
  assert.equal(scheduleRows.reduce((s,r)=>s+Number(r.reviewed_payment_total),0),7685);
  assert.equal(Number(scheduleRows[0].reviewed_payment_total),520);
  await db.exec("begin; update loan_schedules set total_due=100 where loan_id='12018065-3a0d-4434-a9b4-995c735a6968';");
  assert.equal(Number((await db.query(scheduleReview)).rows[0].recovery_schedule_shortfall),420);
  await db.exec('rollback');
  console.log('PASS: full-batch schedule review totals repeated-client payments and identifies Emily schedule shortfalls.');
  await db.exec(sql);await verify(db);
  console.log('PASS: 20 payments, original Kenya dates, balances/schedules, 20 SMS queued, 5 sent SMS unchanged, KES 300 held.');
  await db.exec(sql);await verify(db);
  console.log('PASS: full migration/recovery rerun is idempotent.');
  for(const phone of [manifest[0].phone,`254${manifest[0].phone.slice(1)}`,`+254 ${manifest[0].phone.slice(1)}`]){
    assert.equal((await db.query('select * from bripta_callback_phone_candidates($1,$2)',[phone,'NEW-REF'])).rows.length,1);
  }
  for(const phone of ['50','35350232','abc0729477489','9990729477489','e0270fa3f36c6c180c728ea6bda6859489e65d73848643f5b467258619c385eb']){
    assert.equal((await db.query('select * from bripta_callback_phone_candidates($1,$2)',[phone,'NEW-REF'])).rows.length,0);
  }
  await db.query('insert into loan_clients(id,business_id,full_name,phone) values(gen_random_uuid(),$1,$2,$3)',[biz,'Held Candidate','0757692626']);
  assert.equal((await db.query("select * from bripta_callback_phone_candidates('0757692626','UJ2558DDTO')")).rows.length,0);
  await db.query('insert into loan_clients(id,business_id,full_name,phone) values(gen_random_uuid(),$1,$2,$3)',[biz,'Shared Phone',manifest[0].phone]);
  assert.equal((await db.query('select * from bripta_callback_phone_candidates($1,$2)',[manifest[0].phone,'NEW-REF'])).rows.length,2);
  console.log('PASS: phone formats, business isolation, short/hash rejection, review hold and ambiguous phone detection.');
}finally{await db.close();}

const withBalanceTrigger=await fixture();
try {
  await withBalanceTrigger.exec(`create function test_balance_trigger() returns trigger language plpgsql as $$ begin
    update loans set total_paid=total_paid+new.loan_portion,outstanding_balance=outstanding_balance-new.loan_portion where id=new.loan_id;
    return new; end $$;
    create trigger test_balance after insert on loan_repayments for each row execute function test_balance_trigger();`);
  await withBalanceTrigger.exec(sql);await verify(withBalanceTrigger);
  console.log('PASS: an existing balance trigger does not double-apply repayment amounts.');
}finally{await withBalanceTrigger.close();}

for(const [label,setup] of [
  ['unexpected balance trigger',`create function bad_balance() returns trigger language plpgsql as $$ begin update loans set total_paid=999999 where id=new.loan_id;return new;end $$; create trigger bad_balance after insert on loan_repayments for each row execute function bad_balance();`],
  ['changed callback amount',`update mpesa_callback_queue set trans_amount=999 where trans_id='${manifest[0].ref}'`],
  ['insufficient schedule balance',`update loan_schedules set total_due=1 where loan_id='${manifest[0].loan}'`],
  ['missing callback',`delete from mpesa_callback_queue where trans_id='${manifest[0].ref}'`],
]) {
  const failed=await fixture();
  try{
    await failed.exec(setup);
    await assert.rejects(()=>failed.exec(sql));
    await failed.exec('rollback');
    assert.equal((await failed.query('select count(*)::int n from loan_repayments')).rows[0].n,0);
    assert.equal((await failed.query('select count(*)::int n from bripta_sms_outbox')).rows[0].n,5);
    assert.equal((await failed.query('select sum(total_paid)::float paid from loans')).rows[0].paid,0);
    console.log(`PASS: ${label} rolls back all new money and SMS records.`);
  }finally{await failed.close();}
}

// Regression: the reviewed KES 270 receipt is distinct from the missing KES 50.
// Its 21:05 Kenya timestamp was saved as UTC, although received at 18:05 UTC.
for(const variant of ['verified earlier callback','actually later record','unconfirmed earlier callback','wrong earlier amount']) {
  const regression=await fixture();
  const target=manifest.find(m=>m.ref==='UJ1DD89X72');
  const priorId='62d38abd-86cc-4953-9f58-9e805afb881d';
  try {
    await regression.query(`insert into loan_repayments(id,business_id,loan_id,amount,payment_reference,receipt_no,
      payment_method,payment_date,created_at,notes) values($1,$2,$3,270,'UJ1DD89K9I','UJ1DD89K9I',
      'M-Pesa','2026-10-01T21:05:51Z','2026-10-01T18:05:53.01053Z','Existing historical payment')`,[priorId,biz,target.loan]);
    await regression.query(`update loans set total_paid=270,total_payable=10270,total_interest=1027 where id=$1`,[target.loan]);
    await regression.query('update loan_schedules set total_due=10270,total_paid=270 where loan_id=$1',[target.loan]);
    await regression.query("update bripta_sms_outbox set status='sent' where repayment_id=$1",[priorId]);
    await regression.query(`insert into mpesa_callback_queue(id,business_short_code,trans_id,trans_amount,bill_ref_number,
      created_at,trans_time,confirmed,repayment_id) values(gen_random_uuid(),$1,'UJ1DD89K9I',270,$2,
      '2026-10-01T18:05:52.87495Z','20261001210551',true,$3)`,[biz,target.phone,priorId]);
    if(variant==='actually later record')await regression.query("update loan_repayments set created_at='2026-10-02T10:00:00Z' where id=$1",[priorId]);
    if(variant==='unconfirmed earlier callback')await regression.exec("update mpesa_callback_queue set confirmed=false where trans_id='UJ1DD89K9I'");
    if(variant==='wrong earlier amount')await regression.exec("update mpesa_callback_queue set trans_amount=50 where trans_id='UJ1DD89K9I'");
    const before=(await regression.query('select to_jsonb(r) as r from loan_repayments r where id=$1',[priorId])).rows[0].r;
    if(variant==='verified earlier callback') {
      await regression.exec(sql);
      await regression.exec(sql);
      assert.equal((await regression.query('select count(*)::int n from bripta_payment_recovery_20261002')).rows[0].n,20);
      assert.equal((await regression.query('select count(*)::int n from loan_repayments')).rows[0].n,21);
      const balance=(await regression.query('select total_paid::float paid,outstanding_balance::float balance from loans where id=$1',[target.loan])).rows[0];
      assert.deepEqual(balance,{paid:320,balance:9950});
      assert.equal((await regression.query('select status from bripta_sms_outbox where repayment_id=$1',[priorId])).rows[0].status,'sent');
      console.log('PASS: verified three-hour timestamp error permits only the missing KES 50; earlier KES 270 and its sent SMS are preserved, including on rerun.');
    } else {
      await assert.rejects(()=>regression.exec(sql),/later repayment/);
      await regression.exec('rollback');
      assert.equal((await regression.query('select count(*)::int n from loan_repayments')).rows[0].n,1);
      assert.equal((await regression.query('select count(*)::int n from bripta_sms_outbox')).rows[0].n,6);
      console.log(`PASS: ${variant} still blocks recovery and rolls back the batch.`);
    }
    assert.deepEqual((await regression.query('select to_jsonb(r) as r from loan_repayments r where id=$1',[priorId])).rows[0].r,before);
  } finally {await regression.close();}
}

// Reviewed real-world shape: preserve Emily's accepted balance despite paid,
// malformed legacy schedules. Other shortfalls must still fail closed.
for(const changedSnapshot of [false,true]) {
  const legacy=await fixture();
  const emily='12018065-3a0d-4434-a9b4-995c735a6968';
  const risper='b3635111-1782-4305-b907-b9e9d6191a72';
  try {
    await legacy.exec(`
      update loans set outstanding_balance=719.50,total_paid=6995,total_payable=7995,total_interest=1500,
        disbursed_amount=6000,arrears_amount=719.50,overdue_days=90 where id='${emily}';
      delete from loan_schedules where loan_id='${emily}';
      insert into loan_schedules(id,loan_id,due_date,installment_no,total_due,total_paid,status,paid_at) values
        ('38e694ed-b7c3-4a56-9f00-095a6ca11c1d','${emily}','2026-06-26',1,1500,1500,'paid','2026-07-04T16:22:08.803Z'),
        ('a57e53b4-c6ea-4eaa-a9fc-31f8f540ccb9','${emily}','2026-07-03',2,1500,1500,'paid','2026-07-18T10:51:08Z'),
        ('53f42ca9-006f-4fa7-98f9-6c51140c7446','${emily}','36238-08-03',1785103,1265,1265,'paid','2026-08-15T15:23:06Z');
      alter table loan_repayments disable trigger bripta_prepare_future_repayment_trigger;
    `);
    for(const amount of [220,220,285,335,740,200,200,400,100,1000,400,100,1000,300,100,200,325,770,100]){
      await legacy.query(`insert into loan_repayments(business_id,loan_id,amount,loan_portion,payment_date,created_at)
        values($1,$2,$3,$3,'2026-09-12T10:00:00Z','2026-09-12T10:00:00Z')`,[biz,emily,amount]);
    }
    await legacy.exec(`alter table loan_repayments enable trigger bripta_prepare_future_repayment_trigger;
      update bripta_sms_outbox set status='sent' where loan_id='${emily}';
      update loans set outstanding_balance=7447.5,total_paid=2300,total_payable=9747.5,total_interest=1750,
        disbursed_amount=7000,arrears_amount=7447.5 where id='${risper}';
      update loan_schedules set total_due=11946.666666666666,total_paid=1950 where loan_id='${risper}';`);
    const before=(await legacy.query("select jsonb_agg(to_jsonb(s) order by id) rows from loan_schedules s where loan_id=$1",[emily])).rows[0].rows;
    if(changedSnapshot){
      await legacy.exec(`update loans set outstanding_balance=720 where id='${emily}'`);
      await assert.rejects(()=>legacy.exec(sql),/Remaining schedule amounts/);
      await legacy.exec('rollback');
      assert.equal((await legacy.query('select count(*)::int n from loan_repayments')).rows[0].n,19);
      console.log('PASS: Emily exception rejects a changed live balance and rolls back all new payments.');
    } else {
      await legacy.exec(sql);await legacy.exec(sql);
      const balances=(await legacy.query('select outstanding_balance::float balance,total_paid::float paid,total_payable::float payable,arrears_amount::float arrears,overdue_days from loans where id=$1',[emily])).rows[0];
      assert.deepEqual(balances,{balance:199.5,paid:7515,payable:7995,arrears:199.5,overdue_days:90});
      const audit=(await legacy.query("select schedule_applied_amount::float applied,schedule_unallocated_amount::float pending,schedule_review_reason from bripta_payment_recovery_20261002 where transaction_code='UJ20L8DGO8'")).rows[0];
      assert.equal(audit.applied,0);assert.equal(audit.pending,520);assert.match(audit.schedule_review_reason,/legacy schedule gap/);
      const report=Object.fromEntries((await legacy.query(verification)).rows.map(r=>[r.check_name,Number(r.result)]));
      assert.equal(report.recovered_payments,20);assert.equal(report.recovered_amount,7685);
      assert.equal(report.schedule_allocation_pending_review,520);assert.equal(report.recovery_integrity_errors,0);
      assert.equal(report.emily_balance_after_recovery,199.5);assert.equal(report.recovery_sms_queued,20);
      assert.equal((await legacy.query("select count(*)::int n from bripta_sms_outbox where status='sent'")).rows[0].n,24);
      assert.equal((await legacy.query('select outstanding_balance::float b from loans where id=$1',[risper])).rows[0].b,7147.5);
      assert.equal((await legacy.query('select total_paid::float paid from loan_schedules where loan_id=$1',[risper])).rows[0].paid,2250);
      console.log('PASS: Emily KES 520 reduces only the accepted balance to 199.50; historical schedules/SMS stay unchanged, schedule gap is audited, and Risper keeps its baseline.');
    }
    assert.deepEqual((await legacy.query("select jsonb_agg(to_jsonb(s) order by id) rows from loan_schedules s where loan_id=$1",[emily])).rows[0].rows,before);
  }finally{await legacy.close();}
}
