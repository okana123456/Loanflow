import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
const db=new PGlite(),biz='BIZ-B3F5E5D9',id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
try{
  await db.exec(`create role authenticated;create role anon;create schema auth;
    create function auth.jwt() returns jsonb language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb$$;
    create function auth.uid() returns uuid language sql stable as $$select (auth.jwt()->>'sub')::uuid$$;
    grant usage on schema public,auth to authenticated,anon;
    create table loan_staff(id uuid primary key,auth_user_id uuid,email text,role text,business_id text,branch_id uuid,is_active boolean);
    create table loans(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,outstanding_balance numeric);
    create table loan_penalties(id uuid primary key,loan_id uuid,business_id text,branch_id uuid,penalty_amount numeric,date_charged date,reason text,is_waived boolean,waived_reason text);
    alter table loan_penalties enable row level security;grant select on loan_penalties to authenticated;
    create policy deny_legacy on loan_penalties as restrictive for select to authenticated using(false);
  `);
  for(const[n,role,business,branch,active]of [[11,'admin',biz,1,true],[12,'loan_officer',biz,1,true],[13,'loan_officer',biz,1,true],[14,'branch_manager',biz,1,true],[15,'cashier',biz,1,true],[16,'admin','OTHER',9,true],[17,'loan_officer',biz,1,false]])
    await db.query('insert into loan_staff values($1,$1,$2,$3,$4,$5,$6)',[id(n),`u${n}@test.invalid`,role,business,id(branch),active]);
  for(const[n,business,branch,officer]of [[21,biz,1,12],[22,biz,2,13],[23,'OTHER',9,16],[24,biz,1,13]])
    await db.query('insert into loans values($1,$2,$3,$4,1000)',[id(n),business,id(branch),id(officer)]);
  await db.exec(`insert into loan_penalties values('${id(31)}','${id(21)}','${biz}',null,150,'2026-10-01','One-time rollover penalty',false,null),
    ('${id(32)}','${id(21)}','${biz}','${id(1)}',50,'2026-10-02','Removed duplicate penalty',true,'Historical correction'),
    ('${id(33)}','${id(22)}','${biz}','${id(2)}',200,'2026-10-03','Rollover penalty',false,null);`);
  const snapshot=async()=>(await db.query('select (select sum(outstanding_balance) from loans) balance,(select sum(penalty_amount) from loan_penalties) penalties,(select count(*) from loan_penalties) penalty_count')).rows;
  const before=await snapshot();const migration=readFileSync(new URL('../bripta-penalty-record-reader-20261010.sql',import.meta.url),'utf8');await db.exec(migration);await db.exec(migration);
  const login=async n=>{await db.exec('reset role');await db.query("select set_config('request.jwt.claims',$1,false)",[JSON.stringify(n?{sub:id(n),email:`u${n}@test.invalid`}:{})]);await db.exec(n?'set role authenticated':'set role anon');};
  const read=async n=>(await db.query('select bripta_loan_penalty_records($1) result',[id(n)])).rows[0].result;
  await login(12);assert.equal((await db.query('select * from loan_penalties')).rows.length,0);
  const records=(await read(21)).rows;assert.equal(records.length,2);assert.equal(records[0].penalty_amount,150);assert.equal(records[0].date_charged,'2026-10-01');assert.equal(records[1].is_waived,true);
  await assert.rejects(()=>read(24),/assigned portfolio/);await assert.rejects(()=>read(22),/outside your branch/);await assert.rejects(()=>read(23),/your business/);
  for(const n of [14,15]){await login(n);assert.equal((await read(21)).rows.length,2);await assert.rejects(()=>read(22),/outside your branch/);}
  await login(11);assert.equal((await read(22)).rows.length,1);assert.deepEqual((await read(24)).rows,[]);
  for(const n of [16,17]){await login(n);await assert.rejects(()=>read(21),/not permitted/);}
  await login(null);await assert.rejects(()=>read(21),/permission denied/);await db.exec('reset role');assert.deepEqual(await snapshot(),before);
  console.log('PASS: existing amounts/dates and waived history, missing branch metadata, denied legacy RLS, role/portfolio/branch/business boundaries, repeat-safe read-only installation.');

  const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');let modal='',fail=false;const errors=[];
  const loan={id:id(21),loan_no:'123456',outstanding_balance:1000,total_payable:1000,processing_fee:0,status:'active',loan_clients:{full_name:'Test Client'}};
  const query=table=>({select(){return this;},eq(){return this;},order(){return this;},single:async()=>({data:loan}),
    then(resolve){assert.notEqual(table,'loan_penalties','No legacy penalty read');return Promise.resolve({data:[]}).then(resolve);}});
  const context=vm.createContext({console,Date,sb:query,hasRole:()=>false,statusBadge:x=>x,fmtDate:x=>x,fmtMoney:x=>'KES '+Number(x||0).toFixed(2),
    escapeHtml:x=>String(x).replaceAll('<','&lt;'),openModal:text=>modal=text,toast:text=>errors.push(text),
    supabaseClient:{rpc:async(name,args)=>{assert.equal(name,'bripta_loan_penalty_records');return fail?{error:{message:'Permission denied'}}:{data:{loan_id:args.p_loan_id,rows:records}};}}});
  const fragment=(a,b)=>{const start=html.indexOf(a);return html.slice(start,html.indexOf(b,start+a.length));};
  vm.runInContext(fragment('async function readLoanPenaltyRecords(','async function openEditLoanModal('),context);
  vm.runInContext(fragment('function loanStatementEventTime(','async function downloadPaymentStatementPdf('),context);
  await context.viewLoan(id(21));assert.ok(modal.includes('Rollover Penalties'));assert.ok(modal.includes('KES 150.00'));assert.ok(modal.includes('2026-10-01'));assert.ok(modal.includes('Waived'));
  await context.viewClientPayments(id(21));assert.ok(modal.includes('Rollover Penalty Record'));assert.ok(modal.includes('KES 150.00'));assert.ok(modal.includes('2026-10-01'));
  fail=true;const previous=modal;await context.viewLoan(id(21));assert.equal(modal,previous);assert.ok(errors[0].includes('Permission denied'),'errors do not silently become empty history');
  assert.ok(context.penaltyRecordHtml([{penalty_amount:75,created_at:'2026-10-04',reason:'<unsafe>'}]).includes('2026-10-04'));
  assert.ok(context.penaltyRecordHtml([{penalty_amount:75}]).includes('Date not recorded'));
  assert.ok(context.penaltyRecordHtml([]).includes('No penalty records'));
  console.log('PASS: actual loan and client-payment views show charged amount/date and waived status; errors, missing dates and empty records are explicit.');
}finally{await db.close();}
