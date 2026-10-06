import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
const db=new PGlite(),biz='BIZ-B3F5E5D9',id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
try{
  await db.exec(`create role authenticated;create role anon;create role service_role;create schema auth;
    create function auth.jwt() returns jsonb language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb$$;
    create function auth.uid() returns uuid language sql stable as $$select (auth.jwt()->>'sub')::uuid$$;
    grant usage on schema public,auth to authenticated,anon;
    create table loan_staff(id uuid primary key,auth_user_id uuid,email text,role text,business_id text,branch_id uuid,is_active boolean);
    create table bripta_branches(id uuid primary key,business_id text,is_head_office boolean);
    create table loan_settings(business_id text,mpesa_shortcode text);
    create table mpesa_callback_queue(id uuid primary key,business_short_code text,branch_id uuid,trans_id text,trans_amount numeric,confirmed boolean,repayment_id uuid);
    create table unmatched_payments(id uuid primary key,business_id text,branch_id uuid,mpesa_reference text,amount numeric,resolved boolean);
    create table loan_repayments(id uuid primary key,business_id text,payment_reference text,receipt_no text,amount numeric);
    create table loans(id uuid primary key,outstanding_balance numeric);
    create table bripta_domain_audit(business_id text,branch_id uuid,actor_staff_id uuid,action text,entity_type text,entity_id text,old_value jsonb,new_value jsonb);
    alter table mpesa_callback_queue enable row level security;alter table unmatched_payments enable row level security;
    grant select,update on mpesa_callback_queue,unmatched_payments to authenticated;
    create policy deny_queue on mpesa_callback_queue as restrictive for all to authenticated using(false) with check(false);
    create policy deny_manual on unmatched_payments as restrictive for all to authenticated using(false) with check(false);
  `);
  for(const[n,role,business,branch,active]of [[11,'admin',biz,1,true],[12,'branch_manager',biz,1,true],[13,'cashier',biz,1,true],[14,'loan_officer',biz,1,true],[15,'admin','OTHER',9,true],[16,'admin',biz,1,false]])
    await db.query('insert into loan_staff values($1,$1,$2,$3,$4,$5,$6)',[id(n),`u${n}@test.invalid`,role,business,id(branch),active]);
  await db.exec(`insert into bripta_branches values('${id(1)}','${biz}',true),('${id(2)}','${biz}',false),('${id(9)}','OTHER',true);
    insert into loan_settings values('${biz}','4044341'),('OTHER','5555');insert into loans values('${id(80)}',1000);
    insert into loan_repayments values('${id(81)}','${biz}','PAID','PAID',100);
  `);
  for(const[n,business,branch,ref,confirmed]of [[21,biz,1,'PAIR',false],[22,biz,1,'BULK',false],[23,biz,2,'BRANCH2',false],[24,'OTHER',9,'OTHER',false],[25,biz,1,'PAID',false],[26,biz,1,'CONFIRMED',true],[27,'4044341',1,'NUMERIC',false],[28,biz,1,'ATOMIC',false]])
    await db.query('insert into mpesa_callback_queue values($1,$2,$3,$4,100,$5,null)',[id(n),business,id(branch),ref,confirmed]);
  await db.exec(`insert into unmatched_payments values('${id(31)}','${biz}','${id(1)}','PAIR',100,false),('${id(32)}','${biz}','${id(1)}','MANUAL',100,false);`);
  const files=['bripta-suspense-reader-20261005.sql','bripta-suspense-dismiss-20261006.sql'];
  for(const file of files){const sql=readFileSync(new URL('../'+file,import.meta.url),'utf8');await db.exec(sql);await db.exec(sql);}
  const snapshot=async()=>(await db.query('select (select sum(trans_amount) from mpesa_callback_queue) queue,(select sum(amount) from unmatched_payments) manual,(select sum(amount) from loan_repayments) repayments,(select sum(outstanding_balance) from loans) balance')).rows;
  const before=await snapshot();
  const login=async n=>{await db.exec('reset role');await db.query("select set_config('request.jwt.claims',$1,false)",[JSON.stringify(n?{sub:id(n),email:`u${n}@test.invalid`}:{})]);await db.exec(n?'set role authenticated':'set role anon');};
  const dismiss=async(q=[],m=[])=>(await db.query('select bripta_dismiss_suspense($1,$2) result',[q.map(id),m.map(id)])).rows[0].result;
  await login(12);
  assert.equal((await db.query('select * from mpesa_callback_queue')).rows.length,0,'direct RLS reads denied');
  let result=await dismiss([21]);assert.equal(result.dismissed_rows,2,'queue and fallback dismissed together');
  result=await dismiss([21]);assert.equal(result.dismissed_rows,0);assert.equal(result.already_dismissed_rows,2,'retry is idempotent');
  await assert.rejects(()=>dismiss([28,25]),/already confirmed or recorded/);
  await assert.rejects(()=>dismiss([28,23]),/outside your business\/branch/);
  await assert.rejects(()=>dismiss([24]),/outside your business\/branch/);
  await assert.rejects(()=>dismiss([26]),/already confirmed or recorded/);
  await assert.rejects(()=>dismiss([999]),/missing/);
  await login(13);result=await dismiss([22,27],[32]);assert.equal(result.dismissed_rows,3);
  for(const n of [14,15,16]){await login(n);await assert.rejects(()=>dismiss([28]),/view only/);}
  await login(null);await assert.rejects(()=>dismiss([28]),/permission denied/);
  await login(11);assert.equal((await dismiss([23])).dismissed_rows,1,'admin across branches');
  await db.exec('reset role');
  assert.equal((await db.query('select dismissed from mpesa_callback_queue where id=$1',[id(28)])).rows[0].dismissed,false,'mixed invalid bulk action rolled back');
  const q=(await db.query('select * from mpesa_callback_queue where id=$1',[id(21)])).rows[0];
  assert.equal(q.confirmed,true);assert.equal(q.dismissed,true);assert.equal(q.dismissed_by,id(12));
  assert.equal((await db.query('select count(*)::int n from bripta_domain_audit')).rows[0].n,6,'each changed row audited once');
  assert.deepEqual(await snapshot(),before,'receipt amounts, repayments and loan balances preserved');
  await login(11);
  assert.equal((await db.query("select bripta_suspense_page('queue') result")).rows[0].result.rows.some(q=>q.id===id(21)),false,'dismissed receipt disappears from reader');
  console.log('PASS: single/bulk dismissal despite direct RLS denial, fallback copies, repeat safety, confirmed-payment rejection, atomic rollback, role/branch/business boundaries and unchanged amounts.');

  const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');const errors=[],success=[];let refreshes=0,calls=[];
  const selected=new Set(['mpesa:q','unmatched:m']);let fail=false;
  const context=vm.createContext({requireSuspenseManage:()=>true,confirm_:async()=>true,currentSection:'suspense',
    renderSuspense:async()=>refreshes++,renderRepayments:async()=>refreshes++,fmtMoney:x=>String(x),
    selectedSuspenseEntries:selected,visibleSuspenseEntries:[{key:'mpesa:q',type:'mpesa',id:'q',amount:100},{key:'unmatched:m',type:'unmatched',id:'m',amount:100}],
    document:{getElementById:()=>({})},updateSuspenseBulkControls:()=>{},
    toast:(message,type)=>{(type==='error'?errors:success).push(message);},
    supabaseClient:{rpc:async(name,args)=>{assert.equal(name,'bripta_dismiss_suspense');calls.push(args);
      return fail?{error:{message:'Branch denied'}}:{data:{ok:true,requested:args.p_queue_ids.length+args.p_manual_ids.length,dismissed_rows:1}};}}});
  const fragment=(a,b)=>{const start=html.indexOf(a);return html.slice(start,html.indexOf(b,start+a.length));};
  vm.runInContext(fragment('async function dismissMpesaPayment(','function calculatePaymentAllocation('),context);
  vm.runInContext(fragment('async function dismissSuspenseRecords(','async function openAddSuspenseModal('),context);
  vm.runInContext(fragment('async function dismissManualSuspense(','async function openSuspenseMatchModal('),context);
  await context.dismissMpesaPayment('q');await context.dismissManualSuspense('m');await context.bulkDismissSuspense();
  assert.equal(refreshes,3);assert.equal(calls.length,3);assert.equal(selected.size,0);
  const successes=success.length;fail=true;await context.dismissMpesaPayment('q');assert.equal(success.length,successes);assert.deepEqual(errors,['Branch denied']);
  assert.equal(refreshes,3,'failed action is not presented as saved');
  console.log('PASS: actual single/manual/bulk buttons use verified server result, refresh pending list and do not falsely announce failed dismissals.');
}finally{await db.close();}
