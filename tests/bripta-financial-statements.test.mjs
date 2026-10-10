import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
const db=new PGlite();const biz='BIZ-B3F5E5D9',id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
try{
  await db.exec(`create role authenticated;create role anon;create schema auth;
    create function auth.jwt() returns jsonb language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb$$;
    create function auth.uid() returns uuid language sql stable as $$select (auth.jwt()->>'sub')::uuid$$;
    grant usage on schema public,auth to authenticated,anon;
    create table loan_staff(id uuid primary key,auth_user_id uuid,email text,role text,business_id text,branch_id uuid,is_active boolean);
    create table bripta_branches(id uuid primary key,business_id text);
    create table bripta_staff_permissions(staff_id uuid,business_id text,view_accounting boolean);
    create table bripta_expenses(id uuid primary key,business_id text,branch_id uuid,expense_date date,category text,custom_category text,amount numeric,status text);
    create table bripta_accounting_entries(business_id text,branch_id uuid,entry_date date,account_code text,account_name text,account_type text,debit numeric,credit numeric,source_table text,source_id text,entry_key text);
    create table loans(id uuid primary key,business_id text,branch_id uuid,disbursement_date date,disbursed_amount numeric);
    create table loan_repayments(id uuid primary key,business_id text,branch_id uuid,payment_date timestamptz,amount numeric);
    create table bripta_assets(id uuid primary key,business_id text,branch_id uuid,purchase_date date,initial_price numeric);
  `);
  for(const[n,role,business,branch]of [[11,'admin',biz,1],[12,'branch_manager',biz,1],[13,'cashier',biz,1],[14,'admin','OTHER',9],[15,'loan_officer',biz,1]])
    await db.query('insert into loan_staff values($1,$1,$2,$3,$4,$5,true)',[id(n),`u${n}@test.invalid`,role,business,id(branch)]);
  await db.exec(`insert into bripta_branches values('${id(1)}','${biz}'),('${id(2)}','${biz}'),('${id(9)}','OTHER');
    insert into bripta_staff_permissions values('${id(13)}','${biz}',true);
    insert into loans values('${id(51)}','${biz}','${id(1)}','2026-10-01',100);
    insert into loan_repayments values('${id(52)}','${biz}','${id(1)}','2026-10-02T22:00:00Z',20);
    insert into bripta_assets values('${id(53)}','${biz}','${id(1)}','2026-10-01',50);
  `);
  for(const[n,branch,date,category,custom,amount,status]of [
    [21,1,'2026-10-02','fuel',null,30,'approved'],[22,1,'2026-10-02','rent',null,100,'pending'],
    [23,1,'2026-10-02','rent',null,100,'rejected'],[24,1,'2026-10-03','airtime',null,10,'paid'],
    [25,2,'2026-10-02','custom','Security',70,'approved'],[26,1,'2026-11-01','rent',null,200,'approved']])
    await db.query('insert into bripta_expenses values($1,$2,$3,$4,$5,$6,$7,$8)',[id(n),biz,id(branch),date,category,custom,amount,status]);
  const entry=async(branch,date,code,name,type,debit,credit,source,ref,key)=>db.query('insert into bripta_accounting_entries values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',[biz,id(branch),date,code,name,type,debit,credit,source,ref,key]);
  await entry(1,'2026-09-01','1000','Cash','asset',1000,0,'opening_balance','opening','opening_cash');
  await entry(1,'2026-09-01','3000','Owner Capital','equity',0,1000,'opening_balance','opening','opening_equity');
  await entry(1,'2026-10-01','1000','Cash','asset',100,0,'income','receipt','cash');
  await entry(1,'2026-10-01','4000','Interest','income',0,100,'income','receipt','income');
  await entry(1,'2026-10-02','5000','Fuel','expense',30,0,'bripta_expenses',id(21),'expense_debit');
  await entry(1,'2026-10-02','1000','Cash','asset',0,30,'bripta_expenses',id(21),'expense_payment');
  await entry(2,'2026-09-01','1000','Cash','asset',500,0,'opening_balance','b2','opening_cash');
  await entry(2,'2026-09-01','3000','Owner Capital','equity',0,500,'opening_balance','b2','opening_equity');
  await entry(1,'2026-11-01','5000','Future Expense','expense',200,0,'bripta_expenses',id(26),'expense_debit');
  const snapshot=async()=>db.query('select (select sum(amount) from bripta_expenses) expenses,(select sum(disbursed_amount) from loans) loans,(select sum(amount) from loan_repayments) repayments,(select sum(debit+credit) from bripta_accounting_entries) journal');
  const before=await snapshot();
  await db.exec(`alter table loans add column client_id uuid,add column loan_no text default '251959';
    create table loan_clients(id uuid primary key,business_id text,full_name text);
    create table loan_penalties(id uuid primary key,loan_id uuid,penalty_amount numeric,date_charged date,reason text,is_waived boolean);
    insert into loan_clients values('${id(90)}','${biz}','Hellen Akinyi Opiyo');
    update loans set client_id='${id(90)}' where id='${id(51)}';
    insert into loan_penalties values('${id(91)}','${id(51)}',525,'2026-10-10','Rollover Penalty (15%)',false),
      ('${id(92)}','${id(51)}',50,'2026-10-09','Waived rollover',true);
  `);
  const migration=readFileSync(new URL('../bripta-financial-statements-20261005.sql',import.meta.url),'utf8');
  await db.exec(migration);await db.exec(migration);
  const penaltyMigration=readFileSync(new URL('../bripta-accounting-penalties-20261010.sql',import.meta.url),'utf8');
  await db.exec(penaltyMigration);await db.exec(penaltyMigration);
  const login=async n=>{await db.exec('reset role');await db.query("select set_config('request.jwt.claims',$1,false)",[JSON.stringify({sub:id(n),email:`u${n}@test.invalid`})]);await db.exec('set role authenticated');};
  const report=async(branch=null,start='2026-10-01',end='2026-10-02')=>(await db.query('select bripta_financial_statements($1,$2,$3) result',[start,end,branch])).rows[0].result;
  await login(11);let r=await report(id(1));
  const charged=(await db.query('select bripta_accounting_penalties($1,$2,$3) result',['2026-10-10','2026-10-10',id(1)])).rows[0].result.rows;
  assert.equal(charged.length,1);assert.equal(charged[0].penalty_amount,525);assert.equal(charged[0].charged_on,'2026-10-10');assert.equal(charged[0].client_name,'Hellen Akinyi Opiyo');
  assert.equal((await db.query('select bripta_accounting_penalties($1,$2,$3) result',['2026-10-10','2026-10-10',id(2)])).rows[0].result.rows.length,0,'other branch excludes penalty');
  assert.equal(r.approved_expenses,30);assert.equal(r.expense_categories.length,1);
  assert.equal(r.accounts.find(a=>a.account_code==='1000').signed_balance,1070,'opening and prior income retained for balance sheet');
  assert.equal(r.missing_source_postings.loans,1);assert.equal(r.missing_source_postings.asset_purchases,1);
  assert.equal(r.missing_source_postings.repayments,0,'Kenya next-day payment excluded at cutoff');
  assert.equal(r.opening_balances_identified,true);assert.equal(r.unbalanced_sources,0);
  assert.equal((await report()).approved_expenses,100,'admin all branches');
  assert.equal((await report(id(1),'2026-10-02','2026-10-02')).approved_expenses,30);
  assert.equal((await report(id(1),'2026-10-03','2026-10-03')).approved_expenses,10,'paid expense counted');
  await assert.rejects(()=>report(null,'2026-10-03','2026-10-01'),/valid date range/);
  await login(12);assert.equal((await report()).approved_expenses,30);await assert.rejects(()=>report(id(2)),/Branch access/);
  assert.equal((await db.query('select bripta_accounting_penalties($1,$2) result',['2026-10-09','2026-10-10'])).rows[0].result.rows.length,2);
  await assert.rejects(()=>db.query('select bripta_accounting_penalties($1,$2,$3)',['2026-10-09','2026-10-10',id(2)]),/Branch access/);
  await login(13);assert.equal((await report()).approved_expenses,30);
  for(const n of [14,15]){await login(n);await assert.rejects(()=>report(),/Accounting permission/);await assert.rejects(()=>db.query('select bripta_accounting_penalties($1,$2)',['2026-10-09','2026-10-10']),/Accounting permission/);}
  await db.exec('reset role;set role anon');await assert.rejects(()=>report(),/permission denied/);
  await db.exec('reset role');assert.deepEqual(await snapshot(),before,'report and migration leave every source amount and posting unchanged');
  console.log('PASS: cumulative as-of balances, approved/paid expenses once, date boundaries, category totals, role/branch/business isolation and idempotent read-only SQL.');

  const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');
  const main={innerHTML:''},errors=[];
  const context=vm.createContext({Intl,Date,console,currentUser:{business_id:biz},selectedBranchId:id(1),
    appFilters:{accounting:{type:'custom',start:'2026-10-01',end:'2026-10-02'}},$:()=>main,
    escapeHtml:x=>String(x),fmtMoney:x=>'KES '+Number(x).toFixed(2),getDateFilterHtml:()=>'',REGISTRATION_FEE:5,
    repaymentBusinessDate:r=>r.payment_date,canonicalLoanOfficerId:x=>x,parseRegFeeFromNotes:()=>({amount:5}),toast:x=>errors.push(x),
    supabaseClient:{rpc:async(name,args)=>{assert.equal(args.p_branch,id(1));
      if(name==='bripta_accounting_penalties')return {data:{rows:charged,next_after:null}};
      assert.equal(name,'bripta_financial_statements');return {data:r};}},
    fetchPagedBusinessRows:async table=>({data:table==='loans'?[{id:'loan',client_id:'client',loan_officer_id:'officer',processing_fee:10,disbursed_amount:100,disbursement_date:'2026-10-01',status:'active'}]:
      table==='loan_repayments'?[{loan_id:'loan',amount:50,loan_portion:50,interest_portion:20,penalty_portion:3,payment_date:'2026-10-02'}]:
      table==='loan_clients'?[{id:'client',loan_officer_id:'officer',created_at:'2026-10-01'}]:[{id:'officer',name:'Officer',role:'loan_officer',is_active:true}]})});
  const fragment=(a,b)=>{const start=html.indexOf(a);return html.slice(start,html.indexOf(b,start+a.length));};
  vm.runInContext(fragment('function kenyaDateISO(','const $$ ='),context);
  vm.runInContext(readFileSync(new URL('../bripta-financial-statements.js',import.meta.url),'utf8'),context);
  const sheet=context.briptaBalanceSheet(r.accounts);
  assert.equal(sheet.assets,1070);assert.equal(sheet.retained,70);assert.equal(sheet.equity,1070);assert.equal(sheet.difference,0);
  vm.runInContext(fragment('async function renderAccountingLegacy()','// ═══ MULTI-BRANCH'),context);
  await context.renderAccountingLegacy();assert.deepEqual(errors,[]);
  assert.ok(main.innerHTML.includes('KES 38.00'),'original revenue retained');
  assert.ok(main.innerHTML.includes('KES 8.00'),'net profit deducts approved expense once');
  assert.ok(main.innerHTML.includes('Balance Sheet'));assert.ok(main.innerHTML.includes('1 loan disbursements'),'missing history explicitly shown');
  assert.ok(main.innerHTML.includes('Rollover Penalties Charged'));assert.ok(main.innerHTML.includes('KES 525.00'));assert.ok(main.innerHTML.includes('2026-10-10'));
  assert.ok(main.innerHTML.includes('KES 8.00'),'charged penalty is not counted again as collected profit');
  const waivedHtml=context.accountingPenaltyHtml({data:[...charged,{penalty_amount:50,is_waived:true,charged_on:'2026-10-09'}]});
  assert.ok(waivedHtml.includes('KES 525.00'));assert.ok(waivedHtml.includes('KES 50.00'));assert.ok(!waivedHtml.includes('KES 575.00'),'waived charges excluded from applied total');
  r={...r,approved_expenses:50};await context.renderAccountingLegacy();assert.ok(main.innerHTML.includes('Net loss'));assert.ok(main.innerHTML.includes('KES -12.00'));
  context.supabaseClient.rpc=async()=>({error:{message:'Statement unavailable'}});
  await context.renderAccountingLegacy();assert.ok(main.innerHTML.includes('Unavailable'));assert.ok(main.innerHTML.includes('KES 38.00'),'statement failure preserves income report');
  assert.ok(!main.innerHTML.includes('KES 0.00</div></div>'),'missing expense data is not falsely reported as zero');
  console.log('PASS: actual Accounting renderer keeps income, subtracts expenses once, shows loss, cumulative balance sheet, missing postings and unavailable-data state.');
}finally{await db.close();}
