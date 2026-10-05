import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import Reporting from '../bripta-reporting.js';

const sql=readFileSync(new URL('../bripta-officer-portfolio-reader-20261005.sql',import.meta.url),'utf8');
const db=new PGlite();
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const biz='BIZ-B3F5E5D9',branch=id(1),otherBranch=id(2);
try{
  await db.exec(`
    create role authenticated; create role anon; create schema auth;
    create function auth.jwt() returns jsonb language sql stable as $$
      select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb $$;
    create function auth.uid() returns uuid language sql stable as $$ select (auth.jwt()->>'sub')::uuid $$;
    grant usage on schema public,auth to authenticated,anon;
    create table loan_staff(id uuid primary key, auth_user_id uuid, email text, name text,
      role text, business_id text, branch_id uuid, is_active boolean);
    create table loan_clients(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,full_name text);
    create table loans(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid,
      client_id uuid,outstanding_balance numeric,status text default 'active',loan_no text default 'TEST',
      principal_amount numeric default 1000,total_payable numeric default 1200,total_paid numeric default 200,
      created_at timestamptz default now(),updated_at timestamptz default now());
    create table loan_applications(id uuid primary key,business_id text,branch_id uuid,loan_officer_id uuid);
    create table loan_schedules(id uuid primary key,business_id text,branch_id uuid,loan_id uuid,total_due numeric,
      due_date text default '2026-10-01',total_paid numeric default 0,installment_no integer default 1,created_at timestamptz default now());
    create table loan_repayments(id uuid primary key,business_id text,branch_id uuid,loan_id uuid,amount numeric,
      created_at timestamptz default now());
  `);
  for(const [n,role,business,b,is_active] of [
    [11,'loan_officer',biz,branch,true],[12,'loan_officer',biz,branch,true],
    [13,'loan_officer',biz,otherBranch,true],[14,'loan_officer','OTHER',branch,true],
    [15,'admin',biz,branch,true],[16,'branch_manager',biz,branch,true],
    [17,'cashier',biz,branch,true],[18,'loan_officer',biz,branch,false]
  ])await db.query('insert into loan_staff values($1,$1,$2,$3,$4,$5,$6,$7)',[id(n),`u${n}@test.invalid`,`Officer ${n}`,role,business,b,is_active]);
  await db.query(`insert into loan_clients select md5('c'||g)::uuid,$1,$2,$3,'Client '||g from generate_series(1,581) g`,[biz,branch,id(11)]);
  await db.query(`insert into loans(id,business_id,branch_id,loan_officer_id,client_id,outstanding_balance)
    select md5('l'||g)::uuid,$1,$2,$3,md5('c'||g)::uuid,1000 from generate_series(1,581) g`,[biz,branch,id(11)]);
  await db.query(`insert into loan_repayments(id,business_id,branch_id,loan_id,amount,created_at)
    select md5('r'||g)::uuid,$1,$2,md5('l'||((g%581)+1))::uuid,50,'2026-10-01'::timestamptz
    from generate_series(1,8233) g`,[biz,branch]);
  await db.query(`insert into loan_schedules(id,business_id,branch_id,loan_id,total_due)
    select md5('s'||g)::uuid,$1,$2,md5('l'||((g%581)+1))::uuid,100 from generate_series(1,2671) g`,[biz,branch]);
  for(const [n,business,b,owner] of [[21,biz,branch,12],[22,biz,otherBranch,13],[23,'OTHER',branch,14]]){
    await db.query('insert into loans(id,business_id,branch_id,loan_officer_id,outstanding_balance) values($1,$2,$3,$4,900)',[id(n),business,b,id(owner)]);
    await db.query('insert into loan_repayments(id,business_id,branch_id,loan_id,amount) values($1,$2,$3,$4,999)',[id(n+100),business,b,id(n)]);
  }
  // Child rows with a mismatched tenant/branch must never be exposed.
  await db.query(`insert into loan_repayments(id,business_id,branch_id,loan_id,amount)
    values($1,$2,$3,md5('l1')::uuid,888),($4,'OTHER',$5,md5('l1')::uuid,777)`,[id(200),biz,otherBranch,id(201),branch]);
  for(const table of ['loan_staff','loan_clients','loans','loan_applications','loan_schedules','loan_repayments']){
    await db.exec(`alter table ${table} enable row level security;
      create policy deny_direct on ${table} for select to authenticated using(false);
      grant select on ${table} to authenticated;`);
  }
  const snapshot=async()=> (await db.query(`select
    (select sum(amount) from loan_repayments) as payments,
    (select sum(outstanding_balance) from loans) as balances,
    (select sum(total_due) from loan_schedules) as scheduled`)).rows;
  const before=await snapshot();
  await db.exec(sql);
  await db.exec(sql); // idempotence
  const login=async(n)=>{
    await db.exec('reset role');
    await db.query("select set_config('request.jwt.claims',$1,false)",[JSON.stringify({sub:id(n),email:`u${n}@test.invalid`,role:'authenticated'})]);
    await db.exec('set role authenticated');
  };
  const page=async(table,after=null,since=null,limit=500)=>(await db.query(
    'select public.bripta_officer_portfolio_page($1,$2,$3,$4) as result',[table,after,since,limit])).rows[0].result;
  await login(11);
  assert.equal((await db.query('select count(*)::int n from loan_repayments')).rows[0].n,0,'fixture denies direct table access');
  const start=performance.now();
  let cursor=null,rows=[];
  do{const p=await page('loan_repayments',cursor);rows.push(...p.rows);cursor=p.next_after;}while(cursor);
  assert.equal(rows.length,8233);
  assert.equal(new Set(rows.map(r=>r.id)).size,8233,'keyset paging has no duplicates or omissions');
  assert.equal(rows.reduce((sum,r)=>sum+Number(r.amount),0),8233*50);
  assert.ok(rows.every(r=>r.business_id===biz&&r.branch_id===branch));
  assert.equal((await page('loan_repayments',null,'2026-10-02')).rows.length,0,'delta excludes old rows');
  assert.equal((await page('loan_staff')).rows[0].id,id(11));
  assert.equal((await page('loan_clients')).rows.length,500);
  assert.equal((await page('loans',null,null,999999)).rows.length,500,'server caps page size');
  assert.equal((await page('loan_schedules',null,null,1)).rows.length,1);
  await assert.rejects(()=>page('loan_staff; drop table loans'),/Unsupported/);
  console.log(`PASS: 8,233 historical payments, stable paging, totals and narrow identity checks (${Math.round(performance.now()-start)} ms synthetic PostgreSQL).`);
  // Exercise actual officer loaders and all three reported broken screens
  // against PostgreSQL in an authenticated officer session with direct RLS
  // reads denied. This is not an administrator-only frontend mock.
  const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');
  const fragment=(a,b)=>{const from=html.indexOf(a);return html.slice(from,html.indexOf(b,from+a.length));};
  const main={innerHTML:''},cache=new Map(),errors=[];
  const context=vm.createContext({
    currentUser:{id:id(11),name:'Officer 11',role:'loan_officer',business_id:biz,branch_id:branch},
    selectedBranchId:branch,cachedData:{},currentSection:'repayments',
    hasRole:(...roles)=>roles.includes('loan_officer'),canonicalLoanOfficerId:value=>value,
    BRIPTA_MIN_SYNC_MS:60000,BRIPTA_FULL_SYNC_MS:90*86400000,BRIPTA_SYNC_OVERLAP_MS:300000,
    BRIPTA_CACHE_MAP:{loan_clients:'clients',loans:'loans',loan_repayments:'repayments',loan_staff:'staff',loan_schedules:'schedules',loan_applications:'applications'},
    readBriptaCache:async table=>cache.get(table),
    writeBriptaCache:async(table,data,fullSyncAt)=>cache.set(table,{rows:structuredClone(data),savedAt:Date.now(),fullSyncAt}),
    fetchPagedBusinessRows:()=>{throw new Error('Unexpected legacy RLS read')},
    supabaseClient:{rpc:async(_name,args)=>({data:await page(args.p_table,args.p_after,args.p_since,args.p_limit),error:null})},
    $:()=>main,window:{},document:{getElementById:()=>null},charts:{},BriptaReporting:Reporting,
    today:()=> '2026-10-05',localISODate:()=> '2026-10-05',repaymentBusinessDate:r=>String(r.created_at).slice(0,10),
    appFilters:{repayments:{type:'all_time'}},filterByDate:rows=>rows,
    fmtMoney:value=>String(value),fmtDate:value=>value,fmtLongDate:value=>value,
    statusBadge:value=>value,escapeHtml:value=>String(value),branchName:()=> 'Migori',
    getDateFilterHtml:()=>'',paginationHtml:()=>'',paginate:rows=>({items:rows.slice(0,25)}),
    buildRepaymentBalanceMap:()=>({}),repaymentSenderHtml:()=>'',repaymentDisplayDateTime:r=>r.created_at,
    toast:message=>errors.push(message)
  });
  vm.runInContext(fragment('function isBriptaOfficerOnly(','\nasync function refreshBriptaReports('),context);
  vm.runInContext(fragment('function reportingOfficerVisible(','\n// ═══ KEEP ALIVE PING'),context);
  vm.runInContext(fragment('let repPage=1;','\nasync function confirmMpesaPayment('),context);
  await context.renderRepayments();
  assert.deepEqual(errors,[]);
  assert.equal(vm.runInContext('visibleRepayments.length',context),8233);
  for(const name of ['renderOfficerDashboard','renderOfficerPerformance']){
    vm.runInContext(fragment('async function '+name+'(','\n// ═══'),context);
    await context[name]();
    assert.deepEqual(errors,[],name);
    assert.ok(main.innerHTML.includes('Officer 11'),name+' renders the real officer');
  }
  assert.equal(context.window._perfRows[0].oLoans.length,581);
  assert.equal(context.window._perfRows[0].collected,8233*50);
  console.log('PASS: real Repayments, My Dashboard and Officer Performance render through the scoped database reader for an officer with 581 loans.');
  await login(12);
  assert.deepEqual((await page('loan_repayments')).rows.map(r=>r.loan_id),[id(21)]);
  await login(13);
  assert.deepEqual((await page('loan_repayments')).rows.map(r=>r.loan_id),[id(22)]);
  for(const n of [14,15,16,17,18]){
    await login(n);
    await assert.rejects(()=>page('loan_repayments'),/active Bripta loan officer/);
  }
  await db.exec('reset role; set role anon');
  await assert.rejects(()=>page('loan_repayments'),/permission denied/);
  await db.exec('reset role');
  assert.deepEqual(await snapshot(),before,'migration and reads preserve every financial total');
  console.log('PASS: other officers, branches, businesses, disabled staff, admins, cashiers and anonymous callers cannot cross the reader boundary; financial totals unchanged.');
}finally{await db.close();}
