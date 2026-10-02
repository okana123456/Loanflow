const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(new URL('../index.html',`file://${__filename.replace(/\\/g,'/')}`),'utf8');
for(const [,script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))if(script.trim())new vm.Script(script);
const main={innerHTML:''};const notices=[];const portfolioLoan={id:'loan-juma',loan_no:'TEST',client_id:'client',loan_officer_id:'juma',status:'active',outstanding_balance:1000,overdue_days:1,arrears_amount:500,loan_clients:{full_name:'Test Client',phone:'0700000000'}};
const context=vm.createContext({
  console,window:{},document:{getElementById:()=>null},
  $:()=>main,currentUser:{id:'juma',business_id:'BIZ-B3F5E5D9',role:'loan_officer'},
  cachedData:{loans:[portfolioLoan,{...portfolioLoan,id:'another',loan_officer_id:'other'}],schedules:[{id:'due',loan_id:'loan-juma',due_date:'2026-10-01',total_due:500,total_paid:0,status:'overdue',installment_no:1}],staff:[{id:'juma',name:'Juma Gad'}],clients:[]},
  hasRole:(...roles)=>roles.includes('loan_officer'),canonicalLoanOfficerId:x=>x,
  today:()=> '2026-10-02',loadBriptaTables:async()=>{},
  sb:()=>{const q={select:()=>q,eq:()=>q,then:(resolve)=>resolve({data:[],error:null})};return q},
  fmtMoney:x=>Number(x).toFixed(2),fmtDate:x=>x,buildDuesTable:()=>'<table></table>',
  toast:x=>notices.push(x),charts:{},Chart:()=>{}
});
const arrearsStart=html.indexOf('async function renderOverdue(');
const arrearsEnd=html.indexOf('\nfunction buildDuesTable(',arrearsStart);
vm.runInContext(html.slice(arrearsStart,arrearsEnd),context);
(async()=>{
  await context.renderOverdue();
  assert.deepEqual(notices,[],'arrears renders without an error');
  assert.ok(main.innerHTML.includes('Test Client'));
  assert.ok(main.innerHTML.includes('Refresh'));
  assert.ok(!main.innerHTML.includes('another'));
  assert.ok(!html.slice(arrearsStart,arrearsEnd).includes("rpc('bripta_refresh_my_loan_aging')"),'opening arrears must not write to all loans');
  const approveStart=html.indexOf('async function approveApp(');
  const approveEnd=html.indexOf('\nasync function rejectApp(',approveStart);
  const approveContext=vm.createContext({currentUser:{id:'admin',business_id:'BIZ-B3F5E5D9'},confirm_:async()=>true,
    sb:()=>{const q={update:()=>q,eq:()=>q,select:()=>q,single:async()=>({data:null,error:{message:'RLS denied approval'}})};return q},
    toast:x=>notices.push(x),audit:()=>{throw Error('audit must not run')},closeModal:()=>{throw Error('modal must not close')},setTimeout:()=>{throw Error('disbursement must not start')}});
  vm.runInContext(html.slice(approveStart,approveEnd),approveContext);
  await approveContext.approveApp('app');
  assert.equal(notices.at(-1),'RLS denied approval');
  const officerStart=html.indexOf('async function assignedLoanOfficerForClient(');
  const officerEnd=html.indexOf('\n// Low-egress local cache',officerStart);
  const officerContext=vm.createContext({currentUser:{id:'admin',business_id:'BIZ-B3F5E5D9'},
    hasRole:role=>role==='admin',canonicalLoanOfficerId:x=>x,
    sb:table=>{const q={select:()=>q,eq:()=>q,
      single:async()=>({data:{id:'client',branch_id:'migori',loan_officer_id:'juma'},error:null}),
      in:async()=>({data:[{id:'admin',role:'admin',is_active:true,branch_id:'migori'},
        {id:'juma',role:'loan_officer',is_active:true,branch_id:'migori'}],error:null})};return q}
  });
  vm.runInContext(html.slice(officerStart,officerEnd),officerContext);
  assert.equal(await officerContext.assignedLoanOfficerForClient('client',['admin']),'juma',
    'issuing admin must keep the client with the assigned loan officer');
  assert.ok(html.includes("existingScheduleCount!==Number(appCheck.applied_term_weeks)"),
    'a partially created loan cannot be silently finalized');
  console.log('PASS: officer arrears render read-only with assigned portfolio; rejected application approval cannot start disbursement.');
})().catch(e=>{console.error(e);process.exitCode=1});
