const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const R=require('../bripta-reporting.js');
const loan=(id,client_id,extra={})=>({id,client_id,status:'active',total_payable:200,total_paid:100,outstanding_balance:100,...extra});
const schedule=(id,loan_id,due_date,total_paid=0)=>({id,loan_id,due_date,total_due:100,total_paid});
const l=loan('l','c');
const ss=[schedule('s1','l','2026-10-01'),schedule('s2','l','2026-10-08')];
const reps=[{id:'r',loan_id:'l',amount:130,loan_portion:100,payment_date:'2026-10-03'}];
const date=r=>r.payment_date;
const before=JSON.stringify({l,ss,reps});
let state=R.allocatedSchedules([l],ss,reps,'2026-10-02',date);
assert.equal(R.periodCollection(state.schedules,()=>true,'2026-10-02').unpaid,100);
state=R.allocatedSchedules([l],ss,reps,'2026-10-03',date);
assert.equal(R.periodCollection(state.schedules,()=>true,'2026-10-03').unpaid,0);
assert.equal(R.periodCollection(state.schedules,()=>true,'2026-10-03').paid,100,'fees/excess excluded');
const afterMonth=[{...reps[0],payment_date:'2026-11-01'}];
assert.equal(R.periodCollection(R.allocatedSchedules([l],ss,afterMonth,'2026-10-31',date).schedules,()=>true,'2026-10-31').paid,0);
const completed=loan('l','c',{status:'completed',total_paid:200,outstanding_balance:0});
assert.equal(R.periodCollection(R.allocatedSchedules([completed],ss,[], '2026-10-31',date).schedules,()=>true,'2026-10-31').paid,200);
assert.equal(JSON.stringify({l,ss,reps}),before,'reporting must not mutate source data');
// Two arrears loans owned by one client count once. Clean clients affect BQ once.
const loans=[loan('a','one',{total_paid:0,outstanding_balance:200}),loan('b','one',{total_paid:0,outstanding_balance:200}),loan('c','two',{total_paid:200,outstanding_balance:50})];
const schedules=loans.flatMap(x=>[schedule(x.id+'1',x.id,'2026-10-01',x.total_paid/2),schedule(x.id+'2',x.id,'2026-10-08',x.total_paid/2)]);
const q=R.portfolioQuality(loans,schedules,'2026-10-02');
assert.equal(q.clients,2);assert.equal(q.arrearsClients,1);assert.equal(q.bq,50);assert.equal(q.arrears,200);
assert.equal(q.par,200/450*100);
assert.deepEqual(R.qualityTotals(q.active.concat(q.active)),q,'totals deduplicate loans');
const split=[R.portfolioQuality(loans.slice(0,2),schedules,'2026-10-02'),R.portfolioQuality(loans.slice(2),schedules,'2026-10-02')];
assert.deepEqual(R.qualityTotals(split.flatMap(x=>x.active)),q,'combined dashboard uses same calculation');
const irregular=R.portfolioQuality([loan('bad','e',{total_paid:7515,total_payable:7995,outstanding_balance:199.5,arrears_amount:199.5,overdue_days:25})],[schedule('bad1','bad','36238-01-01',4265)],'2026-10-02');
assert.equal(irregular.arrears,199.5,'preserve accepted balance for historical schedule discrepancy');
const dueLoans=[loan('a','same',{loan_clients:{full_name:'Mary'},outstanding_balance:90}),loan('b','same',{loan_clients:{full_name:'Mary'},outstanding_balance:80}),loan('c','different',{loan_clients:{full_name:'Mary'}})];
const due=R.dueClients([schedule('1','a','2026-10-02'),schedule('2','a','2026-10-02'),schedule('3','b','2026-10-02'),schedule('4','c','2026-10-02')],dueLoans);
assert.equal(due.length,2,'group by client ID, never by name');assert.equal(due[0].loans.length,2);assert.equal(due[0].loan_balance,170);assert.equal(due[0].remaining,300);
assert.equal(R.actorName({user_id:'auth'},[{id:'staff',auth_user_id:'auth',name:'Juma Gad',is_active:false}]),'Juma Gad');
assert.equal(R.actorName({user_id:null},[{auth_user_id:null,name:'Someone'}]),'System');
assert.equal(R.actorName({user_id:'missing'},[]),'Former or unknown staff');
const html=fs.readFileSync(new URL('../index.html',`file://${__filename.replace(/\\/g,'/')}`),'utf8');
for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))if(m[1].trim())new vm.Script(m[1]);
assert.ok(html.includes('<script src="bripta-reporting.js"></script>'));
// Execute the actual HTML renderer with two loans for one client.
const context=vm.createContext({BriptaReporting:R,window:{_staffMap:{}},canonicalLoanOfficerId:x=>x,hasRole:()=>true,escapeHtml:x=>String(x),fmtMoney:x=>String(x)});
const fn=html.slice(html.indexOf('function buildDueTodayTable('),html.indexOf('async function loadDueByDate('));
vm.runInContext(fn,context);
const rendered=context.buildDueTodayTable([schedule('1','a','2026-10-02'),schedule('2','b','2026-10-02')],dueLoans,[],'2026-10-02');
assert.equal((rendered.match(/<tr\b/g)||[]).length,2,'one heading and one client row');
assert.ok(rendered.includes("openPaymentModal('a')"));assert.ok(rendered.includes("openPaymentModal('b')"));
console.log('PASS: period allocation, cutoff dates, completed loans, client BQ/PAR, due-client grouping, audit names, HTML syntax and rendered payment actions.');
// Exercise the real dashboard templates, with no network or financial writes.
(async()=>{
  const main={innerHTML:''};const errors=[];
  Object.assign(context,{
    $:()=>main,document:{getElementById:()=>null},charts:{},currentUser:{id:'juma',business_id:'BIZ-B3F5E5D9',name:'Juma Gad',role:'admin'},
    selectedBranchId:null,cachedData:{loans:loans.map(l=>({...l,loan_officer_id:'juma'})),schedules,repayments:[],applications:[],clients:[],staff:[{id:'juma',name:'Juma Gad',role:'loan_officer',is_active:true}]},
    loadBriptaTables:async()=>{},today:()=> '2026-10-02',localISODate:()=> '2026-10-02',repaymentBusinessDate:date,
    fmtDate:x=>x,fmtLongDate:x=>x,statusBadge:x=>x,branchName:()=> 'Migori',getDateFilterHtml:()=>'',appFilters:{dashboard:{type:'this_month'}},
    filterByDate:rows=>rows,toast:message=>errors.push(message)
  });
  const visibleStart=html.indexOf('function reportingOfficerVisible(');
  vm.runInContext(html.slice(visibleStart,html.indexOf('\n//',visibleStart)),context);
  for(const name of ['renderDashboard','renderOfficerDashboard','renderOfficerPerformance']){
    const start=html.indexOf('async function '+name+'(');
    const end=html.indexOf('\n// ═══',start);
    vm.runInContext(html.slice(start,end),context);
    await context[name]();
    assert.deepEqual(errors,[],name+' must render without runtime errors');
    assert.ok(main.innerHTML.includes('Dashboard'));
    if(name==='renderOfficerPerformance'){
      assert.ok(main.innerHTML.includes('TOTALS'));
      assert.ok(main.innerHTML.includes('50.0%'));
      assert.ok(main.innerHTML.includes('Migori'));
      assert.equal(context.window._perfRows[0].quality.arrearsClients,1);
    }
  }
  console.log('PASS: real main, individual officer and performance templates render successfully with shared client counts.');
})().catch(e=>{console.error(e);process.exitCode=1;});
