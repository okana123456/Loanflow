const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(new URL('../index.html',`file://${__filename.replace(/\\/g,'/')}`),'utf8');
const between=(start,end)=>{
  const a=html.indexOf(start),b=html.indexOf(end,a+start.length);
  assert.ok(a>=0&&b>a);
  return html.slice(a,b);
};

let calls=0;
let written=null;
const memory={repayments:[{id:'old',amount:10}]};
const cacheContext=vm.createContext({
  BRIPTA_MIN_SYNC_MS:60000,BRIPTA_FULL_SYNC_MS:90*86400000,BRIPTA_SYNC_OVERLAP_MS:300000,
  BRIPTA_CACHE_MAP:{loan_repayments:'repayments'},cachedData:memory,
  readBriptaCache:async()=>({rows:[],savedAt:Date.now(),fullSyncAt:Date.now()}),
  fetchPagedBusinessRows:async(_table,configureQuery)=>{
    calls++;
    assert.equal(configureQuery,undefined,'explicit refresh must fetch the whole visible portfolio');
    return{data:[{id:'visible-after-policy-repair'}],error:null};
  },
  writeBriptaCache:async(_table,rows)=>{written=rows}
});
vm.runInContext(between('async function fetchBriptaTableOptimized(','\nfunction hydrateBriptaRelations('),cacheContext);
vm.runInContext(between('async function upsertBriptaCachedRows(','\nasync function fetchPagedBusinessRows('),cacheContext);
cacheContext.fetchBriptaPortfolioRows=(table,configureQuery)=>cacheContext.fetchPagedBusinessRows(table,configureQuery);

const loanRows=Array.from({length:120},(_,i)=>({id:`loan-${i}`,loan_officer_id:'officer-1'}));
loanRows.push({id:'other-officer-loan',loan_officer_id:'officer-2'});
const requested=[];
const scopedContext=vm.createContext({
  currentUser:{id:'officer-1'},cachedData:{loans:loanRows},
  hasRole:(...roles)=>roles.includes('loan_officer'),canonicalLoanOfficerId:id=>id,
  fetchPagedBusinessRows:async(_table,configureQuery)=>{
    const query={in:(_column,ids)=>{requested.push(ids);return query}};
    configureQuery(query);
    return{data:requested.at(-1).map(loan_id=>({id:`payment-${loan_id}`,loan_id})),error:null};
  }
});
vm.runInContext(between('function isBriptaOfficerOnly(','\nasync function fetchBriptaTableOptimized('),scopedContext);

const stagedContext=vm.createContext({
  cachedData:{},hasRole:(...roles)=>roles.includes('loan_officer'),
  isBriptaOfficerOnly:()=>true,
  fetchBriptaTableOptimized:async table=>{
    if(table==='loan_repayments')assert.equal(stagedContext.cachedData.loans.length,1,
      'load visible loans before requesting their repayments');
    return{data:table==='loans'?[{id:'loan-1'}]:[],error:null};
  },BRIPTA_CACHE_MAP:{loans:'loans',loan_repayments:'repayments'}
});
vm.runInContext(between('function hydrateBriptaRelations(','\nasync function refreshBriptaReports('),stagedContext);

const officerContext=vm.createContext({
  currentUser:{id:'officer-1',role:'loan_officer'},selectedBranchId:'migori',cachedData:{loans:[]},
  canonicalLoanOfficerId:id=>id
});
vm.runInContext(between('function reportingOfficerVisible(','\n// ═══ KEEP ALIVE PING'),officerContext);

(async()=>{
  const result=await cacheContext.fetchBriptaTableOptimized('loan_repayments',true);
  assert.equal(calls,1);
  assert.equal(result.data[0].id,'visible-after-policy-repair');
  await cacheContext.upsertBriptaCachedRows('loan_repayments',[{id:'new',amount:50}]);
  assert.equal(memory.repayments.find(row=>row.id==='new').amount,50);
  assert.equal(written.find(row=>row.id==='new').amount,50,
    'a saved payment must replace the stale device snapshot before repainting');
  assert.equal(officerContext.reportingOfficerVisible({id:'officer-1',role:'loan_officer',is_active:true}),true,
    'an officer with no cached loans still gets their dashboard');
  assert.equal(officerContext.reportingOfficerVisible({id:'other-branch',role:'loan_officer',branch_id:'other',is_active:true}),false);
  const scoped=await scopedContext.fetchBriptaPortfolioRows('loan_repayments');
  assert.equal(scoped.data.length,120);
  assert.deepEqual(requested.map(batch=>batch.length),[50,50,20]);
  assert.ok(requested.every(batch=>!batch.includes('other-officer-loan')));
  const retried=[];
  scopedContext.fetchPagedBusinessRows=async(_table,configureQuery)=>{
    const query={in:(_column,ids)=>{retried.push(ids);return query}};
    configureQuery(query);
    const ids=retried.at(-1);
    return ids.length>25
      ? {data:[],error:{code:'57014',message:'canceling statement due to statement timeout'}}
      : {data:ids.map(loan_id=>({id:`payment-${loan_id}`,loan_id})),error:null};
  };
  const recovered=await scopedContext.fetchBriptaPortfolioRows('loan_repayments');
  assert.equal(recovered.data.length,120,'smaller retry batches preserve every payment');
  assert.ok(retried.some(batch=>batch.length===25),'timed-out batches are split');
  await stagedContext.loadBriptaTables(['loans','loan_repayments']);
  const repaymentMain={innerHTML:''};
  const repaymentContext=vm.createContext({
    $:()=>repaymentMain,
    cachedData:{loans:[{id:'my-loan',loan_officer_id:'officer-1'}],repayments:[
      {id:'mine',loan_id:'my-loan',amount:50,payment_date:'2026-10-04'},
      {id:'other',loan_id:'other-loan',amount:70,payment_date:'2026-10-04'}
    ]},
    currentUser:{id:'officer-1',role:'loan_officer'},
    appFilters:{repayments:{type:'all_time'}},
    loadBriptaTables:async()=>{},filterByDate:rows=>rows,
    repaymentBusinessDate:r=>r.payment_date,
    hasRole:(...roles)=>roles.includes('loan_officer'),
    canonicalLoanOfficerId:id=>id,
    buildRepaymentBalanceMap:()=>({}),
    paginate:rows=>({items:rows,totalPages:1,page:1,total:rows.length}),
    fmtMoney:n=>String(n),getDateFilterHtml:()=>'',escapeHtml:x=>String(x),
    repaymentSenderHtml:()=>'',repaymentDisplayDateTime:r=>r.payment_date,
    paginationHtml:()=>'',toast:()=>{}
  });
  vm.runInContext(between('let repPage=1;','\nasync function confirmMpesaPayment('),repaymentContext);
  await repaymentContext.renderRepayments();
  assert.equal(vm.runInContext('visibleRepayments.length',repaymentContext),1);
  assert.equal(vm.runInContext('visibleRepayments[0].id',repaymentContext),'mine',
    'officer payments should be matched by visible loan ID even without an embedded loan relation');
  console.log('PASS: Officer repayments load by assigned loan in retryable batches; refreshed dashboard and cache stay visible.');
})().catch(error=>{console.error(error);process.exitCode=1});
