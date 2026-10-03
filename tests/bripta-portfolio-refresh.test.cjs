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
  console.log('PASS: Refresh rebuilds visible repayment rows; officer dashboard keeps the signed-in officer visible.');
})().catch(error=>{console.error(error);process.exitCode=1});
