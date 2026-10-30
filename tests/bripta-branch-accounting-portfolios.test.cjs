const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(require('node:path').join(__dirname,'..','index.html'),'utf8');
const start=html.indexOf('function currentAccountingPortfolioOfficers(');
const end=html.indexOf('\nasync function renderAccountingLegacy()',start);
assert(start>=0&&end>start);
const context=vm.createContext({currentUser:{business_id:'BIZ-B3F5E5D9'},canonicalLoanOfficerId:id=>id||null});
vm.runInContext(html.slice(start,end),context);
const migori='00000000-0000-4000-8000-000000000001',mbita='mbita';
const staff=[
  {id:'brian',name:'Brian Ochieng',role:'loan_officer',branch_id:migori,is_active:true},
  {id:'faith',name:'Faith',role:'loan_officer',branch_id:migori,is_active:true},
  {id:'juma',name:'Juma',role:'loan_officer',branch_id:migori,is_active:true},
  {id:'phelix',name:'Phelix',role:'loan_officer',branch_id:migori,is_active:true},
  {id:'george',name:'George',role:'loan_officer',branch_id:mbita,is_active:true},
  {id:'james',name:'James',role:'loan_officer',branch_id:mbita,is_active:true},
  {id:'admin',role:'admin,loan_officer',branch_id:migori,is_active:true},
  {id:'former',role:'loan_officer',branch_id:migori,is_active:false}
];
const officers=branch=>Array.from(context.currentAccountingPortfolioOfficers(staff,branch),s=>s.id);
assert.deepEqual(officers(migori),['brian','faith','juma','phelix']);
assert.deepEqual(officers(mbita),['george','james']);
assert.deepEqual(officers(null),['brian','faith','juma','phelix','george','james']);
assert.equal(context.briptaAccountingRevenueOwner('george',migori,staff),'brian');
assert.equal(context.briptaAccountingRevenueOwner(null,migori,staff),'brian');
assert.equal(context.briptaAccountingRevenueOwner('george',mbita,staff),'george');
assert.equal(context.briptaAccountingRevenueOwner(null,mbita,staff),'unassigned');
assert.match(html,/const historicalOther=Object\.entries\(officerStats\)/);
assert.match(html,/Included in the totals above/);
console.log('PASS: Migori shows four current portfolios, Mbita two; historical revenue remains in totals.');
