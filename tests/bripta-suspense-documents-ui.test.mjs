import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');
const fragment=(a,b)=>{const start=html.indexOf(a);assert.ok(start>=0);return html.slice(start,html.indexOf(b,start+a.length));};
let renders=0;
const elements={accountingStart:{value:'2026-10-01'},accountingEnd:{value:'2026-10-02'},accountingCustomDates:{style:{}}};
class FixedDate extends Date{constructor(...args){super(...(args.length?args:['2026-10-05T12:00:00Z']));}}
const context=vm.createContext({Date:FixedDate,Intl,document:{getElementById:id=>elements[id]},
  renderAccountingLegacy:()=>renders++,renderAccounting:()=>{throw Error('Wrong journal report');},
  appFilters:{accounting:{type:'this_month'}}});
vm.runInContext(fragment('function updateDateFilter(','const $$ ='),context);
for(const type of ['yesterday','this_week','this_month'])context.updateDateFilter('accounting',type);
context.updateDateFilter('accounting','custom');context.applyCustomDate('accounting');
assert.equal(renders,4);
const rows=[{date:'2026-10-04T20:59:59Z',amount:100},{date:'2026-10-04T21:00:00Z',amount:200},{date:'2026-10-01T12:00:00Z',amount:300}];
assert.equal(context.filterByDate(rows,'date',{type:'yesterday'}).reduce((s,r)=>s+r.amount,0),100);
assert.equal(context.filterByDate(rows,'date',{type:'this_week'}).reduce((s,r)=>s+r.amount,0),200);
assert.equal(context.filterByDate(rows,'date',context.appFilters.accounting).reduce((s,r)=>s+r.amount,0),300);
console.log('PASS: Accounting period controls use original records; yesterday, Monday week boundary and custom dates use Kenya time.');

const main={innerHTML:''},errors=[];
Object.assign(context,{$:()=>main,canManageSuspense:()=>false,selectedBranchId:'branch',
  selectedSuspenseEntries:new Set(),visibleSuspenseEntries:[],
  escapeHtml:x=>String(x),fmtDate:x=>x,fmtDateTime:x=>x,fmtMoney:x=>`KES ${x}`,paymentPhoneHtml:()=>'',toast:x=>errors.push(x),
  supabaseClient:{rpc:async(name,args)=>{
    assert.equal(name,'bripta_suspense_page');assert.equal(args.p_branch,'branch');
    if(args.p_source==='manual')return {data:{rows:[{id:'m1',mpesa_reference:'UNKNOWN',amount:150},{id:'m2',amount:50}],next_after:null}};
    return {data:args.p_after?{rows:[{id:'q2',trans_id:'SECOND',trans_amount:200}],next_after:null}:
      {rows:[{id:'q1',trans_id:'UNKNOWN',trans_amount:150,dismissed:null}],next_after:'q1'}};
  }}});
vm.runInContext(fragment('async function fetchBriptaSuspenseRows(','function toggleSuspenseSelection('),context);
await context.renderSuspense();
assert.deepEqual(errors,[]);assert.equal(context.visibleSuspenseEntries.length,3);
assert.ok(main.innerHTML.includes('KES 400'));assert.ok(main.innerHTML.includes('View only'));
assert.ok(!main.innerHTML.includes('onclick="openSuspenseMatchModal'));
console.log('PASS: Suspense pages load, duplicate receipts count once and officers retain view-only controls.');

const calls=[];let fail=true;
const bucket={upload:async(path)=>{calls.push(['upload',path]);return {};},remove:async(paths)=>{calls.push(['remove',...paths]);return {};}};
Object.assign(context,{currentUser:{business_id:'BIZ-B3F5E5D9'},crypto:{randomUUID:()=> 'new-id'},console,
  supabaseClient:{storage:{from:()=>bucket}},sb:()=>({select(){return this;},eq(){return this;},
    maybeSingle:async()=>({data:{object_path:'old-file'}}),upsert:async()=>({error:fail?{message:'Save interrupted'}:null})})});
vm.runInContext(readFileSync(new URL('../bripta-client-documents.js',import.meta.url),'utf8'),context);
assert.equal((context.clientDocumentInputs().match(/type="file"/g)||[]).length,6);
assert.throws(()=>context.validateClientDocument('client_passport',{type:'application/pdf',size:10}),/passport/);
assert.throws(()=>context.validateClientDocument('client_id_front',{type:'image/jpeg',size:9*1024*1024}),/8 MB/);
const files=[{kind:'client_id_front',file:{name:'front.pdf',type:'application/pdf',size:10}}];
assert.equal((await context.saveClientDocumentFiles('client',files)).length,1);
assert.ok(!calls.some(c=>c[0]==='remove'&&c[1]==='old-file'),'failed replacement preserves old file');
assert.equal(vm.runInContext("pendingClientDocumentUploads.get('client').length",context),1);
fail=false;
assert.equal((await context.saveClientDocumentFiles('client',files)).length,0);
assert.ok(calls.some(c=>c[0]==='remove'&&c[1]==='old-file'));
assert.equal(vm.runInContext('pendingClientDocumentUploads.size',context),0);
console.log('PASS: six upload slots, type/size validation, failed-upload cleanup and retry without recreating clients.');
