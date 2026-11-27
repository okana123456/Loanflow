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
Object.assign(context,{$:()=>main,canManageSuspense:()=>false,canManageSuspenseEntry:()=>false,
  branchName:id=>id==='branch'?'Migori':'Mbita',selectedBranchId:'branch',
  selectedSuspenseEntries:new Set(),visibleSuspenseEntries:[],
  escapeHtml:x=>String(x),fmtDate:x=>x,fmtDateTime:x=>x,fmtMoney:x=>`KES ${x}`,paymentPhoneHtml:()=>'',toast:x=>errors.push(x),
  supabaseClient:{rpc:async(name,args)=>{
    assert.equal(name,'bripta_suspense_page');assert.equal(args.p_branch,null);
    if(args.p_source==='manual')return {data:{rows:[{id:'m1',mpesa_reference:'UNKNOWN',amount:150,branch_id:'other'},{id:'m2',amount:50,branch_id:'branch'}],next_after:null}};
    return {data:args.p_after?{rows:[{id:'q2',trans_id:'SECOND',trans_amount:200,branch_id:'other'}],next_after:null}:
      {rows:[{id:'q1',trans_id:'UNKNOWN',trans_amount:150,dismissed:null,branch_id:'branch'}],next_after:'q1'}};
  }}});
vm.runInContext(fragment('async function fetchBriptaSuspenseRows(','function toggleSuspenseSelection('),context);
await context.renderSuspense();
assert.deepEqual(errors,[]);assert.equal(context.visibleSuspenseEntries.length,0);
assert.ok(main.innerHTML.includes('KES 400'));assert.ok(main.innerHTML.includes('View only'));
assert.ok(!main.innerHTML.includes('onclick="openSuspenseMatchModal'));
console.log('PASS: Suspense pages load, duplicate receipts count once and officers retain view-only controls.');

context.canManageSuspense=()=>true;
context.canManageSuspenseEntry=row=>row.branch_id==='branch';
context.updateSuspenseBulkControls=()=>{};
await context.renderSuspense();
assert.equal(context.visibleSuspenseEntries.length,2);
assert.ok(main.innerHTML.includes('Migori'));
assert.ok(main.innerHTML.includes('Mbita'));
assert.equal((main.innerHTML.match(/onclick="openSuspenseMatchModal/g)||[]).length,2);
assert.equal((main.innerHTML.match(/View only/g)||[]).length,1);
console.log('PASS: Shared Suspense shows both branches while branch staff can manage only their own entries.');

const calls=[];let fail=true;
const bucket={upload:async(path)=>{calls.push(['upload',path]);return {};},remove:async(paths)=>{calls.push(['remove',...paths]);return {};}};
// Before the bripta-documents function is deployed the gateway answers 404 (non-JSON): legacy path is used.
const serviceCalls=[];let serviceDeployed=false;
Object.assign(context,{currentUser:{business_id:'BIZ-B3F5E5D9'},crypto:{subtle:{digest:async()=>new Uint8Array(32).buffer}},console,
  SUPABASE_URL:'https://example.invalid',SUPABASE_KEY:'anon',FormData,File,Blob,
  fetch:async(url,init)=>{serviceCalls.push({url,init});return serviceDeployed
    ?new Response(JSON.stringify({ok:true,status:'saved'}),{status:200,headers:{'content-type':'application/json'}})
    :new Response('Not found',{status:404,headers:{'content-type':'text/plain'}});},
  supabaseClient:{storage:{from:()=>bucket},auth:{getSession:async()=>({data:{session:{access_token:'user-jwt'}}})}},sb:()=>({select(){return this;},eq(){return this;},
    maybeSingle:async()=>({data:{object_path:'old-file'}}),upsert:async()=>({error:fail?{message:'Save interrupted'}:null})})});
vm.runInContext(readFileSync(new URL('../bripta-client-documents.js',import.meta.url),'utf8'),context);
assert.equal((context.clientDocumentInputs().match(/type="file"/g)||[]).length,6);
assert.throws(()=>context.validateClientDocument('client_passport',{type:'application/pdf',size:10}),/passport/);
assert.throws(()=>context.validateClientDocument('client_id_front',{type:'image/jpeg',size:9*1024*1024}),/8 MB/);
let bitmapClosed=false;
context.createImageBitmap=async()=>({width:3000,height:2000,close:()=>{bitmapClosed=true;}});
context.document.createElement=()=>({width:0,height:0,getContext:()=>({fillRect(){},drawImage(){},set fillStyle(_value){}}),
  toBlob:callback=>callback({size:900000})});
const compressed=await context.prepareClientDocument('client_id_front',{name:'front.jpg',type:'image/jpeg',size:3000000});
assert.equal(compressed.blob.size,900000);assert.equal(compressed.mimeType,'image/jpeg');assert.equal(bitmapClosed,true);
const files=[{kind:'client_id_front',file:{name:'front.pdf',type:'application/pdf',size:10,arrayBuffer:async()=>new Uint8Array(10).buffer}}];
assert.equal((await context.saveClientDocumentFiles('client',files)).length,1);
assert.ok(!calls.some(c=>c[0]==='remove'&&c[1]==='old-file'),'failed replacement preserves old file');
assert.equal(vm.runInContext("pendingClientDocumentUploads.get('client').length",context),1);
fail=false;
assert.equal((await context.saveClientDocumentFiles('client',files)).length,0);
assert.ok(calls.some(c=>c[0]==='remove'&&c[1]==='old-file'));
assert.equal(vm.runInContext('pendingClientDocumentUploads.size',context),0);
console.log('PASS: six upload slots, image compression below 1 MB, failed-upload cleanup and retry without recreating clients.');

// Once deployed, uploads go to the server function (Google Cloud) and never touch Supabase Storage.
serviceDeployed=true;calls.length=0;serviceCalls.length=0;
assert.equal((await context.saveClientDocumentFiles('client',files)).length,0);
assert.equal(calls.length,0);
assert.equal(serviceCalls.length,1);
const form=serviceCalls[0].init.body;
assert.equal(serviceCalls[0].init.headers.Authorization,'Bearer user-jwt');
assert.equal(form.get('kind'),'client_id_front');assert.equal(form.get('expected_previous'),'old-file');
assert.ok(serviceCalls[0].url.endsWith('/functions/v1/bripta-documents'));
console.log('PASS: with the document service deployed, uploads go to Google Cloud through the server function.');
