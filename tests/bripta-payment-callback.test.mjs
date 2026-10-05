// Exercise the real Edge Function with a simulated Supabase client/provider.
// No network requests, payments or SMS are sent.
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source=readFileSync(new URL('../supabase/functions/payment-callback/index.ts',import.meta.url),'utf8')
  .replace(/^import .*;\r?\n/gm,'');
const js=stripTypeScriptTypes(source);
const biz='BIZ-B3F5E5D9';

async function run(options={}) {
  const business=options.business||biz;
  const calls=[],sms=[],errors=[];
  const loan={id:'loan',loan_no:'123',client_id:'client',business_id:business,
    outstanding_balance:1000,total_paid:0,total_payable:1000,total_interest:100,status:'active'};
  class Query {
    constructor(table){this.table=table;this.action='read';this.filters=[];}
    select(){return this;}
    eq(...v){this.filters.push(['eq',...v]);return this;}
    or(...v){this.filters.push(['or',...v]);return this;}
    in(...v){this.filters.push(['in',...v]);return this;}
    gt(){return this;}lt(){return this;}neq(){return this;}order(){return this;}limit(){return this;}
    insert(value){this.action='insert';this.value=value;return this;}
    update(value){this.action='update';this.value=value;return this;}
    async execute(){
      calls.push({table:this.table,action:this.action,value:this.value,filters:this.filters});
      if(this.action==='insert')return {data:{id:this.table==='loan_repayments'?'repayment':'queue'},error:null};
      if(this.action==='update')return {data:null,error:null};
      if(this.table==='loan_settings')return {data:[{business_id:business,mpesa_auto_confirm:true,mpesa_shortcode:'4044341'}],error:null};
      if(this.table==='loan_repayments')return {data:options.existingRepayment?{id:'repayment',loan_id:'loan',business_id:business}:null,error:null};
      if(this.table==='mpesa_callback_queue')return {data:options.previousQueue||null,error:null};
      if(this.table==='loans')return {data:options.noActiveLoan?null:loan,error:null};
      if(this.table==='loan_schedules')return {data:[],error:null};
      if(this.table==='loan_clients')return {data:[{id:'client',business_id:business,full_name:'Test Client'}],error:null};
      throw new Error('Unexpected table '+this.table);
    }
    maybeSingle(){return this.execute();}single(){return this.execute();}
    then(resolve,reject){return this.execute().then(resolve,reject);}
  }
  const client={from:table=>new Query(table),rpc:async(name,args)=>{
    calls.push({rpc:name,args});
    assert.equal(name,'bripta_callback_phone_candidates');
    if(options.lookupError)return {data:null,error:{message:'temporary lookup failure'}};
    return {data:options.candidates??[{id:'client',business_id:business,full_name:'Test Client'}],error:null};
  }};
  let handler;
  const context={serve:fn=>{handler=fn;},createClient:()=>client,Response,Request,AbortSignal,Date,Map,Set,
    Deno:{env:{get:key=>key==='SUPABASE_URL'?'https://test.invalid':'test-key'}},
    console:{error:(...args)=>errors.push(args)},fetch:async(url,request)=>{
      sms.push({url,body:JSON.parse(request.body)});return new Response('{}',{status:200});
    }};
  vm.runInNewContext(js,context);
  const response=await handler(new Request('https://test.invalid/callback',{method:'POST',body:JSON.stringify({
    TransID:options.ref||'TEST-REF',BusinessShortCode:'4044341',BillRefNumber:options.account||'0729477489',
    TransAmount:'150',TransTime:'20261001223822',MSISDN:'hashed-sender',FirstName:'Test'
  })}));
  assert.equal(response.status,200);
  return {calls,sms,errors};
}

let result=await run();
assert.equal(result.errors.length,0);
assert.equal(result.calls.filter(x=>x.table==='loan_clients').length,0);
const payment=result.calls.find(x=>x.table==='loan_repayments'&&x.action==='insert').value;
assert.equal(payment.loan_id,'loan');assert.equal(payment.amount,150);
assert.equal(payment.payment_date,'2026-10-01T22:38:22+03:00');
assert.match(payment.notes,/borrower phone account/);
assert.equal(result.sms.length,1);assert.equal(result.sms[0].body.repayment_id,'repayment');
console.log('PASS: Bripta resolves the account phone, retains Kenya transaction time and requests its repayment SMS.');

for(const [name,options] of [
  ['held payment',{ref:'UJ2558DDTO',candidates:[],previousQueue:{id:'saved-queue',confirmed:false}}],
  ['ambiguous client phone',{candidates:[{id:'one',business_id:biz},{id:'two',business_id:biz}]}],
  ['short account reference',{account:'50'}],
  ['unknown account phone',{candidates:[]}],
  ['client without active loan',{noActiveLoan:true}],
  ['temporary lookup error',{lookupError:true}],
]){
  result=await run(options);
  assert.equal(result.calls.filter(x=>x.table==='loan_repayments'&&x.action==='insert').length,0,name);
  assert.equal(result.calls.filter(x=>x.table==='unmatched_payments'&&x.action==='insert').length,0,name);
  assert.equal(result.sms.length,0,name);
  assert.equal(result.calls.filter(x=>x.table==='mpesa_callback_queue'&&x.action==='insert').length,options.previousQueue?0:1,name);
  if(!options.previousQueue)assert.equal(result.calls.find(x=>x.table==='mpesa_callback_queue'&&x.action==='insert').value.dismissed,false,name);
  console.log(`PASS: ${name} stays queued without duplicate suspense money or SMS.`);
}
result=await run({existingRepayment:true});
assert.equal(result.calls.filter(x=>x.action==='insert').length,0);assert.equal(result.sms.length,0);
console.log('PASS: an already recorded transaction produces no second repayment or SMS request.');

result=await run({business:'ANOTHER_BUSINESS',account:'12345678'});
assert.equal(result.calls.filter(x=>x.rpc).length,0);
assert.ok(result.calls.find(x=>x.table==='loan_clients').filters.some(f=>f[1]==='id_number'&&f[2]==='12345678'));
assert.match(result.calls.find(x=>x.table==='loan_repayments'&&x.action==='insert').value.notes,/National ID account/);
console.log('PASS: other businesses retain their existing matching rule.');
