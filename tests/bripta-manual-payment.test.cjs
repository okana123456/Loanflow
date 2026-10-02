const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');

const html=fs.readFileSync(new URL('../index.html',`file://${__filename.replace(/\\/g,'/')}`),'utf8');
const start=html.indexOf('async function savePayment(){');
const end=html.indexOf('\nasync function printReceipt(',start);
assert.ok(start>0&&end>start);
const source=html.slice(start,end);

async function run(existingRepayment,insertError){
  const messages=[];
  let insertCount=0;
  const values={payLoanId:'loan-1',payAmount:'50',payMethod:'mpesa',payRef:'U123',payDate:'2026-10-01',payPenaltyAmt:'0',payNotes:'',paySenderName:'',paySenderPhone:''};
  const loan={id:'loan-1',outstanding_balance:1000,total_payable:1250,total_interest:250};
  const context=vm.createContext({
    $:key=>({value:values[key.slice(1)]}),document:{getElementById:()=>({disabled:false,textContent:''})},
    currentUser:{id:'staff-1',business_id:'BIZ-B3F5E5D9'},today:()=> '2026-10-02',
    toast:(message,kind)=>messages.push({message,kind}),generateUniqueNo:()=> 'RCP-1',
    calculatePaymentAllocation:()=>({loanPortion:50,creditPortion:0,registrationFeePortion:0,processingFeePortion:0}),
    paymentAllocationText:()=> 'loan KES 50',fmtMoney:x=>String(x),
    localDateTime:()=> '2026-10-02T12:00:00',
    sb:table=>{
      const q={select:()=>q,eq:()=>q,limit:async()=>({data:existingRepayment?[{id:'old',receipt_no:'RCP-OLD'}]:[],error:null}),
        single:async()=>({data:loan,error:null}),insert:()=>{insertCount++;return{select:()=>({single:async()=>({data:null,error:insertError})})}}};
      if(table==='loans')return q;
      return q;
    }
  });
  vm.runInContext(source,context);
  await context.savePayment();
  return{messages,insertCount};
}

(async()=>{
  const duplicate=await run(true,null);
  assert.equal(duplicate.insertCount,0,'an existing M-Pesa code must never be inserted again');
  assert.match(duplicate.messages[0].message,/already recorded/);
  const denied=await run(false,{message:'new row violates row-level security policy'});
  assert.equal(denied.insertCount,1);
  assert.equal(denied.messages.at(-1).kind,'error');
  assert.match(denied.messages.at(-1).message,/row-level security/);
  console.log('PASS: duplicate M-Pesa reference blocked; rejected repayment is not reported as successful.');
})().catch(error=>{console.error(error);process.exitCode=1});
