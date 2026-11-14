const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(new URL('../index.html',`file://${__filename.replace(/\\/g,'/')}`),'utf8');
const start=html.indexOf('async function checkAuthSession(){');
const end=html.indexOf('\nasync function handleLogout()',start);
assert.ok(start>=0&&end>start);
const values=new Map([['bripta_current_user',JSON.stringify({id:'william',is_active:true})]]);
let signedOut=0,shown=0;
const query={select(){return query;},eq(){return query;},maybeSingle:async()=>({data:null,error:null})};
const context=vm.createContext({
  BRIPTA_BUSINESS_ID:'BIZ-B3F5E5D9',currentUser:null,localStorage:{
    getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key),
  },
  checkSessionTimeout:()=>false,
  supabaseClient:{auth:{getSession:async()=>({data:{session:{user:{id:'auth-william',email:'william@example.test'}}}}),
    signOut:async()=>{signedOut++;}}},
  sb:()=>query,touchSession(){},showApp(){shown++;},loadSettings(){},applyRolloverPenalties(){},
  resolveUserRole:async()=>{throw new Error('Retired user must not reach fallback role resolution');},
});
vm.runInContext(html.slice(start,end),context);
context.checkAuthSession().then(result=>{
  assert.equal(result,false);
  assert.equal(signedOut,1);
  assert.equal(shown,0);
  assert.equal(values.has('bripta_current_user'),false);
  console.log('PASS: a retired staff member cannot reopen Bripta from a cached browser profile.');
}).catch(error=>{console.error(error);process.exitCode=1;});
