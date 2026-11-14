import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import vm from 'node:vm';

const source=stripTypeScriptTypes(readFileSync(new URL('../supabase/functions/bripta-create-staff/index.ts',import.meta.url),'utf8')
  .replace(/^import .*;\r?\n/gm,''));
const targetId='11111111-1111-4111-8111-111111111111';

async function run({clients=0,loans=0,applications=0,shared=false,deleteError=null}={}){
  const calls=[];
  let handler;
  const client={
    auth:{
      getUser:async()=>({data:{user:{id:'actor-auth',email:'admin@bripta.test'}},error:null}),
      admin:{deleteUser:async id=>{calls.push(['auth-delete',id]);return{error:deleteError};}},
    },
    from(table){
      const filters={}; let operation='select';
      const q={
        select(){return q;},
        eq(key,value){filters[key]=value;return q;},
        neq(){return q;},
        in(){return q;},
        limit(){return q;},
        update(value){operation='update';calls.push([table,'update',value]);return q;},
        insert(value){operation='insert';calls.push([table,'insert',value]);return q;},
        maybeSingle:async()=>({data:filters.auth_user_id==='actor-auth'
          ? {id:'actor',role:'admin',is_active:true,business_id:'BIZ-B3F5E5D9'}
          : {id:targetId,name:'William Omuya',role:'loan_officer',is_active:true,
             auth_user_id:'william-auth',business_id:'BIZ-B3F5E5D9'},error:null}),
        then(resolve){return Promise.resolve({error:null,
          count:table==='loan_clients'?clients:table==='loans'?loans:table==='loan_applications'?applications:null,
          data:table==='loan_staff'&&filters.auth_user_id==='william-auth'&&operation==='select'?(shared?[{id:'other'}]:[]):null}).then(resolve);},
      };
      return q;
    },
  };
  vm.runInNewContext(source,{
    serve:fn=>{handler=fn;},createClient:()=>client,Response,Request,Set,Date,Promise,
    Deno:{env:{get:name=>name==='SUPABASE_URL'?'https://example.test':'key'}},
    console:{error(){}},
  });
  const response=await handler(new Request('https://example.test/retire',{
    method:'POST',headers:{authorization:'Bearer valid-token'},
    body:JSON.stringify({action:'retire_staff',staff_id:targetId}),
  }));
  return {status:response.status,data:await response.json(),calls};
}

let result=await run();
assert.equal(result.status,200);
assert.equal(result.calls.filter(c=>c[0]==='auth-delete').length,1);
assert.ok(result.calls.some(c=>c[0]==='loan_staff'&&c[1]==='update'&&c[2].is_active===false&&c[2].auth_user_id===null));
assert.ok(result.calls.some(c=>c[0]==='loan_audit_log'&&c[1]==='insert'));

result=await run({clients:1});
assert.equal(result.status,409);
assert.equal(result.calls.length,0);

result=await run({shared:true});
assert.equal(result.status,409);
assert.equal(result.calls.length,0);

result=await run({deleteError:{message:'Auth removal failed'}});
assert.equal(result.status,500);
assert.ok(result.calls.some(c=>c[0]==='loan_staff'&&c[1]==='update'&&c[2].auth_user_id==='william-auth'));
console.log('PASS: staff retirement blocks active portfolios and shared logins, removes Auth access and preserves retry state.');
