// Runs only on an ephemeral GitHub hosted runner. No document is written to disk.
import {createClient} from '@supabase/supabase-js';
import sharp from 'sharp';
import {createHash} from 'node:crypto';

const project='https://nngscmpsxtqqjzcnsrbi.supabase.co';
const key=process.env.BRIPTA_SUPABASE_SERVICE_ROLE_KEY;
if(!key)throw new Error('GitHub secret BRIPTA_SUPABASE_SERVICE_ROLE_KEY is missing.');
const client=createClient(project,key,{auth:{persistSession:false,autoRefreshToken:false}});
const bucket=client.storage.from('bripta-client-documents');
const limit=Math.min(Math.max(Number(process.env.BRIPTA_MAX_FILES||600),1),600);
const hash=b=>createHash('sha256').update(b).digest('hex');
const fail=(step)=>{throw new Error(`${step} failed; inspect Supabase logs without publishing document details.`);};
function safeDatabaseError(error){
  const code=String(error?.code||'unknown').replace(/[^A-Za-z0-9]/g,'').slice(0,12);
  const message=String(error?.message||'unknown')
    .replace(/eyJ[A-Za-z0-9._-]+/g,'[token]')
    .replace(/BIZ-[A-Za-z0-9_-]+\/[^\s'"),;]+/g,'[document path]')
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,'[id]')
    .slice(0,240);
  return `${code}: ${message}`;
}
let completed=0,skipped=0,freed=0;
const skippedIds=new Set();

async function pendingCleanup(){
  for(;;){
    const {data,error}=await client.rpc('bripta_cloud_optimizer_pending_deletes');
    if(error)fail('Pending cleanup lookup');
    if(!data?.length)return;
    for(const row of data){
      const {error:removeError}=await bucket.remove([row.old_path]);
      if(removeError)fail('Old copy removal');
      const {data:marked,error:markError}=await client.rpc('bripta_cloud_optimizer_mark_deleted',{p_object_id:row.old_object_id});
      if(markError||!marked)fail('Old copy verification');
    }
  }
}

async function optimize(original){
  const image=sharp(original,{failOn:'error',limitInputPixels:50000000});
  const info=await image.metadata();
  if(!info.width||!info.height||Math.min(info.width,info.height)<800)return null;
  // Preserve a useful reading resolution; skip images that need worse settings.
  for(const side of [1800,1600,1400,1200]){
    for(const quality of [74,66,58,50]){
      const result=await sharp(original,{failOn:'error',limitInputPixels:50000000})
        .rotate().resize(side,side,{fit:'inside',withoutEnlargement:true})
        .flatten({background:'#ffffff'}).jpeg({quality,mozjpeg:true}).toBuffer();
      if(result.length<=200000&&result.length>=10000)return result;
    }
  }
  return null;
}

await pendingCleanup();
for(let i=0;i<limit;i++){
  const {data,error}=await client.rpc('bripta_cloud_optimizer_candidates',{p_limit:1,p_exclude:[...skippedIds]});
  if(error)fail('Candidate lookup');
  const item=data?.[0];
  if(!item)break;
  try{
    const {data:blob,error:readError}=await bucket.download(item.object_path);
    if(readError||!blob)fail('Private original download');
    const original=Buffer.from(await blob.arrayBuffer());
    if(original.length!==Number(item.original_bytes))throw new Error('Original size changed; stopped safely.');
    const output=await optimize(original);
    if(!output||output.length>=original.length){skipped++;skippedIds.add(item.object_id);continue;}
    const path=`BIZ-B3F5E5D9/${item.client_id}/${item.kind}/${hash(output)}.jpg`;
    if(path===item.object_path){skipped++;skippedIds.add(item.object_id);continue;}
    const {error:uploadError}=await bucket.upload(path,output,{contentType:'image/jpeg',upsert:false,cacheControl:'60'});
    if(uploadError&&!String(uploadError.message).toLowerCase().includes('already exists'))fail('Optimized upload');
    const {data:checkBlob,error:checkError}=await bucket.download(path);
    if(checkError||!checkBlob)fail('Optimized download check');
    const checked=Buffer.from(await checkBlob.arrayBuffer());
    if(checked.length!==output.length||hash(checked)!==hash(output))throw new Error('Optimized bytes did not verify.');
    const {data:committed,error:commitError}=await client.rpc('bripta_cloud_optimizer_commit',{
      p_object_id:item.object_id,p_old_path:item.object_path,p_new_path:path,
      p_original_bytes:original.length,p_optimized_bytes:output.length
    });
    if(commitError)throw new Error(`Document link switch failed (${safeDatabaseError(commitError)}).`);
    if(!committed)fail('Document link switch');
    await pendingCleanup();
    completed++;freed+=original.length-output.length;
    if(completed%10===0)console.log(`Verified and replaced ${completed} documents; saved ${(freed/1048576).toFixed(1)} MiB this run.`);
  }catch(error){
    console.error(`Stopped at candidate ${i+1}: ${error.message}`);
    throw error;
  }
}
console.log(`Complete: ${completed} verified replacements, ${skipped} needing manual review; saved ${(freed/1048576).toFixed(1)} MiB this run.`);
