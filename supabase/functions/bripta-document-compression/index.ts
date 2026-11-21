// Bripta-only cloud compression, one audited image per invocation.
import { serve } from 'https://deno.land/std@0.224.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';
import { ImageMagick, initializeImageMagick, MagickFormat } from 'npm:@imagemagick/magick-wasm@0.0.37';

const BUCKET='bripta-client-documents';
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'authorization, apikey, content-type, x-client-info','Access-Control-Allow-Methods':'POST, OPTIONS'};
function json(body:Record<string,unknown>,status=200){return new Response(JSON.stringify(body),{status,headers:{...cors,'Content-Type':'application/json','Cache-Control':'no-store'}});}
const wasmBytes=await Deno.readFile(new URL('magick.wasm',import.meta.resolve('npm:@imagemagick/magick-wasm@0.0.37')));
await initializeImageMagick(wasmBytes);

serve(async req=>{
  if(req.method==='OPTIONS')return new Response('ok',{headers:cors});
  if(req.method!=='POST')return json({ok:false,message:'Use POST.'},405);
  const token=/^Bearer (.+)$/i.exec(req.headers.get('authorization')||'')?.[1];
  if(!token)return json({ok:false,message:'Sign in to Bripta first.'},401);
  const url=Deno.env.get('SUPABASE_URL')||'';
  const key=Deno.env.get('SUPABASE_ANON_KEY')||'';
  if(!url||!key)return json({ok:false,message:'Supabase configuration missing.'},500);
  try{
    const body=await req.json();
    const objectId=String(body?.object_id||'');
    if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(objectId))
      return json({ok:false,message:'A reviewed object ID is required.'},400);
    const client=createClient(url,key,{global:{headers:{Authorization:`Bearer ${token}`}},auth:{persistSession:false,autoRefreshToken:false}});
    const {data:{user},error:userError}=await client.auth.getUser(token);
    if(userError||!user)return json({ok:false,message:'Your login expired. Sign in again.'},401);
    const bucket=client.storage.from(BUCKET);
    if(body.action==='cleanup'){
      const {data:oldPath,error:pendingError}=await client.rpc('bripta_pending_old_document_cleanup',{p_object_id:objectId});
      if(pendingError)return json({ok:false,message:'Could not verify pending cleanup.'},403);
      if(!oldPath)return json({ok:true,status:'already_clean'});
      const {error:removeError}=await bucket.remove([oldPath]);
      if(removeError)return json({ok:false,status:'pending_cleanup',message:'The new document is linked, but the old copy still needs removal.'},502);
      const {data:marked,error:markError}=await client.rpc('bripta_mark_old_document_deleted',{p_object_id:objectId});
      if(markError||!marked)return json({ok:false,status:'pending_cleanup',message:'Old copy removal needs an audit check.'},502);
      return json({ok:true,status:'completed'});
    }
    if(body.action==='commit'){
      const stagePath=String(body.stage_path||'');
      const approvalNote=String(body.approval_note||'');
      const {data:committed,error:commitError}=await client.rpc('bripta_commit_document_compression',{
        p_object_id:objectId,p_new_path:stagePath,p_approval_note:approvalNote
      });
      if(commitError||!committed?.length)return json({ok:false,message:commitError?.message||'Document replacement was not confirmed.'},409);
      const oldPath=committed[0].old_path;
      const {error:removeError}=await bucket.remove([oldPath]);
      if(removeError)return json({ok:false,status:'pending_cleanup',message:'The new document is linked, but the old copy still needs removal.'},502);
      const {data:marked,error:markError}=await client.rpc('bripta_mark_old_document_deleted',{p_object_id:objectId});
      if(markError||!marked)return json({ok:false,status:'pending_cleanup',message:'Old copy removal needs an audit check.'},502);
      return json({ok:true,status:'completed',object_id:objectId,
        saved_bytes:Number(committed[0].original_bytes)-Number(committed[0].compressed_bytes)});
    }
    if(body.action!=='stage')return json({ok:false,message:'Unknown compression action.'},400);
    const {data:candidates,error:lookupError}=await client.rpc('bripta_document_compression_candidates',{p_object_id:objectId,p_limit:1,p_batch:1});
    if(lookupError||!candidates?.length)return json({ok:false,message:'The linked document is not in Bripta’s approved manifest.'},403);
    const item=candidates[0];
    const oldPath=String(item.object_path||'');
    if(!oldPath.startsWith(`BIZ-B3F5E5D9/${item.client_id}/${item.kind}/`)||Number(item.original_bytes)>8000000)
      return json({ok:false,message:'The original document path or size is unexpected.'},409);
    const {data:oldBlob,error:downloadError}=await bucket.download(oldPath);
    if(downloadError||!oldBlob)return json({ok:false,message:'Could not read the original document.'},502);
    const original=new Uint8Array(await oldBlob.arrayBuffer());
    if(original.byteLength!==Number(item.original_bytes))return json({ok:false,message:'The original changed; run the audit again.'},409);
    let compressed:Uint8Array|undefined;
    ImageMagick.read(original,img=>{
      for(const [side,quality] of [[1800,72],[1600,64]] as const){
        img.resize(side,side);
        img.quality=quality;
        img.write(MagickFormat.Jpeg,data=>{compressed=Uint8Array.from(data);});
        if(compressed&&compressed.byteLength<=950000)break;
      }
    });
    if(!compressed||compressed.byteLength>950000)
      return json({ok:false,message:'This image could not be compressed below 950 KB safely; the original is unchanged.'},422);
    const digestBytes=new Uint8Array(await crypto.subtle.digest('SHA-256',compressed));
    const digest=Array.from(digestBytes,b=>b.toString(16).padStart(2,'0')).join('');
    const stagePath=`BIZ-B3F5E5D9/${item.client_id}/${item.kind}/${digest}.jpg`;
    if(stagePath===oldPath)return json({ok:false,message:'The image is already stored in optimized form.'},409);
    const {error:uploadError}=await bucket.upload(stagePath,compressed,{contentType:'image/jpeg',upsert:false,cacheControl:'60'});
    if(uploadError){
      const {data:existing}=await bucket.info(stagePath);
      if(!existing||existing.size!==compressed.byteLength)return json({ok:false,message:'Could not stage the compressed copy.'},502);
    }
    const {data:newBlob,error:verifyError}=await bucket.download(stagePath);
    if(verifyError||!newBlob)return json({ok:false,message:'Could not verify the compressed copy; original unchanged.'},502);
    const verifyBytes=new Uint8Array(await newBlob.arrayBuffer());
    const verifyDigest=new Uint8Array(await crypto.subtle.digest('SHA-256',verifyBytes));
    if(verifyBytes.byteLength!==compressed.byteLength||!verifyDigest.every((b,i)=>b===digestBytes[i]))
      return json({ok:false,message:'Compressed copy did not verify; original unchanged.'},502);
    const [{data:oldUrl,error:oldUrlError},{data:newUrl,error:newUrlError}]=await Promise.all([
      bucket.createSignedUrl(oldPath,900),bucket.createSignedUrl(stagePath,900)]);
    if(oldUrlError||newUrlError||!oldUrl||!newUrl)return json({ok:false,message:'Could not create private review links; original unchanged.'},502);
    return json({ok:true,status:'staged',object_id:objectId,rank_no:item.rank_no,kind:item.kind,stage_path:stagePath,original_bytes:original.byteLength,
      compressed_bytes:compressed.byteLength,old_preview_url:oldUrl.signedUrl,new_preview_url:newUrl.signedUrl,
      message:'The original remains linked until an admin approves replacement.'});
  }catch(error){
    console.error('Bripta compression failed',error instanceof Error?error.message:'unknown error');
    return json({ok:false,message:'Compression failed; check the document before retrying.'},500);
  }
});
