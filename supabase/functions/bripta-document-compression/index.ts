// One-image, Bripta-only cloud compression pilot. The original and its DB link stay intact.
import { serve } from 'https://deno.land/std@0.224.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';
import { ImageMagick, initializeImageMagick, MagickFormat } from 'npm:@imagemagick/magick-wasm@0.0.37';

const PILOT_OBJECT_ID='92cf7113-b8ed-487b-b7b7-9fffb9258121';
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
    if(body?.object_id!==PILOT_OBJECT_ID)return json({ok:false,message:'Only the approved pilot image is available for review.'},400);
    const client=createClient(url,key,{global:{headers:{Authorization:`Bearer ${token}`}},auth:{persistSession:false,autoRefreshToken:false}});
    const {data:{user},error:userError}=await client.auth.getUser(token);
    if(userError||!user)return json({ok:false,message:'Your login expired. Sign in again.'},401);
    const {data:candidates,error:lookupError}=await client.rpc('bripta_document_compression_candidates',{p_object_id:PILOT_OBJECT_ID,p_limit:1});
    if(lookupError||!candidates?.length)return json({ok:false,message:'The linked pilot document could not be verified for this admin.'},403);
    const item=candidates[0];
    const oldPath=String(item.object_path||'');
    if(!oldPath.startsWith(`BIZ-B3F5E5D9/${item.client_id}/${item.kind}/`)||Number(item.original_bytes)>8000000)
      return json({ok:false,message:'The original document path or size is unexpected.'},409);
    const bucket=client.storage.from(BUCKET);
    const {data:oldBlob,error:downloadError}=await bucket.download(oldPath);
    if(downloadError||!oldBlob)return json({ok:false,message:'Could not read the original document.'},502);
    const original=new Uint8Array(await oldBlob.arrayBuffer());
    if(original.byteLength!==Number(item.original_bytes))return json({ok:false,message:'The original changed; run the audit again.'},409);
    let compressed:Uint8Array|undefined;
    ImageMagick.read(original,img=>{
      for(const [side,quality] of [[1800,72],[1600,64],[1400,56]] as const){
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
    return json({ok:true,object_id:PILOT_OBJECT_ID,kind:item.kind,original_bytes:original.byteLength,
      compressed_bytes:compressed.byteLength,old_preview_url:oldUrl.signedUrl,new_preview_url:newUrl.signedUrl,
      message:'Review only. The original remains linked; nothing was replaced or deleted.'});
  }catch(error){
    console.error('Bripta compression pilot failed',error instanceof Error?error.message:'unknown error');
    return json({ok:false,message:'Compression pilot failed; the original remains unchanged.'},500);
  }
});
