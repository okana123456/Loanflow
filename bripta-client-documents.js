// Private onboarding documents. Storage and database policies enforce access.
const BRIPTA_DOCUMENT_BUCKET='bripta-client-documents';
const BRIPTA_DOCUMENT_TYPES=[
  ['client_passport','Client passport photo'],['client_id_front','Client ID — front'],
  ['client_id_back','Client ID — back'],['guarantor_passport','Guarantor passport photo'],
  ['guarantor_id_front','Guarantor ID — front'],['guarantor_id_back','Guarantor ID — back']
];
const pendingClientDocumentUploads=new Map();
function clientDocumentInputs(prefix='onboardDoc'){
  return `<h3 style="margin:16px 0 8px">Client &amp; Guarantor Documents</h3>
    <p style="font-size:12px;color:var(--text-secondary)">Attach passport photos and both sides of each ID. JPG, PNG or WEBP images are reduced below 1 MB before upload; ID copies may also be PDF. Maximum source file: 8 MB.</p>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr));gap:12px">
    ${BRIPTA_DOCUMENT_TYPES.map(([kind,label])=>`<div class="form-group"><label for="${prefix}_${kind}">${label}</label>
      <input class="input" style="max-width:100%" id="${prefix}_${kind}" type="file" accept="image/jpeg,image/png,image/webp${kind.endsWith('passport')?'':',application/pdf'}"></div>`).join('')}</div>`;
}
function validateClientDocument(kind,file){
  if(!BRIPTA_DOCUMENT_TYPES.some(([key])=>key===kind))throw new Error('Unknown document type');
  const extensions={'image/jpeg':'jpg','image/png':'png','image/webp':'webp','application/pdf':'pdf'};
  const ext=extensions[file?.type];
  if(!ext||(kind.endsWith('passport')&&ext==='pdf'))throw new Error('Use JPG, PNG or WEBP for passport photos; ID copies may also be PDF.');
  if(!file.size||file.size>8*1024*1024)throw new Error(`${file.name}: maximum file size is 8 MB.`);
  return ext;
}
function selectedClientDocuments(prefix='onboardDoc'){
  const files=[];
  for(const[kind]of BRIPTA_DOCUMENT_TYPES){
    const file=document.getElementById(`${prefix}_${kind}`)?.files?.[0];
    if(file){validateClientDocument(kind,file);files.push({kind,file});}
  }
  return files;
}
async function prepareClientDocument(kind,file){
  const originalExt=validateClientDocument(kind,file);
  if(originalExt==='pdf'||file.size<=700000){
    return {blob:file,ext:originalExt,mimeType:file.type};
  }
  const bitmap=await createImageBitmap(file);
  try{
    const canvas=document.createElement('canvas');
    const ctx=canvas.getContext('2d');
    if(!ctx)throw new Error('Could not prepare document image');
    for(const maxSide of [2200,1800,1600,1400,1200]){
      const scale=Math.min(1,maxSide/Math.max(bitmap.width,bitmap.height));
      canvas.width=Math.max(1,Math.round(bitmap.width*scale));
      canvas.height=Math.max(1,Math.round(bitmap.height*scale));
      ctx.fillStyle='#fff';
      ctx.fillRect(0,0,canvas.width,canvas.height);
      ctx.drawImage(bitmap,0,0,canvas.width,canvas.height);
      for(const quality of [0.86,0.76,0.66,0.56]){
        const blob=await new Promise(resolve=>canvas.toBlob(resolve,'image/jpeg',quality));
        if(!blob)throw new Error('Could not compress document image');
        if(blob.size<=950000)return {blob,ext:'jpg',mimeType:'image/jpeg'};
      }
    }
    throw new Error('Document image is too large after compression. Choose a clearer, smaller image.');
  }finally{bitmap.close();}
}
async function clientDocumentDigest(blob){
  const bytes=new Uint8Array(await crypto.subtle.digest('SHA-256',await blob.arrayBuffer()));
  return Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');
}
// New uploads, previews and deletions go through the bripta-documents server function
// (Backblaze B2, short-lived signed links). Until it is deployed the legacy
// Supabase Storage path keeps the site working.
const BRIPTA_DOCUMENT_FUNCTION='bripta-documents';
class DocumentServiceMissing extends Error{}
async function callDocumentService(body){
  const{data:{session}}=await supabaseClient.auth.getSession();
  if(!session?.access_token)throw new Error('Sign in again to continue.');
  const isForm=body instanceof FormData;
  let response;
  try{
    response=await fetch(`${SUPABASE_URL}/functions/v1/${BRIPTA_DOCUMENT_FUNCTION}`,{method:'POST',
      headers:{Authorization:`Bearer ${session.access_token}`,apikey:SUPABASE_KEY,...(isForm?{}:{'Content-Type':'application/json'})},
      body:isForm?body:JSON.stringify(body)});
  }catch(_){throw new Error('Document service is unreachable. Check the connection and try again.');}
  if(response.status===404&&!(response.headers.get('content-type')||'').includes('application/json'))throw new DocumentServiceMissing();
  const result=await response.json().catch(()=>({}));
  if(response.status===404&&result?.code==='NOT_FOUND')throw new DocumentServiceMissing();
  if(!response.ok||!result.ok)throw new Error(result.message||`Document service error (${response.status})`);
  return result;
}
async function uploadPrivateClientDocument(clientId,kind,file){
  const prepared=await prepareClientDocument(kind,file);
  const{data:old,error:readError}=await sb('bripta_client_documents').select('*').eq('client_id',clientId).eq('kind',kind).maybeSingle();
  if(readError)throw readError;
  const form=new FormData();
  form.append('client_id',clientId);form.append('kind',kind);
  form.append('expected_previous',old?(old.ext_object_key||old.object_path||''):'');
  form.append('file',new File([prepared.blob],file.name||'document',{type:prepared.mimeType}));
  try{await callDocumentService(form);return;}
  catch(error){if(!(error instanceof DocumentServiceMissing))throw error;}
  return uploadLegacySupabaseDocument(clientId,kind,file,prepared,old);
}
async function uploadLegacySupabaseDocument(clientId,kind,file,prepared,old){
  const digest=await clientDocumentDigest(prepared.blob);
  const path=`${currentUser.business_id}/${clientId}/${kind}/${digest}.${prepared.ext}`;
  if(old?.object_path===path)return;
  const bucket=supabaseClient.storage.from(BRIPTA_DOCUMENT_BUCKET);
  const{error:uploadError}=await bucket.upload(path,prepared.blob,{contentType:prepared.mimeType,upsert:false});
  if(uploadError)throw uploadError;
  const{error:saveError}=await sb('bripta_client_documents').upsert({
    client_id:clientId,kind,business_id:currentUser.business_id,object_path:path,
    file_name:file.name,mime_type:prepared.mimeType,file_size:prepared.blob.size
  },{onConflict:'client_id,kind'});
  if(saveError){await bucket.remove([path]);throw saveError;}
  if(old?.object_path&&old.object_path!==path&&old.storage_provider!=='b2'){
    const{error}=await bucket.remove([old.object_path]);
    if(error)console.warn('Previous document could not be removed',error.message);
  }
}
async function saveClientDocumentFiles(clientId,files){
  const failed=[];
  for(const item of files){
    try{await uploadPrivateClientDocument(clientId,item.kind,item.file);}
    catch(error){failed.push({...item,error:error.message||'Upload failed'});}
  }
  if(failed.length)pendingClientDocumentUploads.set(clientId,failed);
  else pendingClientDocumentUploads.delete(clientId);
  return failed;
}
async function openClientDocuments(clientId){
  try{
    const[{data:docs,error},{data:client,error:clientError}]=await Promise.all([
      sb('bripta_client_documents').select('*').eq('client_id',clientId),
      sb('loan_clients').select('id,full_name,photo_path').eq('id',clientId).single()
    ]);
    if(error||clientError)throw error||clientError;
    const byKind=new Map((docs||[]).map(doc=>[doc.kind,doc]));
    const retry=pendingClientDocumentUploads.get(clientId)||[];
    openModal(`<h2>Documents — ${escapeHtml(client.full_name)}</h2>
      <p style="font-size:12px;color:var(--text-secondary)">Documents are private and available to authorized staff assigned to this client.</p>
      ${retry.length?`<div class="card" style="margin:12px 0"><p>Client saved. ${retry.length} attachment(s) still need uploading.</p>
        ${retry.map(item=>`<p>${escapeHtml(BRIPTA_DOCUMENT_TYPES.find(([key])=>key===item.kind)?.[1])}: ${escapeHtml(item.error)}</p>`).join('')}
        <button class="btn btn-primary btn-sm" onclick="retryClientDocuments('${clientId}',this)">Retry pending uploads</button></div>`:''}
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr));gap:12px">
      ${BRIPTA_DOCUMENT_TYPES.map(([kind,label])=>{const doc=byKind.get(kind);const legacy=kind==='client_passport'&&!doc&&client.photo_path;
        return `<div class="card" style="padding:12px;min-width:0"><strong>${label}</strong>
          <p style="font-size:12px;overflow-wrap:anywhere">${doc?escapeHtml(doc.file_name):legacy?'Existing client photo':'Not yet attached'}</p>
          ${doc?`<button class="btn btn-outline btn-sm" onclick="viewPrivateClientDocument('${clientId}','${kind}')">View / Download</button>`:
            legacy?`<a class="btn btn-outline btn-sm" target="_blank" rel="noopener noreferrer" href="${escapeHtml(clientPhotoUrl(client))}">View existing photo</a>`:''}
          <label style="display:block;margin-top:10px;font-size:12px">${doc?'Replace attachment':'Attach file'}
            <input class="input" style="max-width:100%" type="file" accept="image/jpeg,image/png,image/webp${kind.endsWith('passport')?'':',application/pdf'}"
              onchange="attachClientDocument('${clientId}','${kind}',this)"></label></div>`;
      }).join('')}</div><div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Close</button></div>`,true);
  }catch(error){toast('Could not load documents: '+error.message,'error');}
}
async function attachClientDocument(clientId,kind,input){
  const file=input?.files?.[0];if(!file)return;
  input.disabled=true;
  try{await uploadPrivateClientDocument(clientId,kind,file);toast('Document saved','success');await openClientDocuments(clientId);}
  catch(error){toast('Document upload failed: '+error.message,'error');input.disabled=false;}
}
async function retryClientDocuments(clientId,button){
  button.disabled=true;button.textContent='Uploading…';
  await saveClientDocumentFiles(clientId,pendingClientDocumentUploads.get(clientId)||[]);
  await openClientDocuments(clientId);
}
async function viewPrivateClientDocument(clientId,kind){
  const popup=window.open('about:blank','_blank');if(popup)popup.opener=null;
  try{
    let signedUrl;
    try{signedUrl=(await callDocumentService({action:'sign',client_id:clientId,kind})).url;}
    catch(error){
      if(!(error instanceof DocumentServiceMissing))throw error;
      const{data:doc,error:readError}=await sb('bripta_client_documents').select('object_path,storage_provider').eq('client_id',clientId).eq('kind',kind).single();
      if(readError)throw readError;
      if(doc.storage_provider==='b2'||!doc.object_path)throw new Error('Document service is not available yet.');
      const{data,error:linkError}=await supabaseClient.storage.from(BRIPTA_DOCUMENT_BUCKET).createSignedUrl(doc.object_path,60);
      if(linkError)throw linkError;
      signedUrl=data.signedUrl;
    }
    if(popup)popup.location.replace(signedUrl);
    else openModal(`<h2>Document ready</h2><a class="btn btn-primary" href="${escapeHtml(signedUrl)}" target="_blank" rel="noopener noreferrer">Open document</a><p>This link expires in one minute.</p>`);
  }catch(error){if(popup)popup.close();toast('Could not open document: '+error.message,'error');}
}
