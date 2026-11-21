// Admin-only Bripta cloud compression. No local backup or image download is created.
let briptaStorageStopRequested=false;
let briptaStorageBatchRunning=false;
function briptaStorageAdmin(){return hasRole('admin')&&currentUser?.business_id==='BIZ-B3F5E5D9';}
function briptaStorageStatus(message){const node=document.getElementById('briptaStorageStatus');if(node)node.textContent=message;}
function openBriptaStorageCleanup(){
  if(!briptaStorageAdmin())return;
  briptaStorageStopRequested=false;
  openModal(`<div class="modal-header"><h2>Bripta document storage</h2><button class="modal-close" onclick="closeModal()">×</button></div>
    <p>The audited list has 200 large, linked documents. Each smaller copy is verified and linked before the old copy is removed.</p>
    <p><strong>Approved without an external backup.</strong> Check the first image again, then run one 50-document batch at a time. Keep this page open while a batch runs.</p>
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin:12px 0">
      <button class="btn btn-outline btn-sm" onclick="previewBriptaStoragePilot()">Preview first image</button>
      ${[1,2,3,4].map(n=>`<button class="btn btn-primary btn-sm" onclick="runBriptaStorageBatch(${n})">Compress batch ${n}</button>`).join('')}
      <button class="btn btn-outline btn-sm" onclick="briptaStorageStopRequested=true">Stop after current image</button>
    </div>
    <div id="briptaStorageStatus" role="status">Ready.</div>
    <div id="briptaStorageImages" style="display:none;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-top:16px">
      <div><strong>Original</strong><br><img id="briptaStorageOriginal" alt="Original private document" style="width:100%;max-height:600px;object-fit:contain"></div>
      <div><strong>Compressed</strong><br><img id="briptaStorageCompressed" alt="Compressed private document" style="width:100%;max-height:600px;object-fit:contain"></div>
    </div>`,true);
}
async function briptaStorageCall(body){
  const {data,error}=await supabaseClient.functions.invoke('bripta-document-compression',{body});
  if(error||!data?.ok){
    let message=data?.message||error?.message||'Cloud compression failed';
    try{if(error?.context?.json)message=(await error.context.json()).message||message;}catch(_){/* response unavailable */}
    throw new Error(message);
  }
  return data;
}
async function previewBriptaStoragePilot(){
  if(!briptaStorageAdmin())return;
  briptaStorageStatus('Preparing the first image inside Supabase…');
  try{
    const data=await briptaStorageCall({action:'stage',object_id:'92cf7113-b8ed-487b-b7b7-9fffb9258121'});
    briptaStorageStatus(`${data.kind}: ${(data.original_bytes/1048576).toFixed(2)} MB → ${(data.compressed_bytes/1048576).toFixed(2)} MB. The original is still linked.`);
    const panel=document.getElementById('briptaStorageImages');if(!panel)return;
    for(const [id,url] of [['briptaStorageOriginal',data.old_preview_url],['briptaStorageCompressed',data.new_preview_url]]){
      const img=document.getElementById(id);if(!img)continue;
      img.src=url;img.style.cursor='zoom-in';img.onclick=()=>window.open(url,'_blank','noopener');
    }
    panel.style.display='grid';
  }catch(error){briptaStorageStatus(error.message||'Preview failed; original unchanged.');}
}
async function runBriptaStorageBatch(batchNo){
  if(!briptaStorageAdmin()||briptaStorageBatchRunning||![1,2,3,4].includes(batchNo))return;
  if(!window.confirm(`Approve replacing the 50 audited original documents in Bripta batch ${batchNo} after each smaller copy verifies? No external backup will be kept. Files that fail verification will remain unchanged.`))return;
  briptaStorageBatchRunning=true;briptaStorageStopRequested=false;
  let completed=0,saved=0,skipped=0;
  try{
    const {data:pending,error:pendingError}=await supabaseClient.rpc('bripta_pending_document_cleanup_ids',{p_batch:batchNo});
    if(pendingError)throw pendingError;
    for(const row of pending||[]){
      if(briptaStorageStopRequested)break;
      await briptaStorageCall({action:'cleanup',object_id:row.object_id});
    }
    const {data:rows,error:listError}=await supabaseClient.rpc('bripta_document_compression_candidates',{
      p_object_id:null,p_limit:50,p_batch:batchNo
    });
    if(listError)throw listError;
    for(const row of rows||[]){
      if(briptaStorageStopRequested)break;
      briptaStorageStatus(`Batch ${batchNo}: checking document ${completed+skipped+1} of ${rows.length}. ${completed} replaced, ${skipped} left unchanged.`);
      let staged;
      try{staged=await briptaStorageCall({action:'stage',object_id:row.object_id});}
      catch(error){
        if(/could not be compressed below 950 KB/i.test(error.message)){skipped++;continue;}
        throw new Error(`Stopped at audited rank ${row.rank_no}: ${error.message}`);
      }
      if(staged.compressed_bytes>950000||staged.compressed_bytes>=staged.original_bytes)
        throw new Error(`Stopped at audited rank ${row.rank_no}: compressed size did not verify.`);
      const result=await briptaStorageCall({action:'commit',object_id:row.object_id,
        stage_path:staged.stage_path,
        approval_note:`Bripta admin approved audited batch ${batchNo} without external backup`});
      completed++;saved+=Number(result.saved_bytes||0);
    }
    briptaStorageStatus(`Batch ${batchNo} finished or stopped: ${completed} replaced, ${skipped} left unchanged, ${(saved/1048576).toFixed(1)} MB saved. ${briptaStorageStopRequested?'Stopped by admin.':'Check Storage usage in Supabase.'}`);
  }catch(error){briptaStorageStatus(`Batch ${batchNo} stopped after ${completed} replacements: ${error.message}. Linked documents remain available. Retry this batch to resume.`);}
  finally{briptaStorageBatchRunning=false;}
}
