// Bripta admin review of one cloud-compressed document. No replacement action here.
async function openBriptaStoragePilot(){
  if(!hasRole('admin')||currentUser?.business_id!=='BIZ-B3F5E5D9')return;
  openModal(`<div class="modal-header"><h2>Document storage pilot</h2><button class="modal-close" onclick="closeModal()">×</button></div>
    <p>Preparing one smaller copy inside Bripta's private Storage. The original remains linked to the client.</p>
    <div id="briptaStoragePilotStatus">Preparing comparison…</div>
    <div id="briptaStoragePilotImages" style="display:none;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-top:16px">
      <div><strong>Original</strong><br><img id="briptaStoragePilotOriginal" alt="Original private document" style="width:100%;max-height:600px;object-fit:contain"></div>
      <div><strong>Compressed</strong><br><img id="briptaStoragePilotCompressed" alt="Compressed private document" style="width:100%;max-height:600px;object-fit:contain"></div>
    </div><p style="margin-top:14px">Click each image to inspect the ID text and face at full size. Nothing will be replaced from this pilot.</p>`,true);
  try{
    const {data,error}=await supabaseClient.functions.invoke('bripta-document-compression',{
      body:{object_id:'92cf7113-b8ed-487b-b7b7-9fffb9258121'}
    });
    if(error||!data?.ok)throw new Error(data?.message||'Cloud compression could not be completed');
    const status=document.getElementById('briptaStoragePilotStatus');
    const panel=document.getElementById('briptaStoragePilotImages');
    if(!status||!panel)return;
    status.textContent=`${data.kind}: ${(data.original_bytes/1048576).toFixed(2)} MB → ${(data.compressed_bytes/1048576).toFixed(2)} MB. The original remains linked.`;
    for(const [imageId,url] of [['briptaStoragePilotOriginal',data.old_preview_url],['briptaStoragePilotCompressed',data.new_preview_url]]){
      const img=document.getElementById(imageId);
      if(!img)continue;
      img.src=url;
      img.style.cursor='zoom-in';
      img.onclick=()=>window.open(url,'_blank','noopener');
    }
    panel.style.display='grid';
  }catch(error){
    const status=document.getElementById('briptaStoragePilotStatus');
    if(status)status.textContent=error.message||'Cloud compression could not be completed. The original is unchanged.';
  }
}
