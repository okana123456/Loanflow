-- READ ONLY. The exact Supabase object manifest to review before any bulk removal.
-- Part 1: one summary row per category with the digest that approval must quote.
select category,count(*) as objects,sum(size_bytes) as bytes,round(sum(size_bytes)/1048576.0,1) as mib,
  public.bripta_document_manifest_digest(category) as digest_to_approve
from public.bripta_document_cleanup_manifest group by category order by category;

-- Part 2: every object (object id, document kind, size, upload time). Export as CSV for the record.
select m.category,m.object_id,split_part(m.object_path,'/',3) as kind,m.size_bytes,m.created_at,
  m.ledger_id,g.state as ledger_state,g.verified_at
from public.bripta_document_cleanup_manifest m
left join public.bripta_gcs_document_migration g on g.id=m.ledger_id
order by m.category,m.created_at;

-- APPROVAL (owner only, after review). Approves exactly the objects listed under that digest;
-- it fails if anything changed since the manifest was generated. Example:
--   select public.bripta_document_approve_cleanup('migrated_original','<digest_to_approve>');
-- 'still_linked_original' can never be approved. 'previously_unlinked' (the 14 reviewed files),
-- 'optimizer_staged' (1 reduced copy from the failed optimizer) and 'uncertain' are approved separately, if at all.
