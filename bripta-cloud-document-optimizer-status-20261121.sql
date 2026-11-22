-- Read only. Run in Bripta's Supabase SQL Editor after each cloud run.
WITH objects AS (
  SELECT o.bucket_id,o.name,(o.metadata->>'size')::bigint AS bytes
  FROM storage.objects o
  WHERE o.bucket_id IN ('bripta-client-documents','client-photos')
    AND o.metadata->>'size' ~ '^[0-9]+$'
), document_totals AS (
  SELECT count(*) AS files,coalesce(sum(o.bytes),0) AS bytes,
    count(*) FILTER (WHERE o.bytes>200000) AS over_target
  FROM public.bripta_client_documents d
  JOIN objects o ON o.bucket_id='bripta-client-documents' AND o.name=d.object_path
  WHERE d.business_id='BIZ-B3F5E5D9'
), broken AS (
  SELECT count(*) AS files FROM public.bripta_client_documents d
  LEFT JOIN objects o ON o.bucket_id='bripta-client-documents' AND o.name=d.object_path
  WHERE d.business_id='BIZ-B3F5E5D9' AND o.name IS NULL
)
SELECT (SELECT count(*) FROM public.bripta_cloud_document_optimization) AS optimized_documents,
  (SELECT coalesce(sum(original_bytes-optimized_bytes),0) FROM public.bripta_cloud_document_optimization) AS saved_bytes,
  (SELECT count(*) FROM public.bripta_cloud_document_optimization WHERE old_deleted_at IS NULL) AS old_copies_pending_removal,
  (SELECT files FROM document_totals) AS linked_document_files,
  (SELECT over_target FROM document_totals) AS linked_documents_over_200kb,
  (SELECT files FROM broken) AS broken_document_links,
  (SELECT coalesce(sum(bytes),0) FROM objects) AS project_storage_bytes,
  round((SELECT coalesce(sum(bytes),0) FROM objects)/1048576.0,1) AS project_storage_mib;
