-- Read-only progress and integrity check after each cloud compression batch.
SELECT 'compression_audit' AS check_name,
  count(*)::bigint AS files,
  coalesce(sum(original_bytes-compressed_bytes),0)::bigint AS value_bytes,
  count(*) FILTER (WHERE old_deleted_at IS NULL)::bigint AS old_copies_pending_removal
FROM public.bripta_document_compression_audit
UNION ALL
SELECT 'current_storage',count(*)::bigint,
  coalesce(sum((metadata->>'size')::bigint),0)::bigint,0
FROM storage.objects
WHERE bucket_id IN ('bripta-client-documents','client-photos')
  AND metadata->>'size' ~ '^[0-9]+$'
UNION ALL
SELECT 'broken_document_links',count(*)::bigint,0,0
FROM public.bripta_client_documents d
LEFT JOIN storage.objects o
  ON o.bucket_id='bripta-client-documents' AND o.name=d.object_path
WHERE d.business_id='BIZ-B3F5E5D9' AND o.id IS NULL;
