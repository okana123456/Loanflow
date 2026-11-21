-- Read only. No client names, filenames, or private URLs are returned.
WITH broken AS (
  SELECT d.client_id,d.kind,d.object_path,d.uploaded_at,d.file_size
  FROM public.bripta_client_documents d
  LEFT JOIN storage.objects current_object
    ON current_object.bucket_id='bripta-client-documents'
   AND current_object.name=d.object_path
  WHERE d.business_id='BIZ-B3F5E5D9' AND current_object.id IS NULL
)
SELECT b.client_id,b.kind,b.uploaded_at,b.file_size,
  EXISTS(SELECT 1 FROM public.bripta_document_compression_audit a
    WHERE a.client_id=b.client_id AND a.kind=b.kind AND a.new_path=b.object_path)
    AS linked_by_compression,
  EXISTS(SELECT 1 FROM public.bripta_document_compression_audit a
    JOIN storage.objects old_object
      ON old_object.bucket_id='bripta-client-documents' AND old_object.name=a.old_path
    WHERE a.client_id=b.client_id AND a.kind=b.kind AND a.new_path=b.object_path)
    AS compression_original_still_exists,
  (SELECT count(*) FROM storage.objects candidate
    WHERE candidate.bucket_id='bripta-client-documents'
      AND split_part(candidate.name,'/',1)='BIZ-B3F5E5D9'
      AND split_part(candidate.name,'/',2)=b.client_id::text
      AND split_part(candidate.name,'/',3)=b.kind) AS other_files_in_same_document_slot,
  (SELECT jsonb_agg(jsonb_build_object(
      'object_id',candidate.id,
      'size_bytes',candidate.metadata->>'size',
      'created_at',candidate.created_at)
      ORDER BY candidate.created_at DESC)
   FROM storage.objects candidate
   WHERE candidate.bucket_id='bripta-client-documents'
     AND split_part(candidate.name,'/',1)='BIZ-B3F5E5D9'
     AND split_part(candidate.name,'/',2)=b.client_id::text
     AND split_part(candidate.name,'/',3)=b.kind) AS same_slot_candidates
FROM broken b;
