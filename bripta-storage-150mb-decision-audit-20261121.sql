-- Read only. Run in Bripta's Supabase SQL Editor. No file names or customer details returned.
WITH files AS (
  SELECT o.id,o.bucket_id,o.name,(o.metadata->>'size')::bigint AS bytes,
    d.client_id IS NOT NULL AS linked_document,
    c.id IS NOT NULL AS linked_photo
  FROM storage.objects o
  LEFT JOIN public.bripta_client_documents d
    ON o.bucket_id='bripta-client-documents' AND d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  LEFT JOIN public.loan_clients c
    ON o.bucket_id='client-photos' AND c.business_id='BIZ-B3F5E5D9'
    AND c.photo_path=o.name
  WHERE o.bucket_id IN ('bripta-client-documents','client-photos')
    AND o.metadata->>'size' ~ '^[0-9]+$'
), grouped AS (
  SELECT bucket_id,
    CASE WHEN linked_document THEN 'linked_document'
         WHEN linked_photo THEN 'linked_photo'
         ELSE 'unlinked_needs_review' END AS status,
    count(*) AS files,sum(bytes) AS bytes
  FROM files GROUP BY 1,2
)
SELECT bucket_id,status,files,bytes,
  pg_size_pretty(bytes) AS readable_size,
  round(bytes/1048576.0,1) AS mib
FROM grouped ORDER BY bytes DESC;

-- How much a 150 MiB target would require, without deleting anything.
WITH sizes AS (
 SELECT o.bucket_id,(o.metadata->>'size')::bigint AS bytes
 FROM storage.objects o
 WHERE o.bucket_id IN ('bripta-client-documents','client-photos')
   AND o.metadata->>'size' ~ '^[0-9]+$'
)
SELECT count(*) AS all_files,
  sum(bytes) AS current_bytes,
  round(sum(bytes)/1048576.0,1) AS current_mib,
  greatest(sum(bytes)-150*1048576,0) AS bytes_to_remove_for_150_mib,
  round(greatest(sum(bytes)-150*1048576,0)/1048576.0,1) AS mib_to_remove_for_150_mib,
  round((150*1048576.0-sum(bytes) FILTER (WHERE bucket_id='client-photos'))/
    nullif(count(*) FILTER (WHERE bucket_id='bripta-client-documents'),0),0)
    AS average_document_bytes_allowed_if_all_documents_kept
FROM sizes;
