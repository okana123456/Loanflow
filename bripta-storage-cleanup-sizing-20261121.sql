-- Read only. Run in Bripta's Supabase SQL Editor.
-- No names, paths, customer details, or file contents are returned.
WITH sized AS (
  SELECT o.id, o.bucket_id, o.name,
    (o.metadata->>'size')::bigint AS bytes,
    lower(coalesce(o.metadata->>'mimetype','unknown')) AS mime_type,
    d.kind,
    d.object_path IS NOT NULL AS linked_document,
    c.photo_path IS NOT NULL AS linked_photo
  FROM storage.objects o
  LEFT JOIN public.bripta_client_documents d
    ON o.bucket_id='bripta-client-documents'
   AND d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  LEFT JOIN public.loan_clients c
    ON o.bucket_id='client-photos'
   AND c.business_id='BIZ-B3F5E5D9' AND c.photo_path=o.name
  WHERE o.bucket_id IN ('bripta-client-documents','client-photos')
    AND o.metadata->>'size' ~ '^[0-9]+$'
), grouped AS (
  SELECT bucket_id,
    CASE WHEN bucket_id='client-photos' THEN 'profile_photo'
         ELSE coalesce(kind,'unlinked_document') END AS category,
    mime_type,
    CASE WHEN linked_document OR linked_photo THEN 'linked'
         ELSE 'unlinked_review' END AS reference_status,
    count(*) AS files,
    sum(bytes) AS total_bytes,
    count(*) FILTER (WHERE bytes>1500000) AS files_over_1_5mb,
    sum(bytes) FILTER (WHERE bytes>1500000) AS bytes_in_files_over_1_5mb,
    sum(greatest(bytes-1500000,0)) AS theoretical_saving_at_1_5mb_each
  FROM sized
  GROUP BY 1,2,3,4
)
SELECT *, pg_size_pretty(total_bytes) AS readable_total,
  pg_size_pretty(coalesce(theoretical_saving_at_1_5mb_each,0)) AS readable_theoretical_saving
FROM grouped
ORDER BY total_bytes DESC;
