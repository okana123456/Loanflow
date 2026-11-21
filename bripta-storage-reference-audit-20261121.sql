-- Read only. Run in the Bripta Supabase project.
-- Returns aggregate sizes and object IDs; no customer names or file paths.
WITH objects AS (
  SELECT o.id, o.bucket_id, o.name, o.created_at,
    CASE WHEN o.metadata->>'size' ~ '^[0-9]+$'
      THEN (o.metadata->>'size')::bigint ELSE 0 END AS size_bytes,
    CASE WHEN o.bucket_id='bripta-client-documents'
      THEN split_part(o.name,'/',3) ELSE 'profile_photo' END AS category
  FROM storage.objects o
  WHERE o.bucket_id IN ('bripta-client-documents','client-photos')
), classified AS (
  SELECT o.*,
    CASE
      WHEN o.bucket_id='bripta-client-documents' AND d.object_path IS NOT NULL
        THEN 'linked_document_record'
      WHEN o.bucket_id='client-photos' AND c.photo_path IS NOT NULL
        THEN 'linked_client_photo'
      WHEN o.bucket_id='bripta-client-documents'
        AND folder_client.id IS NOT NULL
        THEN 'unlinked_but_client_exists_review'
      ELSE 'unlinked_or_legacy_review'
    END AS reference_status
  FROM objects o
  LEFT JOIN public.bripta_client_documents d
    ON o.bucket_id='bripta-client-documents'
    AND d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  LEFT JOIN public.loan_clients c
    ON o.bucket_id='client-photos'
    AND c.business_id='BIZ-B3F5E5D9' AND c.photo_path=o.name
  LEFT JOIN public.loan_clients folder_client
    ON o.bucket_id='bripta-client-documents'
    AND folder_client.business_id='BIZ-B3F5E5D9'
    AND folder_client.id=CASE
      WHEN split_part(o.name,'/',2) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN split_part(o.name,'/',2)::uuid ELSE NULL END
), ranked_review AS (
  SELECT *, row_number() OVER (ORDER BY size_bytes DESC) AS rank_no
  FROM classified
  WHERE reference_status LIKE 'unlinked%'
)
SELECT 'REFERENCE TOTAL' AS section, bucket_id AS bucket, category,
  reference_status, count(*)::bigint AS file_count,
  sum(size_bytes)::bigint AS total_bytes,
  pg_size_pretty(sum(size_bytes)::bigint) AS readable_size,
  '{}'::jsonb AS details
FROM classified
GROUP BY bucket_id, category, reference_status

UNION ALL

SELECT 'LARGEST UNLINKED', bucket_id, category, reference_status,
  1, size_bytes, pg_size_pretty(size_bytes),
  jsonb_build_object('rank',rank_no,'object_id',id,'created_at',created_at)
FROM ranked_review
WHERE rank_no<=20

ORDER BY section, total_bytes DESC;
