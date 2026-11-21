-- Read only. Run in Bripta's Supabase SQL Editor.
-- Shows how much the largest linked JPEGs could save in batches of 50.
-- This is an estimate: actual output size and legibility must be checked.
WITH ranked AS (
  SELECT o.id AS object_id, d.kind,
    (o.metadata->>'size')::bigint AS original_bytes,
    greatest((o.metadata->>'size')::bigint-1500000,0) AS possible_saving_bytes,
    row_number() OVER (ORDER BY (o.metadata->>'size')::bigint DESC,o.id) AS rank_no
  FROM storage.objects o
  JOIN public.bripta_client_documents d
    ON d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  WHERE o.bucket_id='bripta-client-documents'
    AND lower(o.metadata->>'mimetype')='image/jpeg'
    AND o.metadata->>'size' ~ '^[0-9]+$'
    AND (o.metadata->>'size')::bigint>1500000
), batches AS (
  SELECT ((rank_no-1)/50)+1 AS batch_no,
    count(*) AS linked_images,
    min(original_bytes) AS smallest_original_bytes,
    max(original_bytes) AS largest_original_bytes,
    sum(original_bytes) AS original_bytes,
    sum(possible_saving_bytes) AS possible_saving_bytes
  FROM ranked
  GROUP BY 1
)
SELECT *,
  sum(possible_saving_bytes) OVER (ORDER BY batch_no) AS cumulative_possible_saving_bytes,
  1157947336-sum(possible_saving_bytes) OVER (ORDER BY batch_no) AS estimated_project_bytes_after_batch
FROM batches
ORDER BY batch_no;
