-- Read only. Run in Bripta's Supabase SQL Editor.
-- Exact candidate IDs for the first four batches (200 linked JPEGs).
-- Keep this output private. It contains no customer names or object paths.
WITH ranked AS (
  SELECT o.id AS object_id, d.kind,
    (o.metadata->>'size')::bigint AS original_bytes,
    row_number() OVER (ORDER BY (o.metadata->>'size')::bigint DESC,o.id) AS rank_no
  FROM storage.objects o
  JOIN public.bripta_client_documents d
    ON d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  WHERE o.bucket_id='bripta-client-documents'
    AND lower(o.metadata->>'mimetype')='image/jpeg'
    AND o.metadata->>'size' ~ '^[0-9]+$'
    AND (o.metadata->>'size')::bigint>1000000
)
SELECT rank_no, ((rank_no-1)/50)+1 AS batch_no,
  object_id, kind, original_bytes,
  greatest(original_bytes-950000,0) AS maximum_possible_saving_at_950kb
FROM ranked
WHERE rank_no<=200
ORDER BY rank_no;
