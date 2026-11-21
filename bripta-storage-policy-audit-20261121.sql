-- Read only. Run in the Bripta Supabase project.
SELECT
  'bucket' AS section,
  b.id AS name,
  CASE WHEN b.public THEN 'public' ELSE 'private' END AS operation,
  pg_size_pretty(b.file_size_limit) AS detail,
  array_to_string(b.allowed_mime_types, ', ') AS rule
FROM storage.buckets b
WHERE b.id IN ('bripta-client-documents','client-photos')
UNION ALL
SELECT
  'storage policy',
  p.policyname,
  p.cmd,
  array_to_string(p.roles, ', '),
  coalesce(p.qual,'') || ' | CHECK: ' || coalesce(p.with_check,'')
FROM pg_policies p
WHERE p.schemaname='storage' AND p.tablename='objects'
ORDER BY section, name;
