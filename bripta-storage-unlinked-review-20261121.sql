-- Read only. Run in the Bripta Supabase project.
-- Object IDs and aggregate path shapes only; no client names or file paths.
WITH document_objects AS (
  SELECT o.id, o.name, o.created_at,
    (o.metadata->>'size')::bigint AS size_bytes,
    split_part(o.name,'/',3) AS kind,
    CASE WHEN split_part(o.name,'/',2) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN split_part(o.name,'/',2)::uuid ELSE NULL END AS folder_client_id
  FROM storage.objects o
  WHERE o.bucket_id='bripta-client-documents'
    AND o.metadata->>'size' ~ '^[0-9]+$'
), document_review AS (
  SELECT o.*, c.id IS NOT NULL AS client_exists,
    current_doc.object_path IS NOT NULL AS current_slot_has_document,
    current_doc.uploaded_at AS current_slot_uploaded_at
  FROM document_objects o
  LEFT JOIN public.bripta_client_documents exact_ref
    ON exact_ref.business_id='BIZ-B3F5E5D9' AND exact_ref.object_path=o.name
  LEFT JOIN public.loan_clients c
    ON c.id=o.folder_client_id AND c.business_id='BIZ-B3F5E5D9'
  LEFT JOIN public.bripta_client_documents current_doc
    ON current_doc.business_id='BIZ-B3F5E5D9'
    AND current_doc.client_id=o.folder_client_id AND current_doc.kind=o.kind
  WHERE exact_ref.object_path IS NULL
), photo_objects AS (
  SELECT o.id, o.name, o.created_at,
    (o.metadata->>'size')::bigint AS size_bytes
  FROM storage.objects o
  WHERE o.bucket_id='client-photos'
    AND o.metadata->>'size' ~ '^[0-9]+$'
), photo_review AS (
  SELECT o.*, c.id IS NOT NULL AS linked_to_any_client
  FROM photo_objects o
  LEFT JOIN public.loan_clients c ON c.photo_path=o.name
)
SELECT 'UNLINKED DOCUMENT' AS section, id AS object_id, kind AS category,
  size_bytes, created_at,
  jsonb_build_object(
    'client_exists',client_exists,
    'same_document_slot_filled',current_slot_has_document,
    'current_slot_uploaded_after_this_file',current_slot_uploaded_at>created_at
  ) AS details
FROM document_review

UNION ALL

SELECT 'UNLINKED PHOTO', id, 'profile_photo', size_bytes, created_at,
  jsonb_build_object('path_business_prefix',split_part(name,'/',1))
FROM photo_review WHERE NOT linked_to_any_client

UNION ALL

SELECT 'PHOTO PATH SHAPE', NULL::uuid, split_part(name,'/',1),
  sum(size_bytes)::bigint, NULL::timestamptz,
  jsonb_build_object('file_count',count(*),
    'current_bripta_format_count',count(*) FILTER (WHERE name ~* '^BIZ-B3F5E5D9/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9]+[.]jpg$'))
FROM photo_objects
GROUP BY split_part(name,'/',1)

ORDER BY section, size_bytes DESC;
