-- Bripta-only read access for the cloud compression review. No files are changed.
BEGIN;
CREATE OR REPLACE FUNCTION public.bripta_document_compression_candidates(
  p_object_id uuid DEFAULT NULL,
  p_limit integer DEFAULT 50
)
RETURNS TABLE(object_id uuid, client_id uuid, kind text, object_path text, original_bytes bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
BEGIN
  IF auth.uid() IS NULL OR NOT EXISTS (
    SELECT 1 FROM public.loan_staff s
    WHERE s.business_id='BIZ-B3F5E5D9'
      AND s.auth_user_id=auth.uid()
      AND coalesce(s.is_active,true)
      AND 'admin'=ANY(regexp_split_to_array(lower(trim(coalesce(s.role,''))),'\s*,\s*'))
  ) THEN
    RAISE EXCEPTION 'An active Bripta admin is required' USING ERRCODE='42501';
  END IF;
  RETURN QUERY
  SELECT o.id, d.client_id, d.kind, d.object_path,
    (o.metadata->>'size')::bigint
  FROM storage.objects o
  JOIN public.bripta_client_documents d
    ON d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  WHERE o.bucket_id='bripta-client-documents'
    AND lower(o.metadata->>'mimetype')='image/jpeg'
    AND o.metadata->>'size' ~ '^[0-9]+$'
    AND (o.metadata->>'size')::bigint>1000000
    AND (p_object_id IS NULL OR o.id=p_object_id)
  ORDER BY (o.metadata->>'size')::bigint DESC,o.id
  LIMIT LEAST(GREATEST(coalesce(p_limit,50),1),50);
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_document_compression_candidates(uuid,integer) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.bripta_document_compression_candidates(uuid,integer) TO authenticated;
COMMIT;
