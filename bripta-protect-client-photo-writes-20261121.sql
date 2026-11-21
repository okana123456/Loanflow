-- Bripta-only Storage write boundary. Does not delete, move, or rewrite files.
-- Run in the Bripta Supabase project after reviewing the policies below.
BEGIN;

-- Abort without changing policies if any existing Bripta photo uses a path
-- that the access check below would reject after its database link changes.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM storage.objects
    WHERE bucket_id='client-photos'
      AND name LIKE 'BIZ-B3F5E5D9/%'
      AND name !~* '^BIZ-B3F5E5D9/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9a-z]{13}[.]jpg$'
  ) THEN
    RAISE EXCEPTION 'Bripta photo path format requires review; no policy changes made';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.bripta_can_write_client_photo_path(p_path text)
RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
  v_client_id uuid;
  v_filename text;
BEGIN
  IF auth.uid() IS NULL OR p_path IS NULL
     OR split_part(p_path,'/',1)<>'BIZ-B3F5E5D9'
     OR cardinality(string_to_array(p_path,'/'))<>2 THEN
    RETURN false;
  END IF;

  -- Current record lookup supports existing photo names when replacing them.
  SELECT c.id INTO v_client_id
  FROM public.loan_clients c
  WHERE c.business_id='BIZ-B3F5E5D9' AND c.photo_path=p_path
  LIMIT 1;

  IF v_client_id IS NULL THEN
    v_filename:=split_part(p_path,'/',2);
    -- New uploads use client UUID + a 13-character unique suffix.
    IF v_filename !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9a-z]{13}[.]jpg$' THEN
      RETURN false;
    END IF;
    v_client_id:=left(v_filename,36)::uuid;
  END IF;

  RETURN public.bripta_can_access_client_documents(v_client_id);
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_can_write_client_photo_path(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.bripta_can_write_client_photo_path(text) TO anon, authenticated;

DROP POLICY IF EXISTS bripta_client_photos_write_boundary_insert ON storage.objects;
CREATE POLICY bripta_client_photos_write_boundary_insert
ON storage.objects AS RESTRICTIVE FOR INSERT TO PUBLIC
WITH CHECK (
  bucket_id<>'client-photos'
  OR name NOT LIKE 'BIZ-B3F5E5D9/%'
  OR public.bripta_can_write_client_photo_path(name)
);

DROP POLICY IF EXISTS bripta_client_photos_write_boundary_update ON storage.objects;
CREATE POLICY bripta_client_photos_write_boundary_update
ON storage.objects AS RESTRICTIVE FOR UPDATE TO PUBLIC
USING (
  bucket_id<>'client-photos'
  OR name NOT LIKE 'BIZ-B3F5E5D9/%'
  OR public.bripta_can_write_client_photo_path(name)
)
WITH CHECK (
  bucket_id<>'client-photos'
  OR name NOT LIKE 'BIZ-B3F5E5D9/%'
  OR public.bripta_can_write_client_photo_path(name)
);

DROP POLICY IF EXISTS bripta_client_photos_write_boundary_delete ON storage.objects;
CREATE POLICY bripta_client_photos_write_boundary_delete
ON storage.objects AS RESTRICTIVE FOR DELETE TO PUBLIC
USING (
  bucket_id<>'client-photos'
  OR name NOT LIKE 'BIZ-B3F5E5D9/%'
  OR public.bripta_can_write_client_photo_path(name)
);

COMMIT;

SELECT policyname, cmd, permissive
FROM pg_policies
WHERE schemaname='storage' AND tablename='objects'
  AND policyname LIKE 'bripta_client_photos_write_boundary_%'
ORDER BY policyname;
