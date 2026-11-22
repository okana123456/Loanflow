-- Run once in Bripta's Supabase SQL Editor. The job never deletes a linked original
-- until a smaller verified copy is stored and its document row is switched.
CREATE TABLE IF NOT EXISTS public.bripta_cloud_document_optimization (
  old_object_id uuid PRIMARY KEY,
  client_id uuid NOT NULL,
  kind text NOT NULL,
  old_path text NOT NULL,
  new_path text NOT NULL,
  original_bytes bigint NOT NULL,
  optimized_bytes bigint NOT NULL,
  switched_at timestamptz NOT NULL DEFAULT now(),
  old_deleted_at timestamptz
);
ALTER TABLE public.bripta_cloud_document_optimization ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.bripta_cloud_document_optimization FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION public.bripta_cloud_optimizer_candidates(p_limit integer DEFAULT 10,p_exclude uuid[] DEFAULT '{}')
RETURNS TABLE(object_id uuid, client_id uuid, kind text, object_path text, original_bytes bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Not authorized' USING ERRCODE='42501'; END IF;
  RETURN QUERY
  SELECT o.id,d.client_id,d.kind,d.object_path,(o.metadata->>'size')::bigint
  FROM public.bripta_client_documents d
  JOIN storage.objects o ON o.bucket_id='bripta-client-documents' AND o.name=d.object_path
  WHERE d.business_id='BIZ-B3F5E5D9'
    AND o.metadata->>'size' ~ '^[0-9]+$'
    AND (o.metadata->>'size')::bigint>200000
    AND lower(o.metadata->>'mimetype') IN ('image/jpeg','image/png','image/webp')
    AND o.id<>ALL(coalesce(p_exclude,'{}'::uuid[]))
    AND NOT EXISTS (SELECT 1 FROM public.bripta_cloud_document_optimization a WHERE a.old_object_id=o.id)
  ORDER BY (o.metadata->>'size')::bigint DESC,o.id
  LIMIT LEAST(GREATEST(coalesce(p_limit,10),1),50);
END $$;
REVOKE ALL ON FUNCTION public.bripta_cloud_optimizer_candidates(integer,uuid[]) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.bripta_cloud_optimizer_candidates(integer,uuid[]) TO service_role;

CREATE OR REPLACE FUNCTION public.bripta_cloud_optimizer_commit(
  p_object_id uuid,p_old_path text,p_new_path text,p_original_bytes bigint,p_optimized_bytes bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_doc public.bripta_client_documents%rowtype; v_old storage.objects%rowtype;
  v_new storage.objects%rowtype; v_count integer;
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Not authorized' USING ERRCODE='42501'; END IF;
  SELECT * INTO v_old FROM storage.objects WHERE id=p_object_id AND bucket_id='bripta-client-documents' AND name=p_old_path;
  SELECT * INTO v_doc FROM public.bripta_client_documents
    WHERE business_id='BIZ-B3F5E5D9' AND object_path=p_old_path FOR UPDATE;
  IF v_old.id IS NULL OR v_doc.client_id IS NULL
    OR (v_old.metadata->>'size')::bigint IS DISTINCT FROM p_original_bytes
    OR p_new_path !~* ('^BIZ-B3F5E5D9/'||v_doc.client_id::text||'/'||v_doc.kind||'/[0-9a-f]{64}[.]jpg$')
    OR p_new_path=p_old_path THEN RAISE EXCEPTION 'Original document changed'; END IF;
  SELECT * INTO v_new FROM storage.objects WHERE bucket_id='bripta-client-documents' AND name=p_new_path;
  IF v_new.id IS NULL OR lower(v_new.metadata->>'mimetype')<>'image/jpeg'
    OR (v_new.metadata->>'size')::bigint IS DISTINCT FROM p_optimized_bytes
    OR p_optimized_bytes<10000 OR p_optimized_bytes>200000 OR p_optimized_bytes>=p_original_bytes
    THEN RAISE EXCEPTION 'Optimized copy failed validation'; END IF;
  UPDATE public.bripta_client_documents SET object_path=p_new_path,file_size=p_optimized_bytes,mime_type='image/jpeg'
    WHERE client_id=v_doc.client_id AND kind=v_doc.kind AND object_path=p_old_path;
  GET DIAGNOSTICS v_count=ROW_COUNT;
  IF v_count<>1 THEN RAISE EXCEPTION 'Document link changed'; END IF;
  INSERT INTO public.bripta_cloud_document_optimization
    (old_object_id,client_id,kind,old_path,new_path,original_bytes,optimized_bytes)
    VALUES(p_object_id,v_doc.client_id,v_doc.kind,p_old_path,p_new_path,p_original_bytes,p_optimized_bytes);
  RETURN true;
END $$;
REVOKE ALL ON FUNCTION public.bripta_cloud_optimizer_commit(uuid,text,text,bigint,bigint) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.bripta_cloud_optimizer_commit(uuid,text,text,bigint,bigint) TO service_role;

CREATE OR REPLACE FUNCTION public.bripta_cloud_optimizer_pending_deletes()
RETURNS TABLE(old_object_id uuid,old_path text,new_path text)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Not authorized' USING ERRCODE='42501'; END IF;
  RETURN QUERY SELECT a.old_object_id,a.old_path,a.new_path
  FROM public.bripta_cloud_document_optimization a
  JOIN public.bripta_client_documents d ON d.client_id=a.client_id AND d.kind=a.kind
    AND d.business_id='BIZ-B3F5E5D9' AND d.object_path=a.new_path
  WHERE a.old_deleted_at IS NULL ORDER BY a.switched_at LIMIT 50;
END $$;
REVOKE ALL ON FUNCTION public.bripta_cloud_optimizer_pending_deletes() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.bripta_cloud_optimizer_pending_deletes() TO service_role;

CREATE OR REPLACE FUNCTION public.bripta_cloud_optimizer_mark_deleted(p_object_id uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_path text;
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Not authorized' USING ERRCODE='42501'; END IF;
  SELECT a.old_path INTO v_path FROM public.bripta_cloud_document_optimization a
  JOIN public.bripta_client_documents d ON d.client_id=a.client_id AND d.kind=a.kind
    AND d.business_id='BIZ-B3F5E5D9' AND d.object_path=a.new_path
  WHERE a.old_object_id=p_object_id AND a.old_deleted_at IS NULL;
  IF v_path IS NULL OR EXISTS(SELECT 1 FROM storage.objects WHERE bucket_id='bripta-client-documents' AND name=v_path) THEN RETURN false; END IF;
  UPDATE public.bripta_cloud_document_optimization SET old_deleted_at=now() WHERE old_object_id=p_object_id;
  RETURN true;
END $$;
REVOKE ALL ON FUNCTION public.bripta_cloud_optimizer_mark_deleted(uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.bripta_cloud_optimizer_mark_deleted(uuid) TO service_role;
