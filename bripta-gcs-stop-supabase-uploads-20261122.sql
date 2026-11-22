-- Phase 3: run ONLY after the bripta-documents function and the new website code are live and a
-- staff upload has been confirmed on Google Cloud (bripta-gcs-status: uploads_last_7_days_gcs > 0).
-- Browsers can no longer add or overwrite files in the Supabase document bucket. Staff reads of
-- not-yet-migrated originals keep working; the service role (Storage API cleanup) is unaffected.
-- Rollback: re-run bripta-client-documents-20261005.sql (restores the original boundary policy).
begin;
drop policy if exists bripta_documents_storage_insert on storage.objects;
drop policy if exists bripta_documents_storage_boundary on storage.objects;
create policy bripta_documents_storage_boundary on storage.objects as restrictive for all to public
using(bucket_id<>'bripta-client-documents' or public.bripta_can_access_document_path(name))
with check(bucket_id<>'bripta-client-documents');
commit;
select polname,pg_get_expr(polwithcheck,polrelid) as with_check from pg_policy
where polrelid='storage.objects'::regclass and polname like 'bripta_documents_storage%';
