-- ROLLBACK for bripta-gcs-documents-20261122.sql. Run in Bripta's Supabase SQL Editor only.
-- Step A (safe at any time before the approved Supabase cleanup):
--   points every migrated document back to its Supabase original, which still exists.
-- Documents uploaded straight to Google Cloud after the cutover have no Supabase original;
-- they are listed (counts only) and stay readable only through the bripta-documents Edge Function.
-- Keep that function deployed until they are copied back or re-uploaded.
begin;
-- Act as the trusted server path so the validation trigger accepts the switch.
select set_config('request.jwt.claims','{"role":"service_role"}',true);

with reverted as (
  update public.bripta_client_documents d
  set storage_provider='supabase',gcs_bucket=null,gcs_object_key=null,gcs_generation=null,sha256=null,migrated_at=null
  where d.business_id='BIZ-B3F5E5D9' and d.storage_provider='gcs' and d.object_path is not null
    and exists(select 1 from storage.objects o where o.bucket_id='bripta-client-documents' and o.name=d.object_path)
  returning d.client_id,d.kind
)
update public.bripta_gcs_document_migration m set state='rolled_back',updated_at=now()
from reverted r where m.client_id=r.client_id and m.kind=r.kind and m.state in ('switched','verified');

-- Stop any further migration or cleanup runs from acting.
update public.bripta_gcs_document_migration set state='rolled_back',updated_at=now()
where state in ('claimed','copied','failed');
update public.bripta_document_cleanup_approval set skipped_reason='rolled_back'
where deleted_at is null and skipped_reason is null;
commit;

select
  count(*) filter (where storage_provider='supabase') as documents_on_supabase,
  count(*) filter (where storage_provider='gcs' and object_path is null) as gcs_only_documents_needing_edge_function,
  count(*) filter (where storage_provider='gcs' and object_path is not null) as gcs_documents_whose_original_is_gone
from public.bripta_client_documents where business_id='BIZ-B3F5E5D9';

-- Step B (optional, only when the query above shows zero GCS documents): restore the
-- 2026-10-05 trigger by re-running bripta-client-documents-20261005.sql, then remove the
-- additive objects. Do NOT run step B while any storage_provider='gcs' row exists.
-- begin;
-- drop view if exists public.bripta_document_cleanup_manifest;
-- drop function if exists public.bripta_document_approve_cleanup(text,text), public.bripta_document_manifest_digest(text),
--   public.bripta_document_cleanup_next(integer), public.bripta_document_cleanup_mark(uuid,text),
--   public.bripta_gcs_migration_claim(integer,text), public.bripta_gcs_migration_mark_copied(bigint,text,bigint,bigint),
--   public.bripta_gcs_migration_switch(bigint), public.bripta_gcs_migration_record(bigint,text,text),
--   public.bripta_gcs_migration_target(bigint), public.bripta_gcs_migration_rollback(bigint),
--   public.bripta_delete_client_with_documents(uuid,text,uuid), public.bripta_document_queue_done(bigint),
--   public.bripta_document_queue_object(text,text,text,bigint,uuid,text,text),
--   public.bripta_document_gcs_commit(uuid,text,uuid,text,text,text,text,bigint,text,bigint,text,text),
--   public.bripta_document_lookup(uuid,text,uuid,text), public.bripta_document_actor(uuid,text),
--   public.bripta_document_access_as(uuid,text,uuid), public.bripta_require_service_role();
-- alter table public.bripta_client_documents drop constraint if exists bripta_client_documents_storage_ck;
-- drop index if exists public.bripta_client_documents_gcs_key_uq;
-- alter table public.bripta_client_documents alter column object_path set not null,
--   drop column if exists storage_provider, drop column if exists gcs_bucket, drop column if exists gcs_object_key,
--   drop column if exists gcs_generation, drop column if exists sha256, drop column if exists migrated_at;
-- (Keep the ledger, queue, approval and baseline tables as the audit record.)
-- notify pgrst,'reload schema';
-- commit;
