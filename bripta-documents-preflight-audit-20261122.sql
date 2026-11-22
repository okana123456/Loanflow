-- READ ONLY. Run in Bripta's Supabase SQL Editor BEFORE bripta-gcs-documents-20261122.sql.
-- Returns object IDs, sizes, kinds, counts and function definitions only: no names, phone numbers or URLs.
-- Paste the whole result grid back for review.
with objs as (
  select o.id,o.name,o.created_at,(o.metadata->>'size')::bigint as bytes,
    split_part(o.name,'/',2) as folder_client,split_part(o.name,'/',3) as kind
  from storage.objects o
  where o.bucket_id='bripta-client-documents' and o.metadata->>'size' ~ '^[0-9]+$'
), linked as (
  select o.* from objs o join public.bripta_client_documents d on d.business_id='BIZ-B3F5E5D9' and d.object_path=o.name
), unlinked as (
  select o.* from objs o where not exists(select 1 from public.bripta_client_documents d where d.object_path=o.name)
)
select '1 totals' as section,null::uuid as object_id,null::text as kind,
  jsonb_build_object(
    'linked_files',(select count(*) from linked),'linked_bytes',(select coalesce(sum(bytes),0) from linked),
    'unlinked_files',(select count(*) from unlinked),'unlinked_bytes',(select coalesce(sum(bytes),0) from unlinked),
    'document_rows',(select count(*) from public.bripta_client_documents where business_id='BIZ-B3F5E5D9'),
    'broken_links',(select count(*) from public.bripta_client_documents d where d.business_id='BIZ-B3F5E5D9'
       and not exists(select 1 from objs o where o.name=d.object_path)),
    'all_storage_bytes',(select coalesce(sum((metadata->>'size')::bigint),0) from storage.objects where metadata->>'size' ~ '^[0-9]+$'),
    'by_bucket',(select jsonb_object_agg(bucket_id,jsonb_build_object('files',n,'bytes',b)) from (
       select bucket_id,count(*) n,sum((metadata->>'size')::bigint) b from storage.objects
       where metadata->>'size' ~ '^[0-9]+$' group by bucket_id) x)
  ) as details
union all
-- Failed GitHub optimizer: committed switches, pending old-copy deletions.
select '2 optimizer ledger',a.old_object_id,a.kind,
  jsonb_build_object('original_bytes',a.original_bytes,'optimized_bytes',a.optimized_bytes,'switched_at',a.switched_at,
    'old_deleted_at',a.old_deleted_at,
    'new_copy_exists',exists(select 1 from objs o where o.name=a.new_path),
    'old_copy_exists',exists(select 1 from objs o where o.name=a.old_path),
    'row_points_to_new',exists(select 1 from public.bripta_client_documents d where d.object_path=a.new_path))
from public.bripta_cloud_document_optimization a
union all
-- Likely optimizer staged copies: unlinked SHA-named JPEG, created since 21 Nov, in a slot that has a linked document.
select '3 possible optimizer staged copy',u.id,u.kind,
  jsonb_build_object('bytes',u.bytes,'created_at',u.created_at,
    'slot_linked_original_bytes',(select l.bytes from linked l where l.folder_client=u.folder_client and l.kind=u.kind limit 1),
    'in_optimizer_ledger',exists(select 1 from public.bripta_cloud_document_optimization a where a.new_path=u.name))
from unlinked u
where u.created_at>='2026-11-21' and u.name ~ '/[0-9a-f]{64}[.]jpg$'
  and exists(select 1 from linked l where l.folder_client=u.folder_client and l.kind=u.kind)
union all
-- All unlinked objects (expected: the 14 previously reviewed, plus any staged copy above).
select '4 unlinked object',u.id,u.kind,jsonb_build_object('bytes',u.bytes,'created_at',u.created_at,
  'client_exists',exists(select 1 from public.loan_clients c where c.id::text=u.folder_client and c.business_id='BIZ-B3F5E5D9'))
from unlinked u
union all
-- Earlier admin compression (15 files): any old copy still awaiting removal.
select '5 compression audit pending',a.object_id,a.kind,
  jsonb_build_object('old_copy_exists',exists(select 1 from objs o where o.name=a.old_path),
    'new_linked',exists(select 1 from public.bripta_client_documents d where d.object_path=a.new_path))
from public.bripta_document_compression_audit a where a.old_deleted_at is null
union all
-- The link that was already broken before this migration (tracked separately).
select '6 known broken link',null,d.kind,jsonb_build_object('client_id',d.client_id,'uploaded_at',d.uploaded_at,'file_size',d.file_size,
  'object_exists',exists(select 1 from objs o where o.name=d.object_path),
  'other_files_in_slot',(select count(*) from objs o where o.folder_client=d.client_id::text and o.kind=d.kind and o.name<>d.object_path))
from public.bripta_client_documents d
where d.client_id='583310a7-3834-4b6e-8c99-a9d2d993f681' and d.kind='client_id_front'
union all
-- Live definitions the migration builds on (no data): confirms nothing was hand-edited since 2026-10-05.
select '7 live function',null,p.proname,jsonb_build_object('md5',md5(pg_get_functiondef(p.oid)),'definition',pg_get_functiondef(p.oid))
from pg_proc p join pg_namespace n on n.oid=p.pronamespace
where n.nspname='public' and p.proname in ('bripta_can_access_client_documents','bripta_can_access_document_path',
  'bripta_validate_client_document','bripta_audit_client_document')
union all
select '8 storage policy',null,pol.polname,jsonb_build_object('cmd',pol.polcmd,'permissive',pol.polpermissive,
  'using',pg_get_expr(pol.polqual,pol.polrelid),'check',pg_get_expr(pol.polwithcheck,pol.polrelid))
from pg_policy pol where pol.polrelid='storage.objects'::regclass
  and (pg_get_expr(pol.polqual,pol.polrelid) ilike '%bripta-client-documents%' or pg_get_expr(pol.polwithcheck,pol.polrelid) ilike '%bripta-client-documents%'
    or pg_get_expr(pol.polqual,pol.polrelid) is null or pol.polname ilike '%bripta%')
order by 1,2;
