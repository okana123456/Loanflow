-- READ ONLY. Run after every pilot/migration/cleanup run. Counts and ledger ids only.
with objs as (
  select o.bucket_id,o.name,(o.metadata->>'size')::bigint as bytes
  from storage.objects o where o.metadata->>'size' ~ '^[0-9]+$'
), docs as (
  select d.* from public.bripta_client_documents d where d.business_id='BIZ-B3F5E5D9'
)
select 'documents' as section,jsonb_build_object(
    'total',(select count(*) from docs),
    'on_supabase',(select count(*) from docs where storage_provider='supabase'),
    'on_b2_migrated',(select count(*) from docs where storage_provider='b2' and migrated_at is not null),
    'on_b2_new_uploads',(select count(*) from docs where storage_provider='b2' and migrated_at is null),
    'b2_expected_bytes',(select coalesce(sum(file_size),0) from docs where storage_provider='b2'),
    'uploads_last_7_days_supabase',(select count(*) from docs where storage_provider='supabase' and uploaded_at>now()-interval '7 days'),
    'uploads_last_7_days_b2',(select count(*) from docs where storage_provider='b2' and migrated_at is null and uploaded_at>now()-interval '7 days')
  ) as details
union all
select 'broken links',jsonb_build_object(
    'supabase_rows_missing_object',(select count(*) from docs d where d.storage_provider='supabase'
       and not exists(select 1 from objs o where o.bucket_id='bripta-client-documents' and o.name=d.object_path)),
    'known_preexisting_broken_still_broken',(select count(*) from docs d where d.client_id='583310a7-3834-4b6e-8c99-a9d2d993f681'
       and d.kind='client_id_front' and d.storage_provider='supabase'
       and not exists(select 1 from objs o where o.bucket_id='bripta-client-documents' and o.name=d.object_path)),
    'b2_rows_without_verified_ledger',(select count(*) from docs d where d.storage_provider='b2' and d.migrated_at is not null
       and not exists(select 1 from public.bripta_b2_document_migration m where m.ext_object_key=d.ext_object_key and m.state='verified')))
union all
select 'ledger',(select coalesce(jsonb_object_agg(state,n),'{}') from (
  select state,count(*) n from public.bripta_b2_document_migration group by state) x)
union all
select 'failures',(select coalesce(jsonb_agg(jsonb_build_object('ledger_id',id,'kind',kind,'attempts',attempts,'error',last_error) order by id),'[]')
  from public.bripta_b2_document_migration where state in ('failed','skipped'))
union all
select 'supabase storage',jsonb_build_object(
    'total_bytes',(select coalesce(sum(bytes),0) from objs),
    'total_mib',round((select coalesce(sum(bytes),0) from objs)/1048576.0,1),
    'below_150_mib',(select coalesce(sum(bytes),0) from objs)<150*1048576,
    'by_bucket',(select jsonb_object_agg(bucket_id,round(b/1048576.0,1)) from (select bucket_id,sum(bytes) b from objs group by 1) x))
union all
select 'object queue',(select coalesce(jsonb_object_agg(reason||case when p then ':pending' else ':done' end,n),'{}')
  from (select reason,deleted_at is null as p,count(*) n from public.bripta_document_object_queue group by 1,2) x)
union all
select 'cleanup approvals',(select coalesce(jsonb_object_agg(category||':'||st,n),'{}') from (
  select category,case when deleted_at is not null then 'deleted' when skipped_reason is not null then 'skipped' else 'approved' end st,count(*) n
  from public.bripta_document_cleanup_approval group by 1,2) x);
