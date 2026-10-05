-- Read-only installation/branch checks. Bripta only.
select 'suspense_reader_installed' as check_name,
  (to_regprocedure('public.bripta_suspense_page(text,uuid,uuid,integer)') is not null)::text as result
union all
select 'document_register_installed',(to_regclass('public.bripta_client_documents') is not null)::text
union all
select 'document_bucket_private',coalesce((select (not public)::text from storage.buckets where id='bripta-client-documents'),'MISSING')
union all
select 'pending_callbacks_wrong_branch',count(*)::text
from public.mpesa_callback_queue q
where public.bripta_is_suspense_business(q.business_short_code::text)
  and not coalesce(q.confirmed,false) and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
  and not exists(select 1 from public.bripta_branches b where b.id=q.branch_id and b.business_id='BIZ-B3F5E5D9');
-- Expected: true, true, true, 0. Financial records are not updated.
