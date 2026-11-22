-- Run once in Bripta's Supabase SQL Editor before resuming the B2 migration.
-- This only updates the service-role claim function. It does not move or delete files.
create or replace function public.bripta_b2_migration_claim_next(
  p_limit integer, p_bucket text, p_exclude_ids bigint[])
returns table(id bigint,client_id uuid,kind text,supabase_path text,expected_bytes bigint,ext_bucket text,
  ext_object_key text,ext_version text,sha256 text,state text,attempts integer)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
begin
  perform public.bripta_require_service_role();
  if coalesce(p_bucket,'')!~'^[a-z0-9][a-z0-9._-]{2,62}$' then raise exception 'Invalid bucket'; end if;
  update public.bripta_b2_document_migration m set state='skipped',last_error='document_changed',updated_at=now()
  where m.state in ('claimed','copied','failed') and not exists(
    select 1 from public.bripta_client_documents d where d.client_id=m.client_id and d.kind=m.kind
      and d.storage_provider='supabase' and d.object_path=m.supabase_path);
  insert into public.bripta_b2_document_migration(client_id,kind,supabase_object_id,supabase_path,expected_bytes,ext_bucket,ext_object_key)
  select d.client_id,d.kind,o.id,o.name,(o.metadata->>'size')::bigint,p_bucket,'bripta/docs/v1/'||gen_random_uuid()::text
  from public.bripta_client_documents d
  join storage.objects o on o.bucket_id='bripta-client-documents' and o.name=d.object_path
  where d.business_id='BIZ-B3F5E5D9' and d.storage_provider='supabase' and o.metadata->>'size' ~ '^[0-9]+$'
  on conflict (supabase_object_id) do nothing;
  return query select m.id,m.client_id,m.kind,m.supabase_path,m.expected_bytes,m.ext_bucket,m.ext_object_key,
    m.ext_version,m.sha256,m.state,m.attempts
  from public.bripta_b2_document_migration m
  where (m.state in ('claimed','copied','switched') or (m.state='failed' and m.attempts<3))
    and not (m.id=any(coalesce(p_exclude_ids,'{}'::bigint[])))
  order by case m.state when 'switched' then 0 when 'copied' then 1 else 2 end,m.id
  limit least(greatest(coalesce(p_limit,5),1),50);
end $$;
revoke all on function public.bripta_b2_migration_claim_next(integer,text,bigint[]) from public,anon,authenticated;
grant execute on function public.bripta_b2_migration_claim_next(integer,text,bigint[]) to service_role;

-- Read-only progress and link integrity summary.
select
  (select count(*) from public.bripta_client_documents where business_id='BIZ-B3F5E5D9' and storage_provider='b2') as linked_on_b2,
  (select count(*) from public.bripta_client_documents where business_id='BIZ-B3F5E5D9' and storage_provider='supabase') as linked_on_supabase,
  (select count(*) from public.bripta_b2_document_migration where state='failed' and attempts>=3) as needs_review,
  (select count(*) from public.bripta_client_documents d where d.business_id='BIZ-B3F5E5D9' and d.storage_provider='supabase'
     and not exists(select 1 from storage.objects o where o.bucket_id='bripta-client-documents' and o.name=d.object_path)) as missing_supabase_link;
