-- Bripta private documents: Google Cloud Storage support (phase 1, additive and idempotent).
-- Run in the Bripta Supabase project (nngscmpsxtqqjzcnsrbi) SQL Editor only.
-- * Existing Supabase-stored documents keep working unchanged (storage_provider='supabase').
-- * Supabase object paths stay in place as the fallback until an approved cleanup.
-- * Access rules are NOT redefined: server RPCs call the live
--   public.bripta_can_access_client_documents() as the signed-in staff member.
-- Rollback: bripta-gcs-documents-rollback-20261122.sql
begin;

-- 1. Storage provider columns on the active register.
alter table public.bripta_client_documents
  add column if not exists storage_provider text not null default 'supabase',
  add column if not exists gcs_bucket text,
  add column if not exists gcs_object_key text,
  add column if not exists gcs_generation bigint,
  add column if not exists sha256 text,
  add column if not exists migrated_at timestamptz;
alter table public.bripta_client_documents alter column object_path drop not null;
do $$ begin
  if not exists (select 1 from pg_constraint where conname='bripta_client_documents_storage_ck') then
    alter table public.bripta_client_documents add constraint bripta_client_documents_storage_ck check (
      (storage_provider='supabase' and object_path is not null and gcs_object_key is null)
      or (storage_provider='gcs' and gcs_bucket is not null and gcs_generation is not null
          and gcs_object_key ~ '^bripta/docs/v1/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and sha256 ~ '^[0-9a-f]{64}$'));
  end if;
end $$;
create unique index if not exists bripta_client_documents_gcs_key_uq
  on public.bripta_client_documents(gcs_object_key) where gcs_object_key is not null;

-- 2. Baseline of the Supabase bucket taken once, before any migration (for the cleanup manifest).
create table if not exists public.bripta_document_storage_baseline (
  object_id uuid primary key,
  object_path text not null,
  size_bytes bigint not null,
  created_at timestamptz,
  linked_at_baseline boolean not null,
  captured_at timestamptz not null default now()
);
alter table public.bripta_document_storage_baseline enable row level security;
revoke all on public.bripta_document_storage_baseline from public,anon,authenticated;
insert into public.bripta_document_storage_baseline(object_id,object_path,size_bytes,created_at,linked_at_baseline)
select o.id,o.name,(o.metadata->>'size')::bigint,o.created_at,
  exists(select 1 from public.bripta_client_documents d where d.business_id='BIZ-B3F5E5D9' and d.object_path=o.name)
from storage.objects o
where o.bucket_id='bripta-client-documents' and o.metadata->>'size' ~ '^[0-9]+$'
  and not exists (select 1 from public.bripta_document_storage_baseline)
on conflict do nothing;

-- 3. Resumable migration ledger (auditable state machine, one row per Supabase original).
create table if not exists public.bripta_gcs_document_migration (
  id bigserial primary key,
  client_id uuid not null,
  kind text not null,
  supabase_object_id uuid not null unique,
  supabase_path text not null,
  expected_bytes bigint not null,
  gcs_bucket text not null,
  gcs_object_key text not null unique,
  gcs_generation bigint,
  sha256 text,
  state text not null default 'claimed'
    check (state in ('claimed','copied','switched','verified','failed','skipped','rolled_back')),
  attempts integer not null default 0,
  last_error text,
  claimed_at timestamptz not null default now(),
  copied_at timestamptz,
  switched_at timestamptz,
  verified_at timestamptz,
  updated_at timestamptz not null default now()
);
alter table public.bripta_gcs_document_migration enable row level security;
revoke all on public.bripta_gcs_document_migration from public,anon,authenticated;

-- 4. Objects that must not be deleted without a decision (replaced originals, orphans, deletions).
create table if not exists public.bripta_document_object_queue (
  id bigserial primary key,
  provider text not null check (provider in ('supabase','gcs')),
  bucket text not null,
  object_key text not null,
  gcs_generation bigint,
  client_id uuid,
  kind text,
  reason text not null check (reason in ('replaced_by_staff','legacy_replaced_gcs','client_deleted','failed_commit')),
  delete_now boolean not null default false,
  queued_at timestamptz not null default now(),
  deleted_at timestamptz,
  unique (provider,bucket,object_key)
);
alter table public.bripta_document_object_queue enable row level security;
revoke all on public.bripta_document_object_queue from public,anon,authenticated;

-- 5. Cleanup approvals: written only by the project owner in the SQL Editor.
create table if not exists public.bripta_document_cleanup_approval (
  object_id uuid primary key,
  category text not null check (category in ('migrated_original','previously_unlinked','uncertain')),
  object_path text not null,
  size_bytes bigint not null,
  ledger_id bigint,
  manifest_digest text not null,
  approved_at timestamptz not null default now(),
  deleted_at timestamptz,
  skipped_reason text
);
alter table public.bripta_document_cleanup_approval enable row level security;
revoke all on public.bripta_document_cleanup_approval from public,anon,authenticated;

-- 6. Run an access check as a specific signed-in staff member, using the LIVE access function.
create or replace function public.bripta_document_access_as(p_uid uuid,p_email text,p_client uuid)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
declare
  saved_claims text:=current_setting('request.jwt.claims',true);
  saved_sub text:=current_setting('request.jwt.claim.sub',true);
  saved_email text:=current_setting('request.jwt.claim.email',true);
  saved_role text:=current_setting('request.jwt.claim.role',true);
  ok boolean;
begin
  if p_uid is null or p_client is null then return false; end if;
  perform set_config('request.jwt.claims',jsonb_build_object('sub',p_uid::text,'email',coalesce(p_email,''),'role','authenticated')::text,true);
  perform set_config('request.jwt.claim.sub',p_uid::text,true);
  perform set_config('request.jwt.claim.email',coalesce(p_email,''),true);
  perform set_config('request.jwt.claim.role','authenticated',true);
  begin
    ok:=coalesce(public.bripta_can_access_client_documents(p_client),false);
  exception when others then ok:=false;
  end;
  perform set_config('request.jwt.claims',coalesce(saved_claims,''),true);
  perform set_config('request.jwt.claim.sub',coalesce(saved_sub,''),true);
  perform set_config('request.jwt.claim.email',coalesce(saved_email,''),true);
  perform set_config('request.jwt.claim.role',coalesce(saved_role,''),true);
  return ok;
end $$;
revoke all on function public.bripta_document_access_as(uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.bripta_document_access_as(uuid,text,uuid) to service_role;

create or replace function public.bripta_document_actor(p_uid uuid,p_email text)
returns public.loan_staff language sql stable security definer set search_path=pg_catalog as $$
  select s.* from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=p_uid or (nullif(trim(p_email),'') is not null and lower(trim(s.email))=lower(trim(p_email))))
  order by (s.auth_user_id=p_uid) desc nulls last,s.id limit 1
$$;
revoke all on function public.bripta_document_actor(uuid,text) from public,anon,authenticated;
grant execute on function public.bripta_document_actor(uuid,text) to service_role;

create or replace function public.bripta_require_service_role()
returns void language plpgsql stable set search_path=pg_catalog as $$
begin
  if auth.role() is distinct from 'service_role' then raise exception 'Not authorized' using errcode='42501'; end if;
end $$;
revoke all on function public.bripta_require_service_role() from public,anon,authenticated;
grant execute on function public.bripta_require_service_role() to service_role;

-- 7. Validation trigger: staff path unchanged; trusted server path validates shape only.
create or replace function public.bripta_validate_client_document()
returns trigger language plpgsql security definer set search_path=pg_catalog as $$
begin
  if auth.role()='service_role' then
    if new.business_id<>'BIZ-B3F5E5D9' then raise exception 'Wrong business' using errcode='42501'; end if;
    if new.storage_provider='supabase' and (
      split_part(new.object_path,'/',1)<>'BIZ-B3F5E5D9' or split_part(new.object_path,'/',2)<>new.client_id::text
      or split_part(new.object_path,'/',3)<>new.kind
      or not exists(select 1 from storage.objects o where o.bucket_id='bripta-client-documents' and o.name=new.object_path))
      then raise exception 'Supabase document path is invalid' using errcode='42501'; end if;
    if new.kind in ('client_passport','guarantor_passport') and new.mime_type='application/pdf' then
      raise exception 'Passport photo must be a JPG, PNG or WEBP image'; end if;
    if tg_op='INSERT' and new.uploaded_at is null then new.uploaded_at:=now(); end if;
    return new;
  end if;
  -- Staff browser path (legacy Supabase uploads): identical checks to 2026-10-05.
  if not public.bripta_can_access_client_documents(new.client_id)
    or split_part(new.object_path,'/',1)<>'BIZ-B3F5E5D9'
    or split_part(new.object_path,'/',2)<>new.client_id::text
    or split_part(new.object_path,'/',3)<>new.kind
    or not public.bripta_can_access_document_path(new.object_path)
    or not exists(select 1 from storage.objects o where o.bucket_id='bripta-client-documents' and o.name=new.object_path)
    then raise exception 'Upload the document to the authorized client folder first' using errcode='42501'; end if;
  if new.kind in ('client_passport','guarantor_passport') and new.mime_type='application/pdf' then
    raise exception 'Passport photo must be a JPG, PNG or WEBP image'; end if;
  -- A browser write always describes a Supabase object; never let it point at a GCS object.
  if tg_op='UPDATE' and old.storage_provider='gcs' and old.gcs_object_key is not null then
    insert into public.bripta_document_object_queue(provider,bucket,object_key,gcs_generation,client_id,kind,reason)
    values('gcs',old.gcs_bucket,old.gcs_object_key,old.gcs_generation,old.client_id,old.kind,'legacy_replaced_gcs')
    on conflict do nothing;
  end if;
  new.storage_provider:='supabase'; new.gcs_bucket:=null; new.gcs_object_key:=null;
  new.gcs_generation:=null; new.sha256:=null; new.migrated_at:=null;
  select s.id into new.uploaded_by from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  new.uploaded_at:=now();
  return new;
end $$;

create or replace function public.bripta_audit_client_document()
returns trigger language plpgsql security definer set search_path=pg_catalog as $$
begin
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
  select new.business_id,c.branch_id,new.uploaded_by,
    case when tg_op='INSERT' then 'document_uploaded'
      when old.storage_provider='supabase' and new.storage_provider='gcs' and old.object_path is not distinct from new.object_path
        then 'document_storage_migrated'
      when old.storage_provider='gcs' and new.storage_provider='supabase' and new.migrated_at is null and old.object_path is not distinct from new.object_path
        then 'document_storage_rolled_back'
      else 'document_replaced' end,
    'client_document',new.client_id::text||'/'||new.kind,
    case when tg_op='UPDATE' then to_jsonb(old) else null end,to_jsonb(new)
  from public.loan_clients c where c.id=new.client_id;
  return new;
end $$;

-- 8. Runtime RPCs for the bripta-documents Edge Function (service role only; actor passed explicitly).
create or replace function public.bripta_document_lookup(p_uid uuid,p_email text,p_client uuid,p_kind text)
returns table(client_id uuid,kind text,storage_provider text,object_path text,gcs_bucket text,gcs_object_key text,
  gcs_generation bigint,sha256 text,mime_type text,file_size bigint)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
begin
  perform public.bripta_require_service_role();
  if not public.bripta_document_access_as(p_uid,p_email,p_client) then
    raise exception 'Not authorized for this client' using errcode='42501'; end if;
  return query select d.client_id,d.kind,d.storage_provider,d.object_path,d.gcs_bucket,d.gcs_object_key,
    d.gcs_generation,d.sha256,d.mime_type,d.file_size
  from public.bripta_client_documents d
  where d.business_id='BIZ-B3F5E5D9' and d.client_id=p_client and (p_kind is null or d.kind=p_kind);
end $$;
revoke all on function public.bripta_document_lookup(uuid,text,uuid,text) from public,anon,authenticated;
grant execute on function public.bripta_document_lookup(uuid,text,uuid,text) to service_role;

create or replace function public.bripta_document_gcs_commit(
  p_uid uuid,p_email text,p_client uuid,p_kind text,p_expected_previous text,
  p_bucket text,p_key text,p_generation bigint,p_sha256 text,p_size bigint,p_mime text,p_file_name text)
returns table(previous_provider text,previous_object_path text,previous_gcs_bucket text,previous_gcs_key text,previous_gcs_generation bigint)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
declare v_old public.bripta_client_documents%rowtype; v_actor public.loan_staff%rowtype;
begin
  perform public.bripta_require_service_role();
  if not public.bripta_document_access_as(p_uid,p_email,p_client) then
    raise exception 'Not authorized for this client' using errcode='42501'; end if;
  v_actor:=public.bripta_document_actor(p_uid,p_email);
  select * into v_old from public.bripta_client_documents d
    where d.client_id=p_client and d.kind=p_kind for update;
  if coalesce(v_old.gcs_object_key,v_old.object_path,'') is distinct from coalesce(p_expected_previous,'') then
    raise exception 'The document changed while uploading; reopen and try again' using errcode='40001'; end if;
  insert into public.bripta_client_documents as d(client_id,kind,business_id,object_path,file_name,mime_type,file_size,
    uploaded_by,uploaded_at,storage_provider,gcs_bucket,gcs_object_key,gcs_generation,sha256,migrated_at)
  values(p_client,p_kind,'BIZ-B3F5E5D9',null,left(coalesce(nullif(trim(p_file_name),''),'document'),200),p_mime,p_size,
    v_actor.id,now(),'gcs',p_bucket,p_key,p_generation,p_sha256,null)
  on conflict (client_id,kind) do update set object_path=null,file_name=excluded.file_name,mime_type=excluded.mime_type,
    file_size=excluded.file_size,uploaded_by=excluded.uploaded_by,uploaded_at=now(),storage_provider='gcs',
    gcs_bucket=excluded.gcs_bucket,gcs_object_key=excluded.gcs_object_key,gcs_generation=excluded.gcs_generation,
    sha256=excluded.sha256,migrated_at=null;
  -- A replaced Supabase original is queued for the approved cleanup, never deleted here.
  if v_old.object_path is not null then
    insert into public.bripta_document_object_queue(provider,bucket,object_key,client_id,kind,reason)
    values('supabase','bripta-client-documents',v_old.object_path,p_client,p_kind,'replaced_by_staff')
    on conflict do nothing;
  end if;
  return query select v_old.storage_provider,v_old.object_path,v_old.gcs_bucket,v_old.gcs_object_key,v_old.gcs_generation;
end $$;
revoke all on function public.bripta_document_gcs_commit(uuid,text,uuid,text,text,text,text,bigint,text,bigint,text,text) from public,anon,authenticated;
grant execute on function public.bripta_document_gcs_commit(uuid,text,uuid,text,text,text,text,bigint,text,bigint,text,text) to service_role;

create or replace function public.bripta_document_queue_object(
  p_provider text,p_bucket text,p_key text,p_generation bigint,p_client uuid,p_kind text,p_reason text)
returns bigint language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v_id bigint;
begin
  perform public.bripta_require_service_role();
  insert into public.bripta_document_object_queue(provider,bucket,object_key,gcs_generation,client_id,kind,reason)
  values(p_provider,p_bucket,p_key,p_generation,p_client,p_kind,p_reason)
  on conflict (provider,bucket,object_key) do update set reason=excluded.reason returning id into v_id;
  return v_id;
end $$;
revoke all on function public.bripta_document_queue_object(text,text,text,bigint,uuid,text,text) from public,anon,authenticated;
grant execute on function public.bripta_document_queue_object(text,text,text,bigint,uuid,text,text) to service_role;

create or replace function public.bripta_document_queue_done(p_id bigint)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v_q public.bripta_document_object_queue%rowtype;
begin
  perform public.bripta_require_service_role();
  select * into v_q from public.bripta_document_object_queue where id=p_id and deleted_at is null for update;
  if v_q.id is null then return false; end if;
  if v_q.provider='supabase' and exists(select 1 from storage.objects o where o.bucket_id=v_q.bucket and o.name=v_q.object_key) then
    return false; end if;
  if exists(select 1 from public.bripta_client_documents d where d.object_path=v_q.object_key or d.gcs_object_key=v_q.object_key) then
    return false; end if;
  update public.bripta_document_object_queue set deleted_at=now() where id=p_id;
  return true;
end $$;
revoke all on function public.bripta_document_queue_done(bigint) from public,anon,authenticated;
grant execute on function public.bripta_document_queue_done(bigint) to service_role;

-- Permanent client deletion (admin only, no loans). Rows go in one transaction; objects are queued
-- with delete_now so the Edge Function removes them through the Storage APIs straight after.
create or replace function public.bripta_delete_client_with_documents(p_uid uuid,p_email text,p_client uuid)
returns table(queue_id bigint,provider text,bucket text,object_key text,gcs_generation bigint)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
declare v_actor public.loan_staff%rowtype; v_client public.loan_clients%rowtype; v_ids bigint[]:='{}'; r record; v_id bigint;
begin
  perform public.bripta_require_service_role();
  v_actor:=public.bripta_document_actor(p_uid,p_email);
  if v_actor.id is null or not ('admin'=any(regexp_split_to_array(lower(trim(coalesce(v_actor.role,''))),'\s*,\s*'))) then
    raise exception 'Only admins can delete clients' using errcode='42501'; end if;
  select * into v_client from public.loan_clients c where c.id=p_client and c.business_id='BIZ-B3F5E5D9' for update;
  if v_client.id is null then raise exception 'Client not found' using errcode='P0002'; end if;
  if exists(select 1 from public.loans l where l.client_id=p_client) then
    raise exception 'Cannot delete client with existing loans' using errcode='23503'; end if;
  for r in select * from public.bripta_client_documents d where d.client_id=p_client loop
    if r.gcs_object_key is not null then
      insert into public.bripta_document_object_queue(provider,bucket,object_key,gcs_generation,client_id,kind,reason,delete_now)
      values('gcs',r.gcs_bucket,r.gcs_object_key,r.gcs_generation,p_client,r.kind,'client_deleted',true)
      on conflict (provider,bucket,object_key) do update set reason='client_deleted',delete_now=true returning id into v_id;
      v_ids:=v_ids||v_id;
    end if;
    if r.object_path is not null then
      insert into public.bripta_document_object_queue(provider,bucket,object_key,client_id,kind,reason,delete_now)
      values('supabase','bripta-client-documents',r.object_path,p_client,r.kind,'client_deleted',true)
      on conflict (provider,bucket,object_key) do update set reason='client_deleted',delete_now=true returning id into v_id;
      v_ids:=v_ids||v_id;
    end if;
  end loop;
  update public.bripta_gcs_document_migration set state='skipped',last_error='client_deleted',updated_at=now()
    where client_id=p_client and state not in ('skipped','rolled_back');
  delete from public.bripta_client_documents where client_id=p_client;
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
    values('BIZ-B3F5E5D9',v_client.branch_id,v_actor.id,'client_deleted_with_documents','client',p_client::text,
      jsonb_build_object('documents',cardinality(v_ids)),null);
  delete from public.loan_clients where id=p_client;
  return query select q.id,q.provider,q.bucket,q.object_key,q.gcs_generation
    from public.bripta_document_object_queue q where q.id=any(v_ids);
end $$;
revoke all on function public.bripta_delete_client_with_documents(uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.bripta_delete_client_with_documents(uuid,text,uuid) to service_role;

-- 9. Migration job RPCs (service role only).
create or replace function public.bripta_gcs_migration_claim(p_limit integer,p_bucket text)
returns table(id bigint,client_id uuid,kind text,supabase_path text,expected_bytes bigint,gcs_bucket text,
  gcs_object_key text,gcs_generation bigint,sha256 text,state text,attempts integer)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
begin
  perform public.bripta_require_service_role();
  if coalesce(p_bucket,'')!~'^[a-z0-9][a-z0-9._-]{2,62}$' then raise exception 'Invalid bucket'; end if;
  -- Retire ledger rows whose document was replaced/removed since the claim.
  update public.bripta_gcs_document_migration m set state='skipped',last_error='document_changed',updated_at=now()
  where m.state in ('claimed','copied','failed') and not exists(
    select 1 from public.bripta_client_documents d where d.client_id=m.client_id and d.kind=m.kind
      and d.storage_provider='supabase' and d.object_path=m.supabase_path);
  insert into public.bripta_gcs_document_migration(client_id,kind,supabase_object_id,supabase_path,expected_bytes,gcs_bucket,gcs_object_key)
  select d.client_id,d.kind,o.id,o.name,(o.metadata->>'size')::bigint,p_bucket,'bripta/docs/v1/'||gen_random_uuid()::text
  from public.bripta_client_documents d
  join storage.objects o on o.bucket_id='bripta-client-documents' and o.name=d.object_path
  where d.business_id='BIZ-B3F5E5D9' and d.storage_provider='supabase' and o.metadata->>'size' ~ '^[0-9]+$'
  on conflict (supabase_object_id) do nothing;
  return query select m.id,m.client_id,m.kind,m.supabase_path,m.expected_bytes,m.gcs_bucket,m.gcs_object_key,
    m.gcs_generation,m.sha256,m.state,m.attempts
  from public.bripta_gcs_document_migration m
  where m.state in ('claimed','copied','switched') or (m.state='failed' and m.attempts<3)
  order by case m.state when 'switched' then 0 when 'copied' then 1 else 2 end,m.id
  limit least(greatest(coalesce(p_limit,5),1),50);
end $$;
revoke all on function public.bripta_gcs_migration_claim(integer,text) from public,anon,authenticated;
grant execute on function public.bripta_gcs_migration_claim(integer,text) to service_role;

create or replace function public.bripta_gcs_migration_mark_copied(p_id bigint,p_sha256 text,p_generation bigint,p_bytes bigint)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v public.bripta_gcs_document_migration%rowtype;
begin
  perform public.bripta_require_service_role();
  select * into v from public.bripta_gcs_document_migration where id=p_id for update;
  if v.id is null or v.state not in ('claimed','copied','failed') then return false; end if;
  if p_bytes is distinct from v.expected_bytes or coalesce(p_sha256,'')!~'^[0-9a-f]{64}$' or p_generation is null then
    raise exception 'Copy does not match the original'; end if;
  if v.sha256 is not null and v.sha256<>p_sha256 then raise exception 'Checksum changed between attempts'; end if;
  update public.bripta_gcs_document_migration set state='copied',sha256=p_sha256,gcs_generation=p_generation,
    copied_at=now(),updated_at=now(),last_error=null where id=p_id;
  return true;
end $$;
revoke all on function public.bripta_gcs_migration_mark_copied(bigint,text,bigint,bigint) from public,anon,authenticated;
grant execute on function public.bripta_gcs_migration_mark_copied(bigint,text,bigint,bigint) to service_role;

create or replace function public.bripta_gcs_migration_switch(p_id bigint)
returns text language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v public.bripta_gcs_document_migration%rowtype; d public.bripta_client_documents%rowtype; n integer;
begin
  perform public.bripta_require_service_role();
  select * into v from public.bripta_gcs_document_migration where id=p_id for update;
  if v.id is null then return 'missing'; end if;
  if v.state in ('switched','verified') then return 'already_switched'; end if;
  if v.state<>'copied' then return 'not_copied'; end if;
  select * into d from public.bripta_client_documents x where x.client_id=v.client_id and x.kind=v.kind for update;
  if d.client_id is null or d.storage_provider<>'supabase' or d.object_path is distinct from v.supabase_path
    or not exists(select 1 from storage.objects o where o.id=v.supabase_object_id and o.bucket_id='bripta-client-documents'
      and o.name=v.supabase_path and (o.metadata->>'size')::bigint=v.expected_bytes) then
    update public.bripta_gcs_document_migration set state='skipped',last_error='document_changed',updated_at=now() where id=p_id;
    return 'document_changed';
  end if;
  update public.bripta_client_documents set storage_provider='gcs',gcs_bucket=v.gcs_bucket,gcs_object_key=v.gcs_object_key,
    gcs_generation=v.gcs_generation,sha256=v.sha256,migrated_at=now()
  where client_id=v.client_id and kind=v.kind and object_path=v.supabase_path and storage_provider='supabase';
  get diagnostics n=row_count;
  if n<>1 then raise exception 'Document link changed'; end if;
  update public.bripta_gcs_document_migration set state='switched',switched_at=now(),updated_at=now() where id=p_id;
  return 'switched';
end $$;
revoke all on function public.bripta_gcs_migration_switch(bigint) from public,anon,authenticated;
grant execute on function public.bripta_gcs_migration_switch(bigint) to service_role;

create or replace function public.bripta_gcs_migration_record(p_id bigint,p_state text,p_error text)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
begin
  perform public.bripta_require_service_role();
  if p_state not in ('verified','failed') then raise exception 'Invalid state'; end if;
  update public.bripta_gcs_document_migration m set state=p_state,
    attempts=m.attempts+case when p_state='failed' then 1 else 0 end,
    last_error=case when p_state='failed' then left(regexp_replace(coalesce(p_error,'unknown'),'[^A-Za-z0-9_:. -]','','g'),120) else null end,
    verified_at=case when p_state='verified' then now() else m.verified_at end,updated_at=now()
  where m.id=p_id and (p_state='failed' or m.state='switched');
  return found;
end $$;
revoke all on function public.bripta_gcs_migration_record(bigint,text,text) from public,anon,authenticated;
grant execute on function public.bripta_gcs_migration_record(bigint,text,text) to service_role;

-- Used by the Edge Function's verify action: returns the switched GCS reference for one ledger row.
create or replace function public.bripta_gcs_migration_target(p_id bigint)
returns table(gcs_bucket text,gcs_object_key text,gcs_generation bigint,sha256 text,file_size bigint,mime_type text)
language plpgsql stable security definer set search_path=pg_catalog as $$
#variable_conflict use_column
begin
  perform public.bripta_require_service_role();
  return query select d.gcs_bucket,d.gcs_object_key,d.gcs_generation,d.sha256,d.file_size,d.mime_type
  from public.bripta_gcs_document_migration m
  join public.bripta_client_documents d on d.client_id=m.client_id and d.kind=m.kind
    and d.storage_provider='gcs' and d.gcs_object_key=m.gcs_object_key
  where m.id=p_id;
end $$;
revoke all on function public.bripta_gcs_migration_target(bigint) from public,anon,authenticated;
grant execute on function public.bripta_gcs_migration_target(bigint) to service_role;

-- Point a migrated document back to its Supabase original (original must still exist).
create or replace function public.bripta_gcs_migration_rollback(p_id bigint)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v public.bripta_gcs_document_migration%rowtype; n integer;
begin
  perform public.bripta_require_service_role();
  select * into v from public.bripta_gcs_document_migration where id=p_id for update;
  if v.id is null or not exists(select 1 from storage.objects o where o.bucket_id='bripta-client-documents' and o.name=v.supabase_path) then
    return false; end if;
  update public.bripta_client_documents set storage_provider='supabase',gcs_bucket=null,gcs_object_key=null,
    gcs_generation=null,sha256=null,migrated_at=null
  where client_id=v.client_id and kind=v.kind and storage_provider='gcs' and gcs_object_key=v.gcs_object_key
    and object_path=v.supabase_path;
  get diagnostics n=row_count;
  update public.bripta_gcs_document_migration set state='rolled_back',updated_at=now() where id=p_id;
  return n=1;
end $$;
revoke all on function public.bripta_gcs_migration_rollback(bigint) from public,anon,authenticated;
grant execute on function public.bripta_gcs_migration_rollback(bigint) to service_role;

-- 10. Cleanup manifest (read by the owner) and approval / deletion RPCs.
create or replace view public.bripta_document_cleanup_manifest with (security_invoker=true) as
with objs as (
  select o.id as object_id,o.name as object_path,(o.metadata->>'size')::bigint as size_bytes,o.created_at
  from storage.objects o where o.bucket_id='bripta-client-documents' and o.metadata->>'size' ~ '^[0-9]+$'
), classified as (
  select x.*,m.id as ledger_id,
    case
      when exists(select 1 from public.bripta_client_documents d where d.storage_provider='supabase' and d.object_path=x.object_path)
        then 'still_linked_original'
      when m.id is not null and m.state='verified' and m.supabase_path=x.object_path and exists(
        select 1 from public.bripta_client_documents d where d.client_id=m.client_id and d.kind=m.kind
          and d.storage_provider='gcs' and d.gcs_object_key=m.gcs_object_key and d.sha256=m.sha256
          and d.object_path=x.object_path)
        then 'migrated_original'
      when exists(select 1 from public.bripta_client_documents d where d.object_path=x.object_path)
        then 'uncertain'  -- linked fallback whose migration is not verified
      when exists(select 1 from public.bripta_document_storage_baseline b where b.object_id=x.object_id and not b.linked_at_baseline)
        then 'previously_unlinked'
      else 'uncertain'
    end as category
  from objs x left join public.bripta_gcs_document_migration m on m.supabase_object_id=x.object_id
)
select * from classified;
revoke all on public.bripta_document_cleanup_manifest from public,anon,authenticated;

create or replace function public.bripta_document_manifest_digest(p_category text)
returns text language sql stable security definer set search_path=pg_catalog as $$
  select md5(coalesce(string_agg(object_id::text||':'||size_bytes::text,',' order by object_id),''))
  from public.bripta_document_cleanup_manifest where category=p_category
$$;
revoke all on function public.bripta_document_manifest_digest(text) from public,anon,authenticated;

-- Owner-only (SQL Editor). Approves exactly the objects whose digest the owner reviewed.
create or replace function public.bripta_document_approve_cleanup(p_category text,p_digest text)
returns integer language plpgsql volatile security definer set search_path=pg_catalog as $$
declare n integer;
begin
  if p_category not in ('migrated_original','previously_unlinked','uncertain') then raise exception 'Unknown category'; end if;
  if public.bripta_document_manifest_digest(p_category)<>p_digest then
    raise exception 'Manifest changed since review; regenerate and review it again'; end if;
  insert into public.bripta_document_cleanup_approval(object_id,category,object_path,size_bytes,ledger_id,manifest_digest)
  select object_id,category,object_path,size_bytes,ledger_id,p_digest
  from public.bripta_document_cleanup_manifest where category=p_category
  on conflict (object_id) do nothing;
  get diagnostics n=row_count;
  return n;
end $$;
revoke all on function public.bripta_document_approve_cleanup(text,text) from public,anon,authenticated,service_role;

create or replace function public.bripta_document_cleanup_next(p_limit integer)
returns table(object_id uuid,category text,object_path text,size_bytes bigint,gcs_bucket text,gcs_object_key text,gcs_generation bigint,sha256 text)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
begin
  perform public.bripta_require_service_role();
  -- Re-check at deletion time: an approval is void if the object's category changed.
  update public.bripta_document_cleanup_approval a set skipped_reason='category_changed'
  where a.deleted_at is null and a.skipped_reason is null and not exists(
    select 1 from public.bripta_document_cleanup_manifest m where m.object_id=a.object_id and m.category=a.category
      and m.size_bytes=a.size_bytes and m.object_path=a.object_path);
  return query select a.object_id,a.category,a.object_path,a.size_bytes,d.gcs_bucket,d.gcs_object_key,d.gcs_generation,d.sha256
  from public.bripta_document_cleanup_approval a
  left join public.bripta_gcs_document_migration m on m.id=a.ledger_id
  left join public.bripta_client_documents d on a.category='migrated_original' and d.client_id=m.client_id and d.kind=m.kind
    and d.gcs_object_key=m.gcs_object_key
  where a.deleted_at is null and a.skipped_reason is null
  order by a.object_id limit least(greatest(coalesce(p_limit,20),1),100);
end $$;
revoke all on function public.bripta_document_cleanup_next(integer) from public,anon,authenticated;
grant execute on function public.bripta_document_cleanup_next(integer) to service_role;

create or replace function public.bripta_document_cleanup_mark(p_object_id uuid,p_skip_reason text)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v public.bripta_document_cleanup_approval%rowtype;
begin
  perform public.bripta_require_service_role();
  select * into v from public.bripta_document_cleanup_approval where object_id=p_object_id and deleted_at is null for update;
  if v.object_id is null then return false; end if;
  if p_skip_reason is not null then
    update public.bripta_document_cleanup_approval set skipped_reason=left(regexp_replace(p_skip_reason,'[^a-z_]','','g'),60)
      where object_id=p_object_id;
    return true;
  end if;
  if exists(select 1 from storage.objects o where o.id=p_object_id) then return false; end if;
  update public.bripta_document_cleanup_approval set deleted_at=now() where object_id=p_object_id;
  return true;
end $$;
revoke all on function public.bripta_document_cleanup_mark(uuid,text) from public,anon,authenticated;
grant execute on function public.bripta_document_cleanup_mark(uuid,text) to service_role;

notify pgrst,'reload schema';
commit;

select storage_provider,count(*) as documents from public.bripta_client_documents
where business_id='BIZ-B3F5E5D9' group by storage_provider order by 1;
