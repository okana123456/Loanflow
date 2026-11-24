-- Bripta only. Run once before deploying the updated bripta-documents Edge Function.
-- Existing client-photos objects remain viewable. New profile photos use private B2.
create table if not exists public.bripta_b2_profile_photos (
  client_id uuid primary key references public.loan_clients(id) on delete cascade,
  business_id text not null default 'BIZ-B3F5E5D9' check (business_id='BIZ-B3F5E5D9'),
  ext_bucket text not null,
  object_key text not null unique,
  ext_version text not null,
  sha256 text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  size_bytes integer not null check (size_bytes between 1 and 280000),
  uploaded_at timestamptz not null default now()
);
alter table public.bripta_b2_profile_photos enable row level security;
revoke all on public.bripta_b2_profile_photos from public,anon,authenticated;

create or replace function public.bripta_b2_profile_lookup(p_uid uuid,p_email text,p_client uuid)
returns table(ext_bucket text,object_key text,ext_version text,sha256 text)
language plpgsql volatile security definer set search_path=pg_catalog as $$
begin
  perform public.bripta_require_service_role();
  if not public.bripta_document_access_as(p_uid,p_email,p_client) then
    raise exception 'Not authorized for this client' using errcode='42501'; end if;
  return query select p.ext_bucket,p.object_key,p.ext_version,p.sha256 from public.bripta_b2_profile_photos p
    join public.loan_clients c on c.id=p.client_id
    where p.client_id=p_client and c.photo_path='b2';
end $$;
revoke all on function public.bripta_b2_profile_lookup(uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.bripta_b2_profile_lookup(uuid,text,uuid) to service_role;

create or replace function public.bripta_b2_profile_commit(
  p_uid uuid,p_email text,p_client uuid,p_previous text,p_bucket text,p_key text,p_version text,p_sha256 text,p_size integer)
returns table(previous_path text,previous_key text,previous_version text)
language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v_client public.loan_clients%rowtype; v_old public.bripta_b2_profile_photos%rowtype;
  v_actor public.loan_staff%rowtype; v_roles text[];
begin
  perform public.bripta_require_service_role();
  if not public.bripta_document_access_as(p_uid,p_email,p_client) then
    raise exception 'Not authorized for this client' using errcode='42501'; end if;
  v_actor:=public.bripta_document_actor(p_uid,p_email);
  v_roles:=regexp_split_to_array(lower(trim(coalesce(v_actor.role,''))),'\s*,\s*');
  if coalesce(p_bucket,'')='' or p_key !~ '^bripta/photos/v1/[0-9a-f-]{36}$' or coalesce(p_version,'')='' or
     p_sha256 !~ '^[0-9a-f]{64}$' or p_size not between 1 and 280000 then
    raise exception 'Invalid profile photo metadata'; end if;
  select * into v_client from public.loan_clients c where c.id=p_client and c.business_id='BIZ-B3F5E5D9' for update;
  if v_client.id is null then raise exception 'Client not found' using errcode='P0002'; end if;
  if not ('admin'=any(v_roles) or 'branch_manager'=any(v_roles) or
          ('loan_officer'=any(v_roles) and v_client.photo_path is null)) then
    raise exception 'Only managers may replace a client photo' using errcode='42501'; end if;
  if coalesce(v_client.photo_path,'') is distinct from coalesce(p_previous,'') then
    raise exception 'Photo changed while uploading' using errcode='40001'; end if;
  select * into v_old from public.bripta_b2_profile_photos p where p.client_id=p_client for update;
  insert into public.bripta_b2_profile_photos as p(client_id,ext_bucket,object_key,ext_version,sha256,size_bytes)
  values(p_client,p_bucket,p_key,p_version,p_sha256,p_size)
  on conflict (client_id) do update set ext_bucket=excluded.ext_bucket,object_key=excluded.object_key,ext_version=excluded.ext_version,
    sha256=excluded.sha256,size_bytes=excluded.size_bytes,uploaded_at=now();
  update public.loan_clients set photo_path='b2' where id=p_client;
  return query select v_client.photo_path,v_old.object_key,v_old.ext_version;
end $$;
revoke all on function public.bripta_b2_profile_commit(uuid,text,uuid,text,text,text,text,text,integer) from public,anon,authenticated;
grant execute on function public.bripta_b2_profile_commit(uuid,text,uuid,text,text,text,text,text,integer) to service_role;

create or replace function public.bripta_b2_profile_remove(p_uid uuid,p_email text,p_client uuid)
returns table(previous_path text,previous_key text,previous_version text)
language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v_client public.loan_clients%rowtype; v_old public.bripta_b2_profile_photos%rowtype;
  v_actor public.loan_staff%rowtype; v_roles text[];
begin
  perform public.bripta_require_service_role();
  if not public.bripta_document_access_as(p_uid,p_email,p_client) then
    raise exception 'Not authorized for this client' using errcode='42501'; end if;
  v_actor:=public.bripta_document_actor(p_uid,p_email);
  v_roles:=regexp_split_to_array(lower(trim(coalesce(v_actor.role,''))),'\s*,\s*');
  if not ('admin'=any(v_roles) or 'branch_manager'=any(v_roles)) then
    raise exception 'Only managers may remove a client photo' using errcode='42501'; end if;
  select * into v_client from public.loan_clients c where c.id=p_client and c.business_id='BIZ-B3F5E5D9' for update;
  if v_client.id is null then raise exception 'Client not found' using errcode='P0002'; end if;
  select * into v_old from public.bripta_b2_profile_photos p where p.client_id=p_client for update;
  update public.loan_clients set photo_path=null where id=p_client;
  delete from public.bripta_b2_profile_photos where client_id=p_client;
  return query select v_client.photo_path,v_old.object_key,v_old.ext_version;
end $$;
revoke all on function public.bripta_b2_profile_remove(uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.bripta_b2_profile_remove(uuid,text,uuid) to service_role;

-- The cleanup acknowledgement must also protect profile-photo references.
create or replace function public.bripta_document_queue_done(p_id bigint)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog as $$
declare v_q public.bripta_document_object_queue%rowtype;
begin
  perform public.bripta_require_service_role();
  select * into v_q from public.bripta_document_object_queue where id=p_id and deleted_at is null for update;
  if v_q.id is null then return false; end if;
  if v_q.provider='supabase' and exists(select 1 from storage.objects o where o.bucket_id=v_q.bucket and o.name=v_q.object_key) then
    return false; end if;
  if exists(select 1 from public.bripta_client_documents d where d.object_path=v_q.object_key or d.ext_object_key=v_q.object_key) then
    return false; end if;
  if exists(select 1 from public.bripta_b2_profile_photos p where p.object_key=v_q.object_key) then
    return false; end if;
  if v_q.bucket='client-photos' and exists(select 1 from public.loan_clients c where c.photo_path=v_q.object_key) then
    return false; end if;
  update public.bripta_document_object_queue set deleted_at=now() where id=p_id;
  return true;
end $$;
revoke all on function public.bripta_document_queue_done(bigint) from public,anon,authenticated;
grant execute on function public.bripta_document_queue_done(bigint) to service_role;

-- Include B2 profile photos and legacy public photos in permanent client deletion.
create or replace function public.bripta_delete_client_with_documents(p_uid uuid,p_email text,p_client uuid)
returns table(queue_id bigint,provider text,bucket text,object_key text,ext_version text)
language plpgsql volatile security definer set search_path=pg_catalog as $$
#variable_conflict use_column
declare v_actor public.loan_staff%rowtype; v_client public.loan_clients%rowtype; v_photo public.bripta_b2_profile_photos%rowtype;
  v_ids bigint[]:='{}'; r record; v_id bigint;
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
    if r.ext_object_key is not null then
      insert into public.bripta_document_object_queue(provider,bucket,object_key,ext_version,client_id,kind,reason,delete_now)
      values('b2',r.ext_bucket,r.ext_object_key,r.ext_version,p_client,r.kind,'client_deleted',true)
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
  select * into v_photo from public.bripta_b2_profile_photos p where p.client_id=p_client;
  if v_photo.client_id is not null then
    insert into public.bripta_document_object_queue(provider,bucket,object_key,ext_version,client_id,kind,reason,delete_now)
    values('b2',v_photo.ext_bucket,v_photo.object_key,v_photo.ext_version,p_client,'profile_photo','client_deleted',true)
    on conflict (provider,bucket,object_key) do update set reason='client_deleted',delete_now=true returning id into v_id;
    v_ids:=v_ids||v_id;
  elsif v_client.photo_path is not null and v_client.photo_path<>'b2' then
    insert into public.bripta_document_object_queue(provider,bucket,object_key,client_id,kind,reason,delete_now)
    values('supabase','client-photos',v_client.photo_path,p_client,'profile_photo','client_deleted',true)
    on conflict (provider,bucket,object_key) do update set reason='client_deleted',delete_now=true returning id into v_id;
    v_ids:=v_ids||v_id;
  end if;
  update public.bripta_b2_document_migration set state='skipped',last_error='client_deleted',updated_at=now()
    where client_id=p_client and state not in ('skipped','rolled_back');
  delete from public.bripta_client_documents where client_id=p_client;
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
    values('BIZ-B3F5E5D9',v_client.branch_id,v_actor.id,'client_deleted_with_documents','client',p_client::text,
      jsonb_build_object('documents',cardinality(v_ids)),null);
  delete from public.loan_clients where id=p_client;
  return query select q.id,q.provider,q.bucket,q.object_key,q.ext_version
    from public.bripta_document_object_queue q where q.id=any(v_ids);
end $$;
revoke all on function public.bripta_delete_client_with_documents(uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.bripta_delete_client_with_documents(uuid,text,uuid) to service_role;
