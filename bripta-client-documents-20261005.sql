-- Private onboarding attachments for Bripta clients and guarantors.
-- Existing client photos, financial records and other businesses are unchanged.
begin;
create table if not exists public.bripta_client_documents (
  client_id uuid not null references public.loan_clients(id),
  kind text not null check (kind in ('client_passport','client_id_front','client_id_back','guarantor_passport','guarantor_id_front','guarantor_id_back')),
  business_id text not null default 'BIZ-B3F5E5D9' check(business_id='BIZ-B3F5E5D9'),
  object_path text not null unique,
  file_name text not null,
  mime_type text not null check(mime_type in ('image/jpeg','image/png','image/webp','application/pdf')),
  file_size bigint not null check(file_size>0 and file_size<=8388608),
  uploaded_by uuid references public.loan_staff(id),
  uploaded_at timestamptz not null default now(),
  primary key(client_id,kind)
);

create or replace function public.bripta_can_access_client_documents(p_client uuid)
returns boolean language plpgsql stable security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; client public.loan_clients%rowtype; roles text[];
begin
  if auth.uid() is null then return false; end if;
  select s.* into actor from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  if actor.id is null then return false; end if;
  select c.* into client from public.loan_clients c where c.id=p_client and c.business_id=actor.business_id;
  if client.id is null then return false; end if;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if 'admin'=any(roles) then return true; end if;
  if actor.branch_id is null or client.branch_id is distinct from actor.branch_id then return false; end if;
  return 'branch_manager'=any(roles) or ('loan_officer'=any(roles) and (
    client.loan_officer_id=actor.id or exists(select 1 from public.loans l
      where l.client_id=client.id and l.business_id=actor.business_id and l.branch_id=actor.branch_id and l.loan_officer_id=actor.id)
  ));
end $$;
revoke all on function public.bripta_can_access_client_documents(uuid) from public,anon;
grant execute on function public.bripta_can_access_client_documents(uuid) to authenticated;

create or replace function public.bripta_can_access_document_path(p_path text)
returns boolean language plpgsql stable security definer set search_path=pg_catalog as $$
declare parts text[]:=string_to_array(p_path,'/');
begin
  if cardinality(parts)<>4 or parts[1]<>'BIZ-B3F5E5D9'
    or parts[2]!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or parts[3] not in ('client_passport','client_id_front','client_id_back','guarantor_passport','guarantor_id_front','guarantor_id_back')
    or parts[4]!~*'^[0-9a-f-]+\.(jpg|png|webp|pdf)$' then return false; end if;
  return public.bripta_can_access_client_documents(parts[2]::uuid);
end $$;
revoke all on function public.bripta_can_access_document_path(text) from public,anon;
grant execute on function public.bripta_can_access_document_path(text) to authenticated;

insert into storage.buckets(id,name,public,file_size_limit,allowed_mime_types)
values('bripta-client-documents','bripta-client-documents',false,8388608,array['image/jpeg','image/png','image/webp','application/pdf'])
on conflict(id) do update set public=false,file_size_limit=excluded.file_size_limit,allowed_mime_types=excluded.allowed_mime_types;

drop policy if exists bripta_documents_storage_read on storage.objects;
create policy bripta_documents_storage_read on storage.objects for select to authenticated
using(bucket_id='bripta-client-documents' and public.bripta_can_access_document_path(name));
drop policy if exists bripta_documents_storage_insert on storage.objects;
create policy bripta_documents_storage_insert on storage.objects for insert to authenticated
with check(bucket_id='bripta-client-documents' and public.bripta_can_access_document_path(name));
drop policy if exists bripta_documents_storage_delete on storage.objects;
create policy bripta_documents_storage_delete on storage.objects for delete to authenticated
using(bucket_id='bripta-client-documents' and public.bripta_can_access_document_path(name));
-- Restrict this private bucket even if a pre-existing broad storage policy exists.
drop policy if exists bripta_documents_storage_boundary on storage.objects;
create policy bripta_documents_storage_boundary on storage.objects as restrictive for all to public
using(bucket_id<>'bripta-client-documents' or public.bripta_can_access_document_path(name))
with check(bucket_id<>'bripta-client-documents' or public.bripta_can_access_document_path(name));
-- The boundary must evaluate safely for unauthenticated requests as well.
grant execute on function public.bripta_can_access_document_path(text) to anon;

alter table public.bripta_client_documents enable row level security;
grant select,insert,update on public.bripta_client_documents to authenticated;
drop policy if exists bripta_client_documents_read on public.bripta_client_documents;
create policy bripta_client_documents_read on public.bripta_client_documents for select to authenticated
using(business_id='BIZ-B3F5E5D9' and public.bripta_can_access_client_documents(client_id));
drop policy if exists bripta_client_documents_insert on public.bripta_client_documents;
create policy bripta_client_documents_insert on public.bripta_client_documents for insert to authenticated
with check(business_id='BIZ-B3F5E5D9' and public.bripta_can_access_client_documents(client_id));
drop policy if exists bripta_client_documents_update on public.bripta_client_documents;
create policy bripta_client_documents_update on public.bripta_client_documents for update to authenticated
using(business_id='BIZ-B3F5E5D9' and public.bripta_can_access_client_documents(client_id))
with check(business_id='BIZ-B3F5E5D9' and public.bripta_can_access_client_documents(client_id));

create or replace function public.bripta_validate_client_document()
returns trigger language plpgsql security definer set search_path=pg_catalog as $$
begin
  if not public.bripta_can_access_client_documents(new.client_id)
    or split_part(new.object_path,'/',1)<>'BIZ-B3F5E5D9'
    or split_part(new.object_path,'/',2)<>new.client_id::text
    or split_part(new.object_path,'/',3)<>new.kind
    or not public.bripta_can_access_document_path(new.object_path)
    or not exists(select 1 from storage.objects o where o.bucket_id='bripta-client-documents' and o.name=new.object_path)
    then raise exception 'Upload the document to the authorized client folder first' using errcode='42501'; end if;
  if new.kind in ('client_passport','guarantor_passport') and new.mime_type='application/pdf' then
    raise exception 'Passport photo must be a JPG, PNG or WEBP image'; end if;
  select s.id into new.uploaded_by from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  new.uploaded_at:=now();
  return new;
end $$;
drop trigger if exists bripta_validate_document on public.bripta_client_documents;
create trigger bripta_validate_document before insert or update on public.bripta_client_documents
for each row execute function public.bripta_validate_client_document();

create or replace function public.bripta_audit_client_document()
returns trigger language plpgsql security definer set search_path=pg_catalog as $$
begin
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
  select new.business_id,c.branch_id,new.uploaded_by,
    case when tg_op='INSERT' then 'document_uploaded' else 'document_replaced' end,
    'client_document',new.client_id::text||'/'||new.kind,
    case when tg_op='UPDATE' then to_jsonb(old) else null end,to_jsonb(new)
  from public.loan_clients c where c.id=new.client_id;
  return new;
end $$;
drop trigger if exists bripta_document_audit on public.bripta_client_documents;
create trigger bripta_document_audit after insert or update on public.bripta_client_documents
for each row execute function public.bripta_audit_client_document();
notify pgrst,'reload schema';
commit;
select id as bucket,not public as private_storage,file_size_limit from storage.buckets where id='bripta-client-documents';
