-- Bripta: transfer the removed staff portfolio to George Majwa and enforce
-- both permanent officer-attribution redirects at the database boundary.

begin;

do $$
declare
  v_business_id text := 'BIZ-B3F5E5D9';
  v_former_officer_id uuid := '910139c3-36b8-49bf-915b-764d7095a3ec';
  v_george_id uuid := '74b5e8ca-fabf-4890-b17b-5f6fc80e0d99';
begin
  if not exists (
    select 1
    from public.loan_staff
    where id = v_george_id
      and business_id = v_business_id
      and trim(regexp_replace(name, '\s+', ' ', 'g')) = 'George Majwa'
      and role like '%loan_officer%'
      and is_active = true
  ) then
    raise exception 'Expected active loan officer George Majwa was not found';
  end if;

  update public.loan_clients
     set loan_officer_id = v_george_id
   where business_id = v_business_id
     and loan_officer_id = v_former_officer_id;

  update public.loan_applications
     set loan_officer_id = v_george_id
   where business_id = v_business_id
     and loan_officer_id = v_former_officer_id;

  update public.loans
     set loan_officer_id = v_george_id
   where business_id = v_business_id
     and loan_officer_id = v_former_officer_id;
end $$;

create or replace function public.bripta_canonicalize_loan_officer()
returns trigger
language plpgsql
as $$
begin
  if new.business_id = 'BIZ-B3F5E5D9' then
    new.loan_officer_id := case new.loan_officer_id
      when '1fb41aa9-e878-400e-a959-54c6a8e794a5'::uuid
        then 'd1479025-fad1-4178-a147-a0faf97764ac'::uuid
      when '910139c3-36b8-49bf-915b-764d7095a3ec'::uuid
        then '74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'::uuid
      else new.loan_officer_id
    end;
  end if;
  return new;
end;
$$;

commit;

select 'loan_clients' as source, count(*) as still_assigned_to_former
from public.loan_clients
where business_id = 'BIZ-B3F5E5D9'
  and loan_officer_id = '910139c3-36b8-49bf-915b-764d7095a3ec'
union all
select 'loan_applications', count(*)
from public.loan_applications
where business_id = 'BIZ-B3F5E5D9'
  and loan_officer_id = '910139c3-36b8-49bf-915b-764d7095a3ec'
union all
select 'loans', count(*)
from public.loans
where business_id = 'BIZ-B3F5E5D9'
  and loan_officer_id = '910139c3-36b8-49bf-915b-764d7095a3ec';
