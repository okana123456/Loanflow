-- Bripta: keep Brian Obanda Ochieng as the audit actor, but never as the
-- portfolio loan officer. All officer attribution for that admin belongs to
-- loan officer Brian Ochieng.

begin;

do $$
declare
  v_business_id text := 'BIZ-B3F5E5D9';
  v_admin_id uuid := '1fb41aa9-e878-400e-a959-54c6a8e794a5';
  v_officer_id uuid := 'd1479025-fad1-4178-a147-a0faf97764ac';
begin
  if not exists (
    select 1 from public.loan_staff
    where id = v_admin_id and business_id = v_business_id
      and name = 'Brian Obanda Ochieng' and role like '%admin%'
  ) then
    raise exception 'Expected Bripta admin was not found';
  end if;

  if not exists (
    select 1 from public.loan_staff
    where id = v_officer_id and business_id = v_business_id
      and trim(regexp_replace(name, '\s+', ' ', 'g')) = 'Brian Ochieng'
      and role like '%loan_officer%'
  ) then
    raise exception 'Expected Bripta loan officer was not found';
  end if;

  update public.loan_clients
     set loan_officer_id = v_officer_id
   where business_id = v_business_id and loan_officer_id = v_admin_id;

  update public.loan_applications
     set loan_officer_id = v_officer_id
   where business_id = v_business_id and loan_officer_id = v_admin_id;

  update public.loans
     set loan_officer_id = v_officer_id
   where business_id = v_business_id and loan_officer_id = v_admin_id;
end $$;

create or replace function public.bripta_canonicalize_loan_officer()
returns trigger
language plpgsql
as $$
begin
  if new.business_id = 'BIZ-B3F5E5D9'
     and new.loan_officer_id = '1fb41aa9-e878-400e-a959-54c6a8e794a5'::uuid then
    new.loan_officer_id := 'd1479025-fad1-4178-a147-a0faf97764ac'::uuid;
  end if;
  return new;
end;
$$;

drop trigger if exists bripta_canonicalize_client_officer on public.loan_clients;
create trigger bripta_canonicalize_client_officer
before insert or update of loan_officer_id on public.loan_clients
for each row execute function public.bripta_canonicalize_loan_officer();

drop trigger if exists bripta_canonicalize_application_officer on public.loan_applications;
create trigger bripta_canonicalize_application_officer
before insert or update of loan_officer_id on public.loan_applications
for each row execute function public.bripta_canonicalize_loan_officer();

drop trigger if exists bripta_canonicalize_loan_officer on public.loans;
create trigger bripta_canonicalize_loan_officer
before insert or update of loan_officer_id on public.loans
for each row execute function public.bripta_canonicalize_loan_officer();

commit;

select 'loan_clients' as source, count(*) as still_assigned_to_admin
from public.loan_clients
where business_id = 'BIZ-B3F5E5D9'
  and loan_officer_id = '1fb41aa9-e878-400e-a959-54c6a8e794a5'
union all
select 'loan_applications', count(*)
from public.loan_applications
where business_id = 'BIZ-B3F5E5D9'
  and loan_officer_id = '1fb41aa9-e878-400e-a959-54c6a8e794a5'
union all
select 'loans', count(*)
from public.loans
where business_id = 'BIZ-B3F5E5D9'
  and loan_officer_id = '1fb41aa9-e878-400e-a959-54c6a8e794a5';
