-- Bripta only: restore the permissive RLS policies required by the existing
-- restrictive business/branch/officer policies. No financial rows are changed.
-- Safe to run again. Keep the existing restrictive policies in place.
begin;

drop policy if exists bripta_clients_issuance_read on public.loan_clients;
create policy bripta_clients_issuance_read on public.loan_clients
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
);

drop policy if exists bripta_clients_issuance_insert on public.loan_clients;
create policy bripta_clients_issuance_insert on public.loan_clients
as permissive for insert to authenticated
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
);

drop policy if exists bripta_clients_issuance_update on public.loan_clients;
create policy bripta_clients_issuance_update on public.loan_clients
as permissive for update to authenticated
using (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
)
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
);

drop policy if exists bripta_applications_issuance_read on public.loan_applications;
create policy bripta_applications_issuance_read on public.loan_applications
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
);

drop policy if exists bripta_applications_issuance_insert on public.loan_applications;
create policy bripta_applications_issuance_insert on public.loan_applications
as permissive for insert to authenticated
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
);

drop policy if exists bripta_applications_issuance_update on public.loan_applications;
create policy bripta_applications_issuance_update on public.loan_applications
as permissive for update to authenticated
using (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
)
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
);

drop policy if exists bripta_loans_issuance_insert on public.loans;
create policy bripta_loans_issuance_insert on public.loans
as permissive for insert to authenticated
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
);

drop policy if exists bripta_loans_issuance_update on public.loans;
create policy bripta_loans_issuance_update on public.loans
as permissive for update to authenticated
using (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
)
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
);

drop policy if exists bripta_schedules_issuance_insert on public.loan_schedules;
create policy bripta_schedules_issuance_insert on public.loan_schedules
as permissive for insert to authenticated
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
);

drop policy if exists bripta_schedules_issuance_update on public.loan_schedules;
create policy bripta_schedules_issuance_update on public.loan_schedules
as permissive for update to authenticated
using (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
)
with check (
  business_id='BIZ-B3F5E5D9' and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
  )
);

-- A loan is not fully issued until its schedule exists. Preserve the original
-- insert-trigger behavior for other businesses in this shared project.
create or replace function public.bripta_mark_linked_application_disbursed()
returns trigger language plpgsql security definer set search_path=public as $$
begin
  if new.business_id='BIZ-B3F5E5D9' then return new; end if;
  if new.application_id is not null then
    update public.loan_applications
       set status='disbursed'
     where id=new.application_id and business_id=new.business_id
       and client_id=new.client_id and status is distinct from 'disbursed';
  end if;
  return new;
end;
$$;

commit;

-- Read-only verification. All rows should say true for both columns.
with required(table_name,operation) as (values
  ('loan_clients','SELECT'),('loan_clients','INSERT'),('loan_clients','UPDATE'),
  ('loan_applications','SELECT'),('loan_applications','INSERT'),('loan_applications','UPDATE'),
  ('loans','SELECT'),('loans','INSERT'),('loans','UPDATE'),
  ('loan_schedules','SELECT'),('loan_schedules','INSERT'),('loan_schedules','UPDATE')
)
select r.table_name,r.operation,
  has_table_privilege('authenticated',format('public.%I',r.table_name),r.operation) as table_grant_present,
  exists(select 1 from pg_policies p where p.schemaname='public'
    and p.tablename=r.table_name and p.cmd in (r.operation,'ALL')
    and p.permissive='PERMISSIVE' and p.policyname like 'bripta_%') as permissive_policy_present
from required r order by r.table_name,r.operation;
