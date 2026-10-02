-- Bripta only. Restore the access needed to record a payment and update its
-- loan and schedule. No existing financial records are changed by this script.
-- The existing restrictive branch policies remain in force. Safe to rerun.
begin;

drop policy if exists bripta_manual_repayments_insert on public.loan_repayments;
create policy bripta_manual_repayments_insert on public.loan_repayments
as permissive for insert to authenticated
with check (
  business_id='BIZ-B3F5E5D9'
  and (
    public.bripta_has_role('admin')
    or public.bripta_has_role('branch_manager')
    or public.bripta_has_role('cashier')
  )
  and exists (
    select 1 from public.loans l
    where l.id=loan_id and l.business_id=business_id and l.branch_id=branch_id
  )
);

-- INSERT ... RETURNING id and receipt display also require SELECT access.
drop policy if exists bripta_manual_repayments_cashier_read on public.loan_repayments;
create policy bripta_manual_repayments_cashier_read on public.loan_repayments
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
);

-- Cashiers need to find the correct branch loan and apply the payment to it.
drop policy if exists bripta_manual_loans_cashier_read on public.loans;
create policy bripta_manual_loans_cashier_read on public.loans
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
);

drop policy if exists bripta_manual_clients_cashier_read on public.loan_clients;
create policy bripta_manual_clients_cashier_read on public.loan_clients
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
);

drop policy if exists bripta_manual_schedules_cashier_read on public.loan_schedules;
create policy bripta_manual_schedules_cashier_read on public.loan_schedules
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
);

drop policy if exists bripta_manual_loans_cashier_update on public.loans;
create policy bripta_manual_loans_cashier_update on public.loans
as permissive for update to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
)
with check (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
);

drop policy if exists bripta_manual_schedules_cashier_update on public.loan_schedules;
create policy bripta_manual_schedules_cashier_update on public.loan_schedules
as permissive for update to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
)
with check (
  business_id='BIZ-B3F5E5D9'
  and public.bripta_has_role('cashier')
  and branch_id=(public.bripta_current_staff()).branch_id
);

commit;

-- Verification: all rows should read true in both final columns.
with required(table_name,operation,expected_policy) as (values
  ('loan_repayments','INSERT','bripta_manual_repayments_insert'),
  ('loan_repayments','SELECT','bripta_manual_repayments_cashier_read'),
  ('loans','SELECT','bripta_manual_loans_cashier_read'),
  ('loans','UPDATE','bripta_manual_loans_cashier_update'),
  ('loan_schedules','SELECT','bripta_manual_schedules_cashier_read'),
  ('loan_schedules','UPDATE','bripta_manual_schedules_cashier_update'),
  ('loan_clients','SELECT','bripta_manual_clients_cashier_read')
)
select r.table_name,r.operation,
  has_table_privilege('authenticated',format('public.%I',r.table_name),r.operation) as table_grant_present,
  exists(select 1 from pg_policies p where p.schemaname='public'
    and p.tablename=r.table_name and p.cmd=r.operation
    and p.permissive='PERMISSIVE' and p.policyname=r.expected_policy) as policy_present
from required r order by r.table_name,r.operation;
