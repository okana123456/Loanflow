-- Add the missing PERMISSIVE read policy. Existing restrictive policies
-- still apply. No staff roles, assignments or financial records are updated.
begin;

grant select on public.loan_staff to authenticated;
drop policy if exists bripta_staff_business_read on public.loan_staff;
create policy bripta_staff_business_read on public.loan_staff
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and (
    public.bripta_has_role('admin')
    or (
      public.bripta_has_role('branch_manager')
      and branch_id=(public.bripta_current_staff()).branch_id
    )
    or id=(public.bripta_current_staff()).id
  )
);

commit;

-- Confirm installation. Login testing must use the actual staff session.
select policyname, permissive, cmd, qual
from pg_policies
where schemaname='public' and tablename='loan_staff'
order by policyname;
