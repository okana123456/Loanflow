-- Restore read access for Bripta's loans, schedules and repayments.
-- Existing restrictive branch and officer policies still apply.
-- No financial or staff records are changed.
begin;

grant select on public.loans, public.loan_schedules, public.loan_repayments to authenticated;

drop policy if exists bripta_loans_business_read on public.loans;
create policy bripta_loans_business_read on public.loans
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and loan_officer_id=(public.bripta_current_staff()).id)
  )
);

drop policy if exists bripta_schedules_business_read on public.loan_schedules;
create policy bripta_schedules_business_read on public.loan_schedules
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and exists (
      select 1 from public.loans l
      where l.id=loan_id and l.loan_officer_id=(public.bripta_current_staff()).id
    ))
  )
);

drop policy if exists bripta_repayments_business_read on public.loan_repayments;
create policy bripta_repayments_business_read on public.loan_repayments
as permissive for select to authenticated
using (
  business_id='BIZ-B3F5E5D9'
  and (
    public.bripta_has_role('admin')
    or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
    or (public.bripta_has_role('loan_officer') and exists (
      select 1 from public.loans l
      where l.id=loan_id and l.loan_officer_id=(public.bripta_current_staff()).id
    ))
  )
);

commit;

-- Read-only checks. Send these results back before any Brian staff repair.
select 'bripta_staff' as check_name, count(*)::numeric as result from public.loan_staff where business_id='BIZ-B3F5E5D9'
union all select 'bripta_loans', count(*) from public.loans where business_id='BIZ-B3F5E5D9'
union all select 'bripta_repayments', count(*) from public.loan_repayments where business_id='BIZ-B3F5E5D9'
union all select 'brian_officer_staff_row', count(*) from public.loan_staff where id='d1479025-fad1-4178-a147-a0faf97764ac'
union all select 'brian_officer_loans', count(*) from public.loans where business_id='BIZ-B3F5E5D9' and loan_officer_id='d1479025-fad1-4178-a147-a0faf97764ac';

select id,name,role,business_id,branch_id,is_active,email
from public.loan_staff
where id='d1479025-fad1-4178-a147-a0faf97764ac'
   or (business_id='BIZ-B3F5E5D9' and lower(name) like '%brian%');
