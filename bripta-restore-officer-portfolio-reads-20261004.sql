-- Bripta only. Restore loan-officer reads for their assigned loan portfolio.
-- This changes access policies only; no clients, loans, payments, schedules,
-- balances, SMS, or accounting records are inserted or updated.
-- Safe to run more than once.
begin;

grant select on public.loan_clients, public.loans, public.loan_schedules,
  public.loan_repayments, public.loan_staff to authenticated;

drop policy if exists bripta_officer_portfolio_loans_read on public.loans;
create policy bripta_officer_portfolio_loans_read on public.loans
as permissive for select to authenticated
using (
  business_id = 'BIZ-B3F5E5D9'
  and public.bripta_has_role('loan_officer')
  and loan_officer_id = (public.bripta_current_staff()).id
  and branch_id = (public.bripta_current_staff()).branch_id
);

drop policy if exists bripta_officer_portfolio_schedules_read on public.loan_schedules;
create policy bripta_officer_portfolio_schedules_read on public.loan_schedules
as permissive for select to authenticated
using (
  business_id = 'BIZ-B3F5E5D9'
  and public.bripta_has_role('loan_officer')
  and branch_id = (public.bripta_current_staff()).branch_id
  and exists (
    select 1 from public.loans l
    where l.id = loan_id
      and l.business_id = business_id
      and l.branch_id = branch_id
      and l.loan_officer_id = (public.bripta_current_staff()).id
  )
);

drop policy if exists bripta_officer_portfolio_repayments_read on public.loan_repayments;
create policy bripta_officer_portfolio_repayments_read on public.loan_repayments
as permissive for select to authenticated
using (
  business_id = 'BIZ-B3F5E5D9'
  and public.bripta_has_role('loan_officer')
  and branch_id = (public.bripta_current_staff()).branch_id
  and exists (
    select 1 from public.loans l
    where l.id = loan_id
      and l.business_id = business_id
      and l.branch_id = branch_id
      and l.loan_officer_id = (public.bripta_current_staff()).id
  )
);

commit;

-- Expected: three rows, both booleans true. Existing restrictive branch and
-- loan-officer policies continue to apply in addition to these policies.
with required(table_name, policy_name) as (values
  ('loans', 'bripta_officer_portfolio_loans_read'),
  ('loan_schedules', 'bripta_officer_portfolio_schedules_read'),
  ('loan_repayments', 'bripta_officer_portfolio_repayments_read')
)
select r.table_name,
  has_table_privilege('authenticated', format('public.%I', r.table_name), 'SELECT') as select_granted,
  exists (
    select 1 from pg_policies p
    where p.schemaname = 'public' and p.tablename = r.table_name
      and p.policyname = r.policy_name and p.cmd = 'SELECT'
      and p.permissive = 'PERMISSIVE'
  ) as officer_read_policy_present
from required r order by r.table_name;

-- Read-only portfolio check. It confirms whether the existing records really
-- belong to each officer and to their branch. No data is changed.
select s.name as officer, s.role, s.branch_id as staff_branch,
  count(distinct l.id) as assigned_loans,
  count(distinct r.id) as recorded_repayments,
  count(distinct sc.id) as schedules,
  count(distinct l.id) filter (where l.branch_id is distinct from s.branch_id) as loans_in_other_branch,
  count(distinct r.id) filter (where r.branch_id is distinct from l.branch_id) as repayments_in_other_branch
from public.loan_staff s
left join public.loans l on l.business_id = s.business_id and l.loan_officer_id = s.id
left join public.loan_repayments r on r.business_id = s.business_id and r.loan_id = l.id
left join public.loan_schedules sc on sc.business_id = s.business_id and sc.loan_id = l.id
where s.business_id = 'BIZ-B3F5E5D9'
  and 'loan_officer' = any(regexp_split_to_array(lower(coalesce(s.role, '')), '\s*,\s*'))
group by s.id, s.name, s.role, s.branch_id
order by s.name;
