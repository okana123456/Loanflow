-- Restore Bripta portfolio visibility after the multi-branch RLS upgrade.
-- Safe to run more than once. This changes access checks only; it does not
-- update staff, clients, loans, repayments, balances, or accounting figures.

begin;

create or replace function public.bripta_can_access_branch(p_branch uuid)
returns boolean
language sql
stable
security definer
set search_path=public
as $$
  select
    public.bripta_has_role('admin')
    or (
      (public.bripta_current_staff()).id is not null
      and (public.bripta_current_staff()).branch_id = p_branch
    )
$$;

revoke all on function public.bripta_can_access_branch(uuid) from public, anon;
grant execute on function public.bripta_can_access_branch(uuid) to authenticated;

commit;

-- Verification only: these figures are read, never recalculated or changed.
select 'active_staff' as check_name, count(*)::numeric as result
from public.loan_staff
where business_id='BIZ-B3F5E5D9' and coalesce(is_active,true)
union all
select 'active_loan_officers', count(*)::numeric
from public.loan_staff
where business_id='BIZ-B3F5E5D9'
  and coalesce(is_active,true)
  and 'loan_officer'=any(regexp_split_to_array(lower(coalesce(role,'')),'\s*,\s*'))
union all
select 'active_branch_managers', count(*)::numeric
from public.loan_staff
where business_id='BIZ-B3F5E5D9'
  and coalesce(is_active,true)
  and 'branch_manager'=any(regexp_split_to_array(lower(coalesce(role,'')),'\s*,\s*'))
union all
select 'clients_in_head_office', count(*)::numeric
from public.loan_clients
where business_id='BIZ-B3F5E5D9'
  and branch_id='00000000-0000-4000-8000-000000000001'
union all
select 'loans_in_head_office', count(*)::numeric
from public.loans
where business_id='BIZ-B3F5E5D9'
  and branch_id='00000000-0000-4000-8000-000000000001';
