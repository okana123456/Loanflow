-- Read-only. One result set for the Bripta Supabase SQL Editor.
-- No branch assignments, repayments or financial figures are changed.
with branch_counts as (
  select 'clients'::text as source, branch_id, count(*)::bigint as rows,
         null::numeric as amount
  from public.loan_clients where business_id='BIZ-B3F5E5D9' group by branch_id
  union all
  select 'active_loans', branch_id, count(*)::bigint, sum(outstanding_balance)
  from public.loans where business_id='BIZ-B3F5E5D9' and status='active' group by branch_id
  union all
  select 'all_loans', branch_id, count(*)::bigint, null::numeric
  from public.loans where business_id='BIZ-B3F5E5D9' group by branch_id
  union all
  select 'repayments', branch_id, count(*)::bigint, sum(amount)
  from public.loan_repayments where business_id='BIZ-B3F5E5D9' group by branch_id
  union all
  select 'applications', branch_id, count(*)::bigint, null::numeric
  from public.loan_applications where business_id='BIZ-B3F5E5D9' group by branch_id
),
summary as (
  select 'branch_totals'::text as check_type,c.source,
         coalesce(b.name,'UNASSIGNED')::text as branch,
         c.rows,c.amount
  from branch_counts c left join public.bripta_branches b on b.id=c.branch_id
  union all
  select 'repayment_branch_mismatch','repayments',
         'Saved payment branch differs from loan',
         count(*)::bigint,coalesce(sum(r.amount),0)
  from public.loan_repayments r join public.loans l on l.id=r.loan_id
  where r.business_id='BIZ-B3F5E5D9' and r.branch_id is distinct from l.branch_id
  union all
  select 'officer_active_portfolio',s.name,coalesce(b.name,'UNASSIGNED'),
         count(*)::bigint,coalesce(sum(l.outstanding_balance),0)
  from public.loans l
  join public.loan_staff s on s.id=l.loan_officer_id
  left join public.bripta_branches b on b.id=l.branch_id
  where l.business_id='BIZ-B3F5E5D9' and l.status='active'
  group by s.name,b.name
)
select check_type,source,branch,rows,amount
from summary
order by check_type,source,branch;
