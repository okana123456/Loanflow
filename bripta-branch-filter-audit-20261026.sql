-- Read-only Bripta branch audit. Run in the Bripta Supabase SQL Editor.
-- This changes no records or accounting figures.
with counts as (
  select 'clients' as source, branch_id, count(*) as records, null::numeric as amount
  from public.loan_clients where business_id='BIZ-B3F5E5D9' group by branch_id
  union all
  select 'active_loans', branch_id, count(*), sum(outstanding_balance)
  from public.loans where business_id='BIZ-B3F5E5D9' and status='active' group by branch_id
  union all
  select 'all_loans', branch_id, count(*), null::numeric
  from public.loans where business_id='BIZ-B3F5E5D9' group by branch_id
  union all
  select 'repayments', branch_id, count(*), sum(amount)
  from public.loan_repayments where business_id='BIZ-B3F5E5D9' group by branch_id
  union all
  select 'applications', branch_id, count(*), null::numeric
  from public.loan_applications where business_id='BIZ-B3F5E5D9' group by branch_id
)
select c.source, coalesce(b.name,'UNASSIGNED') as branch, c.branch_id,
       c.records, c.amount
from counts c left join public.bripta_branches b on b.id=c.branch_id
order by c.source, branch;

-- Repayments whose stored branch does not match their source loan.
select count(*) as repayments_with_wrong_branch,
       coalesce(sum(r.amount),0) as affected_amount
from public.loan_repayments r
join public.loans l on l.id=r.loan_id
where r.business_id='BIZ-B3F5E5D9'
  and r.branch_id is distinct from l.branch_id;

-- George can legitimately have a historical Migori portfolio after his staff
-- account moves to Mbita. This reports the records; it does not transfer them.
select s.name as officer, b.name as loan_branch, l.branch_id,
       count(*) as loans,
       count(*) filter (where l.status='active') as active_loans,
       coalesce(sum(l.outstanding_balance) filter (where l.status='active'),0) as active_outstanding
from public.loans l
join public.loan_staff s on s.id=l.loan_officer_id
left join public.bripta_branches b on b.id=l.branch_id
where l.business_id='BIZ-B3F5E5D9'
  and lower(s.name) like '%george%majwa%'
group by s.name,b.name,l.branch_id
order by loan_branch;
