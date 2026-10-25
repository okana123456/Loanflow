-- Read-only. Run in the Bripta Supabase SQL editor and share the single output row.
-- No client, loan, repayment, ledger, or SMS record is changed.
with george as (
  select id,name,branch_id from public.loan_staff
  where business_id='BIZ-B3F5E5D9'
    and lower(name) like '%george%' and lower(name) like '%majwa%'
), branches as (
  select id,name,created_at from public.bripta_branches
  where business_id='BIZ-B3F5E5D9'
), mbita as (
  select min(created_at) as created_at from branches where name ilike '%mbita%'
), recent_clients as (
  select c.id,c.full_name,c.created_at,c.created_by,c.loan_officer_id,c.branch_id,
         b.name as branch_name,
         (c.created_by in (select id from george)) as created_by_george
  from public.loan_clients c
  left join branches b on b.id=c.branch_id
  where c.business_id='BIZ-B3F5E5D9'
    and c.loan_officer_id in (select id from george)
    and c.created_at >= coalesce((select created_at from mbita),now()-interval '30 days')
), recent_loans as (
  select l.id,l.loan_no,l.created_at,l.disbursement_date,l.status,
         l.loan_officer_id,l.branch_id,l.client_id,c.full_name,
         c.branch_id as client_branch_id,a.branch_id as application_branch_id,
         b.name as loan_branch_name,
         (select count(*) from public.loan_schedules s where s.loan_id=l.id and s.branch_id is distinct from l.branch_id) as schedules_different_branch,
         (select count(*) from public.loan_repayments r where r.loan_id=l.id and r.branch_id is distinct from l.branch_id) as repayments_different_branch,
         (select count(*) from public.bripta_charges x where x.loan_id=l.id and x.branch_id is distinct from l.branch_id) as charges_different_branch,
         (select count(*) from public.bripta_accounting_entries e where e.loan_id=l.id and e.branch_id is distinct from l.branch_id) as accounting_entries_different_branch
  from public.loans l
  join public.loan_clients c on c.id=l.client_id
  left join public.loan_applications a on a.id=l.application_id
  left join branches b on b.id=l.branch_id
  where l.business_id='BIZ-B3F5E5D9'
    and l.loan_officer_id in (select id from george)
    and l.created_at >= coalesce((select created_at from mbita),now()-interval '30 days')
)
select
  (select jsonb_agg(to_jsonb(g) order by g.name) from george g) as george_staff,
  (select jsonb_agg(to_jsonb(b) order by b.name) from branches b) as branches,
  (select jsonb_agg(to_jsonb(c) order by c.created_at desc) from (select * from recent_clients order by created_at desc limit 100) c) as george_recent_clients,
  (select jsonb_agg(to_jsonb(l) order by l.created_at desc) from (select * from recent_loans order by created_at desc limit 100) l) as george_recent_loans,
  (select jsonb_agg(to_jsonb(x) order by x.branch_name) from (
    select coalesce(b.name,'Unassigned') as branch_name,
           count(*) as all_george_loans,
           count(*) filter(where l.created_at >= coalesce((select created_at from mbita),now()-interval '30 days')) as loans_after_mbita_created
    from public.loans l left join branches b on b.id=l.branch_id
    where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id in(select id from george)
    group by b.name
  ) x) as george_loan_counts_by_branch;
