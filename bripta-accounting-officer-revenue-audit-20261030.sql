-- Bripta only. Read-only audit for October 2026; change the dates below if needed.
-- Shows the saved officer on each revenue source, by the record's own branch.
-- No financial record is updated by this query.
with params as (
  select 'BIZ-B3F5E5D9'::text as business_id,
         date '2026-10-01' as period_start,
         date '2026-11-01' as period_end
), revenue as (
  select 'registration'::text as source, c.branch_id, c.loan_officer_id as saved_officer_id,
         c.id::text as source_id,
         coalesce((regexp_match(coalesce(c.notes,''),'\[REG_FEE:([0-9]+(?:\.[0-9]+)?):'))[1]::numeric,200) as amount
  from public.loan_clients c cross join params p
  where c.business_id=p.business_id
    and (c.created_at at time zone 'Africa/Nairobi')::date>=p.period_start
    and (c.created_at at time zone 'Africa/Nairobi')::date<p.period_end
  union all
  select 'processing',l.branch_id,l.loan_officer_id,l.id::text,
         coalesce(l.processing_fee,0)::numeric
  from public.loans l cross join params p
  where l.business_id=p.business_id
    and l.disbursement_date::date>=p.period_start
    and l.disbursement_date::date<p.period_end
  union all
  select 'interest',coalesce(l.branch_id,r.branch_id),l.loan_officer_id,r.id::text,
         case when coalesce(r.interest_portion,0)>0 then r.interest_portion::numeric
              when coalesce(l.total_payable,0)>0 and coalesce(l.total_interest,0)>0
              then round(greatest(0,coalesce(r.loan_portion,r.amount,0)-coalesce(r.penalty_portion,0))::numeric
                         * l.total_interest::numeric/l.total_payable::numeric,2)
              else 0 end
  from public.loan_repayments r join public.loans l on l.id=r.loan_id
  cross join params p
  where r.business_id=p.business_id
    and ((case when (lower(coalesce(r.payment_method,'')) like '%mpesa%'
                          or lower(coalesce(r.payment_method,'')) like '%m-pesa%')
                     and coalesce(r.notes,'') !~ '\[BRIPTA_RECOVERY_20261002\]|\[MANUAL_ENTRY\]|Matched by borrower phone account'
                then coalesce(r.created_at,r.payment_date)
                else coalesce(r.payment_date,r.created_at) end)
         at time zone 'Africa/Nairobi')::date>=p.period_start
    and ((case when (lower(coalesce(r.payment_method,'')) like '%mpesa%'
                          or lower(coalesce(r.payment_method,'')) like '%m-pesa%')
                     and coalesce(r.notes,'') !~ '\[BRIPTA_RECOVERY_20261002\]|\[MANUAL_ENTRY\]|Matched by borrower phone account'
                then coalesce(r.created_at,r.payment_date)
                else coalesce(r.payment_date,r.created_at) end)
         at time zone 'Africa/Nairobi')::date<p.period_end
), labelled as (
  select coalesce(b.name,'[missing branch]') as branch,
         r.source,r.source_id,r.amount,r.saved_officer_id,
         coalesce(s.name,'[unassigned or missing staff]') as saved_officer,
         case when r.branch_id='00000000-0000-4000-8000-000000000001'::uuid
                    and (s.id is null or s.branch_id is distinct from r.branch_id
                         or coalesce(s.is_active,true)=false)
              then 'Brian Ochieng (Migori display allocation)'
              else coalesce(s.name,'[unassigned or missing staff]') end as expected_display_officer
  from revenue r
  left join public.bripta_branches b on b.id=r.branch_id and b.business_id=(select business_id from params)
  left join public.loan_staff s on s.id=r.saved_officer_id and s.business_id=(select business_id from params)
)
select branch,saved_officer,expected_display_officer,
       count(*) filter (where source='registration') as registration_records,
       coalesce(sum(amount) filter (where source='registration'),0) as registration_kes,
       count(*) filter (where source='processing') as processing_records,
       coalesce(sum(amount) filter (where source='processing'),0) as processing_kes,
       count(*) filter (where source='interest') as repayment_records,
       coalesce(sum(amount) filter (where source='interest'),0) as interest_kes,
       coalesce(sum(amount),0) as total_revenue_kes
from labelled
group by branch,saved_officer,expected_display_officer
order by branch,expected_display_officer,saved_officer;
