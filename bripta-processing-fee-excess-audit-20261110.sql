-- READ ONLY. Bripta business only. No balances, repayments, fees or excess are changed.
-- This distinguishes the processing fee charged at disbursement from cash
-- collected for that fee and from refundable/client-owned excess.
with params as (
  select 'BIZ-B3F5E5D9'::text as business_id
), branches as (
  select id,name from public.bripta_branches b join params p on p.business_id=b.business_id
), loan_fees as (
  select l.branch_id,count(*) as loans,
    sum(coalesce(l.processing_fee,0)) as fee_charged,
    sum(coalesce(l.processing_fee_paid,0)) as loan_fee_paid
  from public.loans l join params p on p.business_id=l.business_id
  group by l.branch_id
), charge_fees as (
  select c.branch_id,count(*) as charge_rows,
    sum(c.amount) as charge_amount,sum(c.amount_paid) as charge_paid,
    count(*) filter(where c.status in ('pending','partially_paid')) as unpaid_charge_rows
  from public.bripta_charges c join params p on p.business_id=c.business_id
  where c.charge_type='processing_fee' group by c.branch_id
), repayment_fees as (
  select r.branch_id,count(*) filter(where coalesce(r.processing_fee_portion,0)>0) as fee_payment_rows,
    sum(coalesce(r.processing_fee_portion,0)) as fee_payment_portion,
    count(*) filter(where coalesce(r.credit_portion,0)>0) as overpayment_rows,
    sum(coalesce(r.credit_portion,0)) as overpayment_amount
  from public.loan_repayments r join params p on p.business_id=r.business_id
  group by r.branch_id
), excess as (
  select x.branch_id,count(*) as excess_rows,
    sum(x.amount_original) as excess_original,sum(x.amount_available) as excess_available
  from public.bripta_excess_ledger x join params p on p.business_id=x.business_id
  group by x.branch_id
), allocations as (
  select x.branch_id,count(*) as applied_to_fee_rows,sum(a.amount) as applied_to_fee
  from public.bripta_excess_allocations a
  join public.bripta_excess_ledger x on x.id=a.excess_id
  join public.bripta_charges c on c.id=a.charge_id
  join params p on p.business_id=a.business_id
  where a.action='charge_payment' and c.charge_type='processing_fee'
  group by x.branch_id
)
select 'branch_summary'::text as section,b.name as branch,null::text as reference,
  jsonb_build_object('loans',coalesce(l.loans,0),
    'processing_charged_on_loans',coalesce(l.fee_charged,0),
    'processing_paid_on_loans',coalesce(l.loan_fee_paid,0),
    'processing_charge_rows',coalesce(c.charge_rows,0),
    'processing_charged_in_charge_ledger',coalesce(c.charge_amount,0),
    'processing_paid_in_charge_ledger',coalesce(c.charge_paid,0),
    'processing_fee_repayment_rows',coalesce(r.fee_payment_rows,0),
    'processing_fee_repayment_portion',coalesce(r.fee_payment_portion,0),
    'overpayment_rows',coalesce(r.overpayment_rows,0),
    'overpayment_amount',coalesce(r.overpayment_amount,0),
    'excess_rows',coalesce(e.excess_rows,0),
    'excess_original',coalesce(e.excess_original,0),
    'excess_still_available',coalesce(e.excess_available,0),
    'excess_applied_to_processing_fee',coalesce(a.applied_to_fee,0)) as details
from branches b
left join loan_fees l on l.branch_id=b.id
left join charge_fees c on c.branch_id=b.id
left join repayment_fees r on r.branch_id=b.id
left join excess e on e.branch_id=b.id
left join allocations a on a.branch_id=b.id
union all
select 'recent_excess_with_unpaid_processing_charge',b.name,
  coalesce(x.payment_reference,x.id::text),
  jsonb_build_object('excess_id',x.id,'client',cl.full_name,'loan_no',l.loan_no,
    'excess_original',x.amount_original,'excess_available',x.amount_available,
    'processing_charge_due',c.amount-c.amount_paid,'charge_id',c.id,
    'source',x.source,'created_at',x.created_at)
from public.bripta_excess_ledger x
join params p on p.business_id=x.business_id
join public.loan_clients cl on cl.id=x.client_id
left join public.loans l on l.id=x.loan_id
left join branches b on b.id=x.branch_id
join public.bripta_charges c on c.business_id=x.business_id
  and c.client_id=x.client_id and c.charge_type='processing_fee'
  and c.status in ('pending','partially_paid')
where x.amount_available>0
  and c.id=(select c2.id from public.bripta_charges c2
    where c2.business_id=x.business_id and c2.client_id=x.client_id
      and c2.charge_type='processing_fee' and c2.status in ('pending','partially_paid')
    order by (c2.loan_id=x.loan_id) desc,c2.charge_date,c2.id limit 1)
order by section,branch,reference
limit 75;
