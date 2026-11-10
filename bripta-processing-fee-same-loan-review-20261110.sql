-- READ ONLY. Review whether excess was paid against the same loan that has
-- an unpaid processing fee. No financial value or ownership is changed.
with candidates as (
  select x.id as excess_id,x.payment_reference,x.created_at,x.source,
    x.business_id,x.branch_id,x.client_id,x.loan_id,x.repayment_id,
    x.amount_original,x.amount_available,
    l.loan_no,l.disbursement_date,l.processing_fee,l.processing_fee_paid,
    c.id as charge_id,c.amount as charge_amount,c.amount_paid as charge_paid,
    c.status as charge_status,
    r.amount as repayment_amount,r.loan_portion,r.credit_portion,r.payment_date,
    case when c.id is not null and x.loan_id=c.loan_id and x.client_id=c.client_id
      and x.business_id=c.business_id and x.branch_id=c.branch_id
      and x.amount_available>0 and c.amount-c.amount_paid>0
      and x.repayment_id=r.id and r.loan_id=x.loan_id
      and abs(x.amount_original-r.credit_portion)<=0.01
      then true else false end as same_loan_candidate
  from public.bripta_excess_ledger x
  left join public.loans l on l.id=x.loan_id
  left join public.loan_repayments r on r.id=x.repayment_id
  left join public.bripta_charges c on c.loan_id=x.loan_id
    and c.business_id=x.business_id and c.charge_type='processing_fee'
  where x.business_id='BIZ-B3F5E5D9' and x.amount_available>0
), summary as (
  select b.name as branch,
    count(*) as available_excess_rows,sum(c.amount_available) as available_excess,
    count(*) filter(where same_loan_candidate) as same_loan_candidate_rows,
    sum(least(c.amount_available,c.charge_amount-c.charge_paid))
      filter(where same_loan_candidate) as same_loan_max_fee_allocation,
    count(*) filter(where same_loan_candidate and c.amount_available=c.charge_amount-c.charge_paid)
      as exact_remaining_fee_rows,
    count(*) filter(where c.charge_id is null) as no_same_loan_charge_rows,
    count(*) filter(where c.loan_id is not null and c.charge_id is not null
      and c.charge_amount=c.charge_paid) as already_paid_charge_rows
  from candidates c left join public.bripta_branches b on b.id=c.branch_id
  group by b.name
)
select 'branch_summary'::text as section,s.branch,null::text as reference,
  jsonb_build_object('available_excess_rows',s.available_excess_rows,
    'available_excess',s.available_excess,
    'same_loan_candidate_rows',s.same_loan_candidate_rows,
    'same_loan_max_fee_allocation',coalesce(s.same_loan_max_fee_allocation,0),
    'exact_remaining_fee_rows',s.exact_remaining_fee_rows,
    'no_same_loan_charge_rows',s.no_same_loan_charge_rows,
    'already_paid_charge_rows',s.already_paid_charge_rows) as details
from summary s
union all
select 'candidate_sample',b.name,coalesce(c.payment_reference,c.excess_id::text),
  jsonb_build_object('excess_id',c.excess_id,'loan_no',c.loan_no,
    'charge_id',c.charge_id,'excess_available',c.amount_available,
    'fee_due',c.charge_amount-c.charge_paid,'loan_fee_paid',c.processing_fee_paid,
    'repayment_amount',c.repayment_amount,'loan_portion',c.loan_portion,
    'credit_portion',c.credit_portion,'repayment_date',c.payment_date,
    'disbursement_date',c.disbursement_date) from candidates c
left join public.bripta_branches b on b.id=c.branch_id
where c.same_loan_candidate
order by section,branch,reference
limit 45;
