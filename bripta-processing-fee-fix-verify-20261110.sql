-- READ ONLY. Verify the Bripta processing-fee allocation after running the fix.
with fixed as (
  select a.*,x.branch_id,x.amount_available,c.loan_id,c.amount as charge_amount,
    c.amount_paid,l.processing_fee,l.processing_fee_paid
  from public.bripta_excess_allocations a
  join public.bripta_excess_ledger x on x.id=a.excess_id
  join public.bripta_charges c on c.id=a.charge_id
  join public.loans l on l.id=c.loan_id
  where a.business_id='BIZ-B3F5E5D9' and a.action='charge_payment'
    and c.charge_type='processing_fee'
    and a.note like 'Same-loan client excess applied to outstanding processing fee%'
)
select b.name as branch,count(f.id) as fee_allocations,
  coalesce(sum(f.amount),0) as processing_fee_settled_from_excess,
  count(f.id) filter(where f.amount<=0 or f.amount_paid>f.charge_amount
    or f.processing_fee_paid>f.processing_fee) as invalid_fee_rows,
  (select count(*) from public.bripta_excess_ledger x
   where x.business_id='BIZ-B3F5E5D9' and x.branch_id=b.id
     and x.amount_available>0) as excess_rows_still_available,
  (select coalesce(sum(x.amount_available),0) from public.bripta_excess_ledger x
   where x.business_id='BIZ-B3F5E5D9' and x.branch_id=b.id) as excess_still_available
from public.bripta_branches b left join fixed f on f.branch_id=b.id
where b.business_id='BIZ-B3F5E5D9'
group by b.id,b.name order by b.name;
