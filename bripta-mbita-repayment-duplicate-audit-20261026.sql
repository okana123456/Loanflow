-- Read-only Mbita repayment audit for Bripta. Changes no balances or records.
-- One result set: each Mbita repayment and checks for reused M-Pesa references,
-- repeated client identity numbers and multiple loans per client.
with mbita as (
  select id from public.bripta_branches
  where business_id='BIZ-B3F5E5D9'
    and upper(name) like '%MBITA%'
), payments as (
  select r.id as repayment_id,r.loan_id,r.amount,r.receipt_no,
         r.payment_reference,r.payment_date,r.created_at,
         coalesce(nullif(to_jsonb(r)->>'sender_name',''),
                  nullif(to_jsonb(r)->>'payer_name','')) as sender_name,
         upper(trim(coalesce(nullif(r.payment_reference,''),r.receipt_no,''))) as transaction_code,
         l.loan_no,l.client_id,c.full_name,c.id_number,c.phone
  from public.loan_repayments r
  join public.loans l on l.id=r.loan_id
  join public.loan_clients c on c.id=l.client_id
  where r.business_id='BIZ-B3F5E5D9'
    and r.branch_id in (select id from mbita)
)
select p.payment_date,p.created_at,p.transaction_code,p.receipt_no,
       p.amount,p.sender_name,p.full_name as client_name,p.phone as client_phone,
       p.id_number,p.loan_no,p.loan_id,p.repayment_id,
       (select count(*) from public.loan_repayments x
        where x.business_id='BIZ-B3F5E5D9'
          and upper(trim(coalesce(nullif(x.payment_reference,''),x.receipt_no,'')))=p.transaction_code
       ) as rows_with_same_reference,
       (select count(*) from public.loans x
        where x.business_id='BIZ-B3F5E5D9' and x.client_id=p.client_id
       ) as loans_for_this_client,
       (select count(*) from public.loan_clients x
        where x.business_id='BIZ-B3F5E5D9'
          and nullif(trim(x.id_number),'') is not null
          and trim(x.id_number)=trim(p.id_number)
       ) as clients_with_same_id_number
from payments p
order by p.payment_date desc,p.created_at desc;
