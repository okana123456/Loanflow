-- READ ONLY. Candidate clients for Bripta M-Pesa callbacks since
-- 1 October 2026, 10:00 pm East Africa Time.
-- A phone match is a clue, NOT authority to apply a payment to that loan.
-- Confirm the intended borrower from the original payment instructions.

with pending as (
  select q.id as queue_id,
         q.created_at,
         q.trans_id,
         q.trans_amount,
         q.bill_ref_number,
         concat_ws(' ',nullif(q.first_name,''),nullif(q.middle_name,''),nullif(q.last_name,'')) as payer_name,
         q.msisdn as payer_phone,
         regexp_replace(coalesce(q.bill_ref_number::text,''),'[^0-9]','','g') as account_digits
  from public.mpesa_callback_queue q
  where q.created_at >= timestamptz '2026-10-01 22:00:00+03:00'
    and q.business_short_code::text = 'BIZ-B3F5E5D9'
    and coalesce(q.confirmed,false) = false
    and not exists (
      select 1 from public.loan_repayments r
      where r.business_id = 'BIZ-B3F5E5D9'
        and (r.payment_reference = q.trans_id or r.receipt_no = q.trans_id)
    )
), candidates as (
  select p.*,
         c.id as candidate_client_id,
         c.full_name as candidate_client_name,
         c.phone as candidate_phone,
         c.id_number as candidate_national_id,
         case
           when regexp_replace(coalesce(c.id_number::text,''),'[^0-9]','','g') = p.account_digits
             then 'national_id'
           else 'phone_only_review'
         end as match_reason,
         l.id as active_loan_id,
         l.loan_no as active_loan_no,
         l.outstanding_balance as active_loan_balance
  from pending p
  left join public.loan_clients c
    on c.business_id = 'BIZ-B3F5E5D9'
   and (
     (length(p.account_digits) between 5 and 12
      and regexp_replace(coalesce(c.id_number::text,''),'[^0-9]','','g') = p.account_digits)
     or (length(p.account_digits) >= 9
      and length(regexp_replace(coalesce(c.phone::text,''),'[^0-9]','','g')) >= 9
      and right(regexp_replace(coalesce(c.phone::text,''),'[^0-9]','','g'),9) = right(p.account_digits,9))
   )
  left join lateral (
    select l.id,l.loan_no,l.outstanding_balance
    from public.loans l
    where l.business_id = 'BIZ-B3F5E5D9'
      and l.client_id = c.id
      and l.status = 'active'
      and l.outstanding_balance > 0
    order by l.created_at desc
    limit 1
  ) l on true
)
select created_at,queue_id,trans_id,trans_amount,bill_ref_number,
       payer_name,payer_phone,match_reason,
       candidate_client_id,candidate_client_name,candidate_phone,
       candidate_national_id,active_loan_id,active_loan_no,active_loan_balance,
       count(candidate_client_id) over (partition by queue_id) as candidate_count
from candidates
order by created_at,trans_id,match_reason,candidate_client_name;
