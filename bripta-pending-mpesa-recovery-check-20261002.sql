-- READ ONLY. Inspect Bripta callbacks received after
-- 1 October 2026 at 10:00 pm Kenya time that have no repayment or SMS.
-- This does not confirm payments, allocate money, or send SMS.

with p as (
  select 'BIZ-B3F5E5D9'::text business_id,
         timestamptz '2026-10-01 22:00:00+03:00' since_at
),
config as (
  select s.mpesa_shortcode::text shortcode,s.mpesa_auto_confirm
  from public.loan_settings s cross join p
  where s.business_id=p.business_id
  limit 1
),
pending as (
  select q.*,
         regexp_replace(coalesce(q.bill_ref_number::text,''),'[^0-9]','','g') account_digits
  from public.mpesa_callback_queue q cross join p
  left join config c on true
  where q.created_at>=p.since_at
    and coalesce(q.confirmed,false)=false
    and (q.business_short_code::text=p.business_id
         or trim(q.business_short_code::text)=trim(c.shortcode))
)
select q.created_at as callback_at,
       q.id as queue_id,
       q.trans_id as transaction_code,
       q.trans_amount as amount,
       q.business_short_code as queue_business_code,
       q.loan_id as linked_loan_id,
       c.shortcode as configured_paybill,
       c.mpesa_auto_confirm as auto_confirm_enabled,
       (select count(distinct s.business_id)
          from public.loan_settings s
         where trim(s.mpesa_shortcode::text)=trim(c.shortcode)) as businesses_using_paybill,
       length(q.account_digits) as account_digits_length,
       (select count(*) from public.loan_clients cl cross join p
         where cl.business_id=p.business_id
           and cl.id_number=q.account_digits
           and length(q.account_digits) between 5 and 12) as matching_bripta_clients,
       (select count(*) from public.loans l join public.loan_clients cl on cl.id=l.client_id
          cross join p
         where l.business_id=p.business_id
           and cl.business_id=p.business_id
           and cl.id_number=q.account_digits
           and l.status='active' and l.outstanding_balance>0
           and length(q.account_digits) between 5 and 12) as matching_active_loans,
       (select count(*) from public.loan_repayments r cross join p
         where r.business_id=p.business_id
           and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id)) as existing_repayments,
       (select count(*) from public.unmatched_payments u cross join p
         where u.business_id=p.business_id and u.mpesa_reference=q.trans_id) as existing_suspense,
       (q.raw_payload is not null) as callback_payload_available
from pending q cross join p left join config c on true
order by q.created_at,q.trans_id;
