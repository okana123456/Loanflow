-- READ ONLY: three older pending KES 700 callbacks now visible in Migori Suspense.
-- Bripta uses the borrower's registered PHONE as the Paybill account.
-- No payments are posted and no SMS are sent by this query.
with pending as (
  select q.*,to_jsonb(q) as details
  from public.mpesa_callback_queue q
  where q.business_short_code='BIZ-B3F5E5D9'
    and q.trans_id in ('UJ16F8CN12','UJ1BZ8H7SH','UJ1JL8EY56')
    and not coalesce(q.confirmed,false)
    
    and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
    and not exists (
      select 1 from public.loan_repayments r
      where r.business_id='BIZ-B3F5E5D9'
        and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id)
    )
), phones as (
  select 'account' as source,id::text as id,bill_ref_number::text as raw from pending
  union all
  select 'client',id::text,phone::text from public.loan_clients
  where business_id='BIZ-B3F5E5D9'
), normalized as (
  select source,id,
    case when raw ~ '^[+0-9 ()-]+$'
      and regexp_replace(raw,'[^0-9]','','g') ~ '^(254|0)?[17][0-9]{8}$'
    then right(regexp_replace(raw,'[^0-9]','','g'),9) end as phone
  from phones
)
select q.created_at as callback_at_utc,q.id as queue_id,
  q.trans_id as transaction_code,q.trans_amount as amount,
  q.bill_ref_number as account_reference,
  concat_ws(' ',q.details->>'first_name',q.details->>'middle_name',q.details->>'last_name') as payer_name,
  q.loan_id as linked_loan_id,q.repayment_id as linked_repayment_id,
  q.details->>'bripta_review_reason' as review_reason,
  coalesce(q.details->>'last_error',q.details->>'error_message',q.details->>'error') as recorded_error,
  q.raw_payload is not null as payload_available,
  q.branch_id as branch_id,
  (select count(*) from public.unmatched_payments u
    where u.business_id='BIZ-B3F5E5D9' and u.mpesa_reference=q.trans_id) as existing_suspense_records,
  (select count(*) from public.mpesa_callback_queue duplicate
    where duplicate.business_short_code='BIZ-B3F5E5D9' and duplicate.trans_id=q.trans_id) as queue_rows_for_reference,
  coalesce((
    select jsonb_agg(jsonb_build_object(
      'client_id',c.id,'client_name',c.full_name,
      'active_loans',coalesce((
        select jsonb_agg(jsonb_build_object('loan_id',l.id,'loan_no',l.loan_no,'balance',l.outstanding_balance) order by l.created_at)
        from public.loans l where l.business_id='BIZ-B3F5E5D9'
          and l.client_id=c.id and l.status='active' and l.outstanding_balance>0
      ),'[]'::jsonb)
    ) order by c.full_name)
    from normalized cp join public.loan_clients c on c.id::text=cp.id
    where cp.source='client' and cp.phone=ap.phone and c.business_id='BIZ-B3F5E5D9'
  ),'[]'::jsonb) as account_phone_candidates
from pending q
join normalized ap on ap.source='account' and ap.id=q.id::text
order by q.created_at,q.id;
