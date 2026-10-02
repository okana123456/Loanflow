-- Read-only. Run AFTER the recovery, and again after the SMS sender processes the queue.
with recovered as (
  select a.*,r.id as live_repayment_id,r.amount as live_amount,
    r.loan_id as live_loan_id,r.business_id as live_business_id,
    r.payment_date as live_payment_date,q.confirmed,q.repayment_id as queue_repayment_id
  from public.bripta_payment_recovery_20261002 a
  left join public.loan_repayments r on r.id=a.repayment_id
  left join public.mpesa_callback_queue q on q.id=a.queue_id
  where a.business_id='BIZ-B3F5E5D9'
)
select 'recovered_payments' as check_name,count(live_repayment_id)::numeric as result
from recovered
union all select 'recovered_amount',coalesce(sum(live_amount),0) from recovered
union all select 'schedule_allocation_pending_review',coalesce(sum(schedule_unallocated_amount),0) from recovered
union all select 'emily_balance_after_recovery',outstanding_balance::numeric from public.loans
where business_id='BIZ-B3F5E5D9' and id='12018065-3a0d-4434-a9b4-995c735a6968'::uuid
union all select 'recovery_integrity_errors',count(*)::numeric from recovered
where live_repayment_id is null or live_amount is distinct from amount
  or live_loan_id is distinct from loan_id or live_business_id is distinct from business_id
  or live_payment_date is distinct from payment_date
  or not coalesce(confirmed,false) or queue_repayment_id is distinct from repayment_id
union all select 'held_payment_repayments',count(*)::numeric from public.loan_repayments
where business_id='BIZ-B3F5E5D9' and (payment_reference='UJ2558DDTO' or receipt_no='UJ2558DDTO')
union all select 'held_suspense_amount',coalesce(sum(trans_amount),0)::numeric from public.mpesa_callback_queue
where business_short_code='BIZ-B3F5E5D9' and trans_id='UJ2558DDTO'
  and not coalesce(confirmed,false) and repayment_id is null and bripta_review_reason is not null
union all select 'recovery_sms_missing',count(*)::numeric from recovered a
where not exists(select 1 from public.bripta_sms_outbox s where s.repayment_id=a.repayment_id)
union all select 'recovery_sms_'||s.status,count(*)::numeric
from public.bripta_sms_outbox s join recovered a on a.repayment_id=s.repayment_id group by s.status
union all select 'sms_credits_remaining',(credits_purchased-credits_used)::numeric
from public.bripta_sms_wallets where business_id='BIZ-B3F5E5D9'
union all select 'other_pending_callbacks',count(*)::numeric
from public.mpesa_callback_queue q where q.business_short_code='BIZ-B3F5E5D9'
  and q.created_at>=timestamptz '2026-10-01 22:00:00+03:00'
  and not coalesce(q.confirmed,false) and q.trans_id<>'UJ2558DDTO'
  and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
  and not exists(select 1 from public.loan_repayments r where r.business_id='BIZ-B3F5E5D9'
                 and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id));
