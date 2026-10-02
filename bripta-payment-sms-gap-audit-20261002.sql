-- READ ONLY. Bripta payment and SMS audit since 1 October 2026,
-- 10:00 pm Africa/Nairobi (19:00 UTC). No rows are changed.
-- Run the entire file in the Bripta Supabase SQL Editor and send the result.

with parameters as (
  select 'BIZ-B3F5E5D9'::text as business_id,
         timestamptz '2026-10-01 22:00:00+03:00' as since_at
),
bripta_queue as (
  select q.*
  from public.mpesa_callback_queue q cross join parameters p
  where q.created_at >= p.since_at
    and (
      q.business_short_code = p.business_id
      or trim(q.business_short_code::text) in (
        select trim(s.mpesa_shortcode::text)
        from public.loan_settings s
        where s.business_id=p.business_id and s.mpesa_shortcode is not null
      )
    )
),
bripta_repayments as (
  select r.*
  from public.loan_repayments r cross join parameters p
  where r.business_id=p.business_id and r.created_at>=p.since_at
),
bripta_unmatched as (
  select u.*
  from public.unmatched_payments u cross join parameters p
  where u.business_id=p.business_id and u.created_at>=p.since_at
),
bripta_sms as (
  select o.*
  from public.bripta_sms_outbox o cross join parameters p
  where o.business_id=p.business_id
    and (o.queued_at>=p.since_at or o.sent_at>=p.since_at)
),
audit_rows as (
  select 'total'::text as source,'mpesa_callbacks'::text as state,
         count(*)::bigint as row_count,
         coalesce(sum(q.trans_amount),0)::numeric as amount_or_credits,
         min(q.created_at) as first_seen,max(q.created_at) as last_seen
  from bripta_queue q

  union all
  select 'total','repayments',count(*),coalesce(sum(r.amount),0),
         min(r.created_at),max(r.created_at)
  from bripta_repayments r

  union all
  select 'total','unmatched_payments',count(*),coalesce(sum(u.amount),0),
         min(u.created_at),max(u.created_at)
  from bripta_unmatched u

  union all
  select 'total','sms_outbox',count(*),coalesce(sum(o.credits_reserved),0),
         min(o.queued_at),max(o.queued_at)
  from bripta_sms o

  union all
  select 'mpesa_callback_queue'::text as source,
         case when q.confirmed then 'confirmed' else 'unconfirmed' end::text as state,
         count(*)::bigint as row_count,
         coalesce(sum(q.trans_amount),0)::numeric as amount_or_credits,
         min(q.created_at) as first_seen,
         max(q.created_at) as last_seen
  from bripta_queue q group by q.confirmed

  union all
  select 'loan_repayments',coalesce(r.payment_method,'unknown'),count(*),
         coalesce(sum(r.amount),0),min(r.created_at),max(r.created_at)
  from bripta_repayments r group by r.payment_method

  union all
  select 'unmatched_payments',
         case when u.resolved then 'resolved' else 'unresolved' end,
         count(*),coalesce(sum(u.amount),0),min(u.created_at),max(u.created_at)
  from bripta_unmatched u group by u.resolved

  union all
  select 'sms_outbox',coalesce(o.message_type,'unknown')||' / '||coalesce(o.status,'unknown'),
         count(*),coalesce(sum(o.credits_reserved),0)::numeric,
         min(o.queued_at),max(o.queued_at)
  from bripta_sms o group by o.message_type,o.status

  union all
  select 'needs_review','callback_without_repayment_or_suspense',
         count(*),coalesce(sum(q.trans_amount),0),min(q.created_at),max(q.created_at)
  from bripta_queue q cross join parameters p
  where not exists (
    select 1 from public.loan_repayments r
    where r.business_id=p.business_id
      and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id)
  )
    and not exists (
      select 1 from public.unmatched_payments u
      where u.business_id=p.business_id and u.mpesa_reference=q.trans_id
    )

  union all
  select 'needs_review','repayment_without_sms_outbox',
         count(*),coalesce(sum(r.amount),0),min(r.created_at),max(r.created_at)
  from bripta_repayments r
  where not exists (
    select 1 from public.bripta_sms_outbox o where o.repayment_id=r.id
  )

  union all
  select 'sms_wallet','credits_purchased',1,
         coalesce(w.credits_purchased,0)::numeric,null::timestamptz,null::timestamptz
  from public.bripta_sms_wallets w cross join parameters p
  where w.business_id=p.business_id

  union all
  select 'sms_wallet','credits_used',1,
         coalesce(w.credits_used,0)::numeric,null::timestamptz,null::timestamptz
  from public.bripta_sms_wallets w cross join parameters p
  where w.business_id=p.business_id

  union all
  select 'sms_wallet','credits_remaining',1,
         (coalesce(w.credits_purchased,0)-coalesce(w.credits_used,0))::numeric,
         null::timestamptz,null::timestamptz
  from public.bripta_sms_wallets w cross join parameters p
  where w.business_id=p.business_id
)
select source,state,row_count,amount_or_credits,first_seen,last_seen
from audit_rows
order by source,state;
