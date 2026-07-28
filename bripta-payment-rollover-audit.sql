-- Bripta / Loanflow: payment and rollover-penalty audit
-- READ-ONLY: every section is a SELECT. This file does not change any data.
-- Run each numbered section separately in the Supabase SQL Editor.

-- 1) M-Pesa processing summary for the last 30 days.
-- received_by_callback: Safaricom reached Bripta and the callback saved the payment.
-- waiting_or_unmatched: received, but not automatically posted to a loan.
-- received_without_repayment: callback row exists but no repayment has the same M-Pesa reference.
with recent_queue as (
  select *
  from public.mpesa_callback_queue
  where created_at >= now() - interval '30 days'
),
queue_check as (
  select
    q.id,
    q.trans_id,
    q.trans_amount,
    q.confirmed,
    q.loan_id,
    q.repayment_id,
    exists (
      select 1
      from public.loan_repayments r
      where upper(trim(coalesce(r.payment_reference, ''))) = upper(trim(coalesce(q.trans_id, '')))
         or upper(trim(coalesce(r.receipt_no, ''))) = upper(trim(coalesce(q.trans_id, '')))
    ) as has_repayment
  from recent_queue q
)
select
  count(*) as received_by_callback,
  count(*) filter (where coalesce(confirmed, false) = false) as waiting_or_unmatched,
  count(*) filter (where coalesce(confirmed, false) = true and not has_repayment) as confirmed_without_repayment,
  count(*) filter (where not has_repayment) as received_without_repayment,
  coalesce(sum(trans_amount), 0) as total_received_amount,
  coalesce(sum(trans_amount) filter (where not has_repayment), 0) as amount_not_in_repayments
from queue_check;

-- 2) Exact callback payments from the last 30 days that are not in Repayments.
-- These are the strongest database evidence of payments Bripta received but did not post automatically.
select
  q.id as queue_id,
  q.trans_id as mpesa_reference,
  q.trans_amount as amount,
  q.msisdn as payer_phone,
  q.bill_ref_number as account_reference,
  q.confirmed,
  q.loan_id,
  q.repayment_id,
  q.created_at as callback_received_at
from public.mpesa_callback_queue q
where q.created_at >= now() - interval '30 days'
  and not exists (
    select 1
    from public.loan_repayments r
    where upper(trim(coalesce(r.payment_reference, ''))) = upper(trim(coalesce(q.trans_id, '')))
       or upper(trim(coalesce(r.receipt_no, ''))) = upper(trim(coalesce(q.trans_id, '')))
  )
order by q.created_at desc;

-- 3) Unresolved suspense/unmatched payments.
select
  id,
  mpesa_reference,
  amount,
  payer_phone,
  account_number,
  payer_name,
  business_id,
  created_at
from public.unmatched_payments
where coalesce(resolved, false) = false
order by created_at desc;

-- 4) Check whether each Paybill is configured for automatic confirmation.
-- mpesa_auto_confirm must be true for an identified payment to post automatically.
select
  business_id,
  company_name,
  mpesa_shortcode,
  mpesa_auto_confirm,
  daraja_environment
from public.loan_settings
order by company_name nulls last;

-- 5) Duplicate M-Pesa references in the callback queue or repayment table.
-- A repeated reference can cause an insert to fail or make the same payment look inconsistent.
select
  'callback_queue' as source,
  trans_id as mpesa_reference,
  count(*) as occurrences,
  sum(trans_amount) as combined_amount
from public.mpesa_callback_queue
where nullif(trim(coalesce(trans_id, '')), '') is not null
group by trans_id
having count(*) > 1
union all
select
  'loan_repayments' as source,
  payment_reference as mpesa_reference,
  count(*) as occurrences,
  sum(amount) as combined_amount
from public.loan_repayments
where nullif(trim(coalesce(payment_reference, '')), '') is not null
  and lower(coalesce(payment_method, '')) like '%mpesa%'
group by payment_reference
having count(*) > 1
order by occurrences desc, mpesa_reference;

-- 6) Rollover penalties applied more than once to the same loan.
-- The current business rule is ONE rollover penalty per loan for its lifetime.
with rollover_rows as (
  select
    p.id,
    p.loan_id,
    p.penalty_amount,
    p.reason,
    p.date_charged,
    p.is_waived
  from public.loan_penalties p
  where coalesce(p.reason, '') ilike '%rollover%'
),
repeated as (
  select loan_id
  from rollover_rows
  group by loan_id
  having count(*) > 1
)
select
  c.full_name,
  c.phone,
  l.loan_no,
  l.status,
  l.total_payable,
  l.outstanding_balance,
  count(r.id) as rollover_count,
  count(r.id) filter (where coalesce(r.is_waived, false) = false) as active_rollover_count,
  coalesce(sum(r.penalty_amount), 0) as total_rollover_charged,
  coalesce(sum(r.penalty_amount) filter (where coalesce(r.is_waived, false) = false), 0) as active_rollover_amount,
  min(r.date_charged) as first_rollover_date,
  max(r.date_charged) as latest_rollover_date,
  string_agg(coalesce(r.reason, '(no reason)'), ' | ' order by r.date_charged) as recorded_reasons
from repeated x
join public.loans l on l.id = x.loan_id
join public.loan_clients c on c.id = l.client_id
join rollover_rows r on r.loan_id = l.id
group by c.full_name, c.phone, l.loan_no, l.status, l.total_payable, l.outstanding_balance
order by active_rollover_count desc, rollover_count desc, c.full_name;

-- 7) Show every rollover reason format currently stored.
-- This confirms whether older wording is bypassing the app's one-time safeguard.
select
  reason,
  count(*) as penalty_rows,
  count(distinct loan_id) as affected_loans,
  count(*) filter (where coalesce(is_waived, false) = false) as active_rows,
  min(date_charged) as first_seen,
  max(date_charged) as last_seen
from public.loan_penalties
where coalesce(reason, '') ilike '%rollover%'
group by reason
order by last_seen desc, penalty_rows desc;

-- 8) Overall repeated-rollover totals for the client report.
with per_loan as (
  select
    loan_id,
    count(*) as rollover_count,
    count(*) filter (where coalesce(is_waived, false) = false) as active_rollover_count,
    coalesce(sum(penalty_amount) filter (where coalesce(is_waived, false) = false), 0) as active_rollover_amount
  from public.loan_penalties
  where coalesce(reason, '') ilike '%rollover%'
  group by loan_id
)
select
  count(*) filter (where rollover_count > 1) as loans_with_repeated_rollovers,
  count(*) filter (where active_rollover_count > 1) as loans_with_multiple_active_rollovers,
  coalesce(sum(active_rollover_amount) filter (where active_rollover_count > 1), 0) as repeated_active_penalty_amount
from per_loan;
