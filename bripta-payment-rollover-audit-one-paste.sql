-- Bripta / Loanflow: one-paste payment and rollover audit
-- READ-ONLY: this only checks records. It does not update, delete, or insert anything.
-- Paste the whole script into Supabase SQL Editor and run once.

with
recent_queue as (
  select *
  from public.mpesa_callback_queue
  where created_at >= now() - interval '30 days'
),
queue_check as (
  select
    q.id,
    q.trans_id,
    q.trans_amount,
    q.msisdn,
    q.bill_ref_number,
    q.confirmed,
    q.loan_id,
    q.repayment_id,
    q.created_at,
    exists (
      select 1
      from public.loan_repayments r
      where upper(trim(coalesce(r.payment_reference, ''))) = upper(trim(coalesce(q.trans_id, '')))
         or upper(trim(coalesce(r.receipt_no, ''))) = upper(trim(coalesce(q.trans_id, '')))
    ) as has_repayment
  from recent_queue q
),
missing_repayments as (
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
  from queue_check q
  where not q.has_repayment
  order by q.created_at desc
),
unresolved_suspense as (
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
  order by created_at desc
),
settings_check as (
  select
    business_id,
    company_name,
    mpesa_shortcode,
    mpesa_auto_confirm,
    daraja_environment
  from public.loan_settings
  order by company_name nulls last
),
duplicate_refs as (
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
),
rollover_rows as (
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
repeated_rollover_loans as (
  select loan_id
  from rollover_rows
  group by loan_id
  having count(*) > 1
),
repeated_rollovers as (
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
  from repeated_rollover_loans x
  join public.loans l on l.id = x.loan_id
  join public.loan_clients c on c.id = l.client_id
  join rollover_rows r on r.loan_id = l.id
  group by c.full_name, c.phone, l.loan_no, l.status, l.total_payable, l.outstanding_balance
  order by active_rollover_count desc, rollover_count desc, c.full_name
),
rollover_reason_formats as (
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
  order by last_seen desc, penalty_rows desc
),
rollover_totals as (
  select
    count(*) filter (where rollover_count > 1) as loans_with_repeated_rollovers,
    count(*) filter (where active_rollover_count > 1) as loans_with_multiple_active_rollovers,
    coalesce(sum(active_rollover_amount) filter (where active_rollover_count > 1), 0) as repeated_active_penalty_amount
  from (
    select
      loan_id,
      count(*) as rollover_count,
      count(*) filter (where coalesce(is_waived, false) = false) as active_rollover_count,
      coalesce(sum(penalty_amount) filter (where coalesce(is_waived, false) = false), 0) as active_rollover_amount
    from public.loan_penalties
    where coalesce(reason, '') ilike '%rollover%'
    group by loan_id
  ) per_loan
)
select
  1 as section_order,
  '01_payment_summary_last_30_days' as section,
  jsonb_build_object(
    'received_by_callback', count(*),
    'waiting_or_unmatched', count(*) filter (where coalesce(confirmed, false) = false),
    'confirmed_without_repayment', count(*) filter (where coalesce(confirmed, false) = true and not has_repayment),
    'received_without_repayment', count(*) filter (where not has_repayment),
    'total_received_amount', coalesce(sum(trans_amount), 0),
    'amount_not_in_repayments', coalesce(sum(trans_amount) filter (where not has_repayment), 0)
  ) as result
from queue_check

union all
select
  2,
  '02_callback_payments_not_in_repayments',
  coalesce(jsonb_agg(to_jsonb(missing_repayments)), '[]'::jsonb)
from missing_repayments

union all
select
  3,
  '03_unresolved_suspense_payments',
  coalesce(jsonb_agg(to_jsonb(unresolved_suspense)), '[]'::jsonb)
from unresolved_suspense

union all
select
  4,
  '04_paybill_auto_confirm_settings',
  coalesce(jsonb_agg(to_jsonb(settings_check)), '[]'::jsonb)
from settings_check

union all
select
  5,
  '05_duplicate_mpesa_references',
  coalesce(jsonb_agg(to_jsonb(duplicate_refs)), '[]'::jsonb)
from duplicate_refs

union all
select
  6,
  '06_loans_with_repeated_rollovers',
  coalesce(jsonb_agg(to_jsonb(repeated_rollovers)), '[]'::jsonb)
from repeated_rollovers

union all
select
  7,
  '07_rollover_reason_formats',
  coalesce(jsonb_agg(to_jsonb(rollover_reason_formats)), '[]'::jsonb)
from rollover_reason_formats

union all
select
  8,
  '08_repeated_rollover_totals',
  to_jsonb(rollover_totals)
from rollover_totals

order by section_order;
