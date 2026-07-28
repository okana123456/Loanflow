-- Bripta / Loanflow: M-Pesa visibility + one-time rollover repair
-- Run in Supabase SQL Editor after deploying the updated index.html and payment-callback function.
-- This repairs historical records only. It does not delete any payment or penalty history.

-- 1) Bring hidden M-Pesa callbacks back to Suspense when no repayment exists.
-- These rows were received by Daraja but are not attached to any repayment.
update public.mpesa_callback_queue q
set confirmed = false
where q.created_at >= now() - interval '60 days'
  and coalesce(q.confirmed, false) = true
  and q.repayment_id is null
  and not exists (
    select 1
    from public.loan_repayments r
    where upper(trim(coalesce(r.payment_reference, ''))) = upper(trim(coalesce(q.trans_id, '')))
       or upper(trim(coalesce(r.receipt_no, ''))) = upper(trim(coalesce(q.trans_id, '')))
  );

-- 2) Preview loans with more than one active rollover penalty.
with active_rollovers as (
  select
    p.*,
    row_number() over (
      partition by p.loan_id
      order by p.date_charged asc nulls last, p.id asc
    ) as keep_order
  from public.loan_penalties p
  where coalesce(p.is_waived, false) = false
    and coalesce(p.reason, '') ilike '%rollover%'
),
extras as (
  select loan_id, count(*) as extra_rows, coalesce(sum(penalty_amount), 0)::numeric as extra_amount
  from active_rollovers
  where keep_order > 1
  group by loan_id
),
repayment_totals as (
  select loan_id, coalesce(sum(amount), 0)::numeric as repayment_total
  from public.loan_repayments
  group by loan_id
)
select
  c.full_name,
  c.phone,
  l.loan_no,
  l.status,
  l.total_payable,
  coalesce(rt.repayment_total, l.total_paid, 0) as repayment_total,
  l.outstanding_balance,
  e.extra_rows as repeated_rollovers_to_waive,
  e.extra_amount as repeated_rollover_amount_to_remove,
  greatest(0, coalesce(l.total_payable, 0)::numeric - e.extra_amount - coalesce(rt.repayment_total, l.total_paid, 0)::numeric) as corrected_balance
from extras e
join public.loans l on l.id = e.loan_id
join public.loan_clients c on c.id = l.client_id
left join repayment_totals rt on rt.loan_id = l.id
order by e.extra_amount desc, c.full_name;

-- 3) Waive repeated rollover penalties, keeping only the first active rollover per loan.
with active_rollovers as (
  select
    p.id,
    row_number() over (
      partition by p.loan_id
      order by p.date_charged asc nulls last, p.id asc
    ) as keep_order
  from public.loan_penalties p
  where coalesce(p.is_waived, false) = false
    and coalesce(p.reason, '') ilike '%rollover%'
)
update public.loan_penalties p
set
  is_waived = true,
  waived_reason = 'System repair: repeated rollover removed; one-time rollover policy kept'
from active_rollovers r
where p.id = r.id
  and r.keep_order > 1;

-- 4) Remove those repeated rollover amounts from loan balances.
with waived_extras as (
  select
    loan_id,
    coalesce(sum(penalty_amount), 0)::numeric as extra_amount
  from public.loan_penalties
  where coalesce(is_waived, false) = true
    and coalesce(waived_reason, '') = 'System repair: repeated rollover removed; one-time rollover policy kept'
  group by loan_id
),
repayment_totals as (
  select loan_id, coalesce(sum(amount), 0)::numeric as repayment_total
  from public.loan_repayments
  group by loan_id
),
corrected as (
  select
    l.id,
    greatest(0, coalesce(l.total_payable, 0)::numeric - e.extra_amount) as corrected_total_payable,
    coalesce(rt.repayment_total, l.total_paid, 0)::numeric as repayment_total,
    greatest(0, coalesce(l.total_payable, 0)::numeric - e.extra_amount - coalesce(rt.repayment_total, l.total_paid, 0)::numeric) as corrected_balance,
    greatest(0, coalesce(l.arrears_amount, 0)::numeric - e.extra_amount) as corrected_arrears
  from public.loans l
  join waived_extras e on e.loan_id = l.id
  left join repayment_totals rt on rt.loan_id = l.id
)
update public.loans l
set
  total_payable = round(c.corrected_total_payable, 2),
  total_paid = round(c.repayment_total, 2),
  outstanding_balance = round(c.corrected_balance, 2),
  arrears_amount = case
    when c.corrected_balance <= 0.01 then 0
    else round(least(c.corrected_arrears, c.corrected_balance), 2)
  end,
  overdue_days = case when c.corrected_balance <= 0.01 then 0 else coalesce(l.overdue_days, 0) end,
  status = case when c.corrected_balance <= 0.01 then 'completed' else l.status end
from corrected c
where l.id = c.id;

-- 5) Completed loans must not keep unpaid schedules or arrears.
update public.loan_schedules s
set
  penalty_charged = 0,
  status = 'paid'
from public.loans l
where s.loan_id = l.id
  and l.status = 'completed'
  and coalesce(l.outstanding_balance, 0) <= 0.01
  and s.status in ('pending', 'partial', 'overdue');

update public.loans
set
  outstanding_balance = 0,
  arrears_amount = 0,
  overdue_days = 0
where status = 'completed'
  and coalesce(outstanding_balance, 0) <= 0.01;

-- 6) Final check. These should return zero or very few rows after repair.
with per_loan as (
  select
    loan_id,
    count(*) filter (where coalesce(is_waived, false) = false and coalesce(reason, '') ilike '%rollover%') as active_rollovers
  from public.loan_penalties
  group by loan_id
)
select
  'repeated_active_rollovers_remaining' as check_name,
  count(*) as rows_remaining
from per_loan
where active_rollovers > 1
union all
select
  'hidden_callback_payments_remaining' as check_name,
  count(*) as rows_remaining
from public.mpesa_callback_queue q
where q.created_at >= now() - interval '60 days'
  and coalesce(q.confirmed, false) = true
  and q.repayment_id is null
  and not exists (
    select 1
    from public.loan_repayments r
    where upper(trim(coalesce(r.payment_reference, ''))) = upper(trim(coalesce(q.trans_id, '')))
       or upper(trim(coalesce(r.receipt_no, ''))) = upper(trim(coalesce(q.trans_id, '')))
  );
