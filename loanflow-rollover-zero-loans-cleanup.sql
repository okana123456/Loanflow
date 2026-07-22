-- Bripta / Loanflow rollover and zero-balance cleanup
-- Purpose:
-- 1. Preview Joseph Ochieng Siangla and any similar loans affected by rollover penalties after clearance.
-- 2. Stop cleared loans from carrying balances, arrears, or overdue days.
-- 3. Mark zero-balance active loans as completed.
--
-- Run section 1 first. If the preview looks correct, run sections 2, 3, and 4.

-- 1) Preview Joseph and similar loans where removing rollover penalties shows the loan was already cleared.
with repayment_totals as (
  select
    loan_id,
    coalesce(sum(amount), 0)::numeric as repayment_total
  from public.loan_repayments
  group by loan_id
),
rollover_totals as (
  select
    loan_id,
    coalesce(sum(penalty_amount) filter (
      where coalesce(is_waived, false) = false
        and coalesce(reason, '') ilike 'Rollover Penalty (%'
    ), 0)::numeric as active_rollover_penalty
  from public.loan_penalties
  group by loan_id
),
loan_check as (
  select
    l.id as loan_id,
    c.full_name,
    c.phone,
    l.loan_no,
    l.status,
    coalesce(l.total_payable, 0)::numeric as stored_total_payable,
    coalesce(rt.repayment_total, 0)::numeric as repayment_total,
    coalesce(ro.active_rollover_penalty, 0)::numeric as active_rollover_penalty,
    greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) as current_expected_balance,
    greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(ro.active_rollover_penalty, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) as expected_balance_without_rollover,
    coalesce(l.outstanding_balance, 0)::numeric as stored_outstanding_balance,
    coalesce(l.arrears_amount, 0)::numeric as stored_arrears,
    coalesce(l.overdue_days, 0)::numeric as stored_overdue_days
  from public.loans l
  join public.loan_clients c on c.id = l.client_id
  left join repayment_totals rt on rt.loan_id = l.id
  left join rollover_totals ro on ro.loan_id = l.id
)
select *
from loan_check
where full_name ilike '%Joseph Ochieng Siangla%'
   or regexp_replace(coalesce(phone, ''), '\D', '', 'g') in ('0715549030', '715549030', '254715549030')
   or (
    active_rollover_penalty > 0
    and expected_balance_without_rollover <= 0.01
    and (
      status = 'active'
      or stored_outstanding_balance > 0.01
      or stored_arrears > 0.01
      or stored_overdue_days > 0
    )
  )
order by full_name, loan_no;

-- 2) Repair loans that only still have a balance because of rollover penalties.
with repayment_totals as (
  select
    loan_id,
    coalesce(sum(amount), 0)::numeric as repayment_total
  from public.loan_repayments
  group by loan_id
),
rollover_totals as (
  select
    loan_id,
    coalesce(sum(penalty_amount) filter (
      where coalesce(is_waived, false) = false
        and coalesce(reason, '') ilike 'Rollover Penalty (%'
    ), 0)::numeric as active_rollover_penalty
  from public.loan_penalties
  group by loan_id
),
affected as (
  select
    l.id,
    greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(ro.active_rollover_penalty, 0)::numeric) as corrected_total_payable,
    coalesce(rt.repayment_total, 0)::numeric as repayment_total
  from public.loans l
  left join repayment_totals rt on rt.loan_id = l.id
  left join rollover_totals ro on ro.loan_id = l.id
  where coalesce(ro.active_rollover_penalty, 0) > 0
    and greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(ro.active_rollover_penalty, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) <= 0.01
)
update public.loans l
set
  total_payable = round(a.corrected_total_payable, 2),
  total_paid = round(a.repayment_total, 2),
  outstanding_balance = 0,
  arrears_amount = 0,
  overdue_days = 0,
  status = 'completed'
from affected a
where l.id = a.id;

-- 3) Waive rollover penalties on those cleared loans so they no longer appear as payable.
with repayment_totals as (
  select
    loan_id,
    coalesce(sum(amount), 0)::numeric as repayment_total
  from public.loan_repayments
  group by loan_id
),
rollover_totals as (
  select
    loan_id,
    coalesce(sum(penalty_amount) filter (
      where coalesce(is_waived, false) = false
        and coalesce(reason, '') ilike 'Rollover Penalty (%'
    ), 0)::numeric as active_rollover_penalty
  from public.loan_penalties
  group by loan_id
),
affected as (
  select l.id
  from public.loans l
  left join repayment_totals rt on rt.loan_id = l.id
  left join rollover_totals ro on ro.loan_id = l.id
  where coalesce(ro.active_rollover_penalty, 0) > 0
    and greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(ro.active_rollover_penalty, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) <= 0.01
)
update public.loan_penalties p
set is_waived = true
from affected a
where p.loan_id = a.id
  and coalesce(p.is_waived, false) = false
  and coalesce(p.reason, '') ilike 'Rollover Penalty (%';

-- 4) Close any active loan that already has zero or negative balance.
update public.loans
set
  outstanding_balance = 0,
  arrears_amount = 0,
  overdue_days = 0,
  status = 'completed'
where status = 'active'
  and coalesce(outstanding_balance, 0) <= 0.01;

-- 5) Clear overdue flags on schedules for completed zero-balance loans.
update public.loan_schedules s
set
  penalty_charged = 0,
  status = 'paid'
from public.loans l
where s.loan_id = l.id
  and l.status = 'completed'
  and coalesce(l.outstanding_balance, 0) <= 0.01
  and s.status in ('pending', 'partial', 'overdue');

-- 6) Final check: this should return no rows after repair.
with repayment_totals as (
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
  coalesce(rt.repayment_total, 0) as repayment_total,
  l.outstanding_balance,
  l.arrears_amount,
  l.overdue_days
from public.loans l
join public.loan_clients c on c.id = l.client_id
left join repayment_totals rt on rt.loan_id = l.id
where (l.status = 'active' and coalesce(l.outstanding_balance, 0) <= 0.01)
   or (l.status = 'completed' and (coalesce(l.outstanding_balance, 0) > 0.01 or coalesce(l.arrears_amount, 0) > 0.01 or coalesce(l.overdue_days, 0) > 0))
order by c.full_name, l.loan_no;
