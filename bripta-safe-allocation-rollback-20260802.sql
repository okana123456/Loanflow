-- Bripta / Loanflow: safe rollback of automatic fee allocation
-- Run this ONCE in the Bripta Supabase SQL Editor after deploying the matching
-- index.html and payment-callback updates.
--
-- What this does:
-- 1. Preserves the current state in emergency backup tables.
-- 2. Stops full-history balance recalculation.
-- 3. Makes every affected payment reduce its loan by the full amount again.
-- 4. Restores July 31 pre-correction balances, then subtracts repayments made
--    after that backup so no recent customer payment is lost.

begin;

create table if not exists public.bripta_safe_rollback_loans_20260802 as
select l.*, now() as rollback_backed_up_at
from public.loans l;

create table if not exists public.bripta_safe_rollback_repayments_20260802 as
select r.*, now() as rollback_backed_up_at
from public.loan_repayments r
where coalesce(r.registration_fee_portion, 0) <> 0
   or coalesce(r.processing_fee_portion, 0) <> 0
   or coalesce(r.credit_portion, 0) <> 0
   or abs(coalesce(r.loan_portion, r.amount)::numeric - coalesce(r.amount, 0)::numeric) > 0.01;

create table if not exists public.bripta_safe_rollback_clients_20260802 as
select c.*, now() as rollback_backed_up_at
from public.loan_clients c
where coalesce(c.account_credit, 0) <> 0;

-- Preserve the exact extra amount that had been diverted away from each loan.
create temporary table bripta_allocation_deltas on commit drop as
select
  r.loan_id,
  round(sum(greatest(0, coalesce(r.amount, 0)::numeric - coalesce(r.loan_portion, r.amount, 0)::numeric)), 2) as diverted_amount
from public.loan_repayments r
where r.loan_id is not null
  and (
    coalesce(r.registration_fee_portion, 0) <> 0
    or coalesce(r.processing_fee_portion, 0) <> 0
    or coalesce(r.credit_portion, 0) <> 0
    or abs(coalesce(r.loan_portion, r.amount)::numeric - coalesce(r.amount, 0)::numeric) > 0.01
  )
group by r.loan_id;

-- The trigger introduced by the previous setup recalculated all historical
-- balances. Remove it before restoring business-approved balances.
drop trigger if exists bripta_sync_loan_balance_trigger on public.loan_repayments;

-- All recorded customer payments now count fully toward the selected loan.
update public.loan_repayments
set
  loan_portion = amount,
  registration_fee_portion = 0,
  processing_fee_portion = 0,
  credit_portion = 0,
  notes = case
    when coalesce(notes, '') ilike '%Allocation:%'
      then concat(coalesce(notes, ''), ' | Automatic fee allocation reversed on 02 Aug 2026; full amount applied to loan.')
    else notes
  end
where coalesce(registration_fee_portion, 0) <> 0
   or coalesce(processing_fee_portion, 0) <> 0
   or coalesce(credit_portion, 0) <> 0
   or abs(coalesce(loan_portion, amount)::numeric - coalesce(amount, 0)::numeric) > 0.01;

-- For loans that were not in the July 31 backup, reduce the current balance by
-- only the amount that had been diverted to fees/credit.
update public.loans l
set
  total_paid = round(coalesce(l.total_paid, 0)::numeric + d.diverted_amount, 2),
  outstanding_balance = round(greatest(0, coalesce(l.outstanding_balance, 0)::numeric - d.diverted_amount), 2),
  status = case
    when greatest(0, coalesce(l.outstanding_balance, 0)::numeric - d.diverted_amount) <= 0.01 then 'completed'
    else l.status
  end,
  arrears_amount = case
    when greatest(0, coalesce(l.outstanding_balance, 0)::numeric - d.diverted_amount) <= 0.01 then 0
    else least(coalesce(l.arrears_amount, 0)::numeric, greatest(0, coalesce(l.outstanding_balance, 0)::numeric - d.diverted_amount))
  end,
  overdue_days = case
    when greatest(0, coalesce(l.outstanding_balance, 0)::numeric - d.diverted_amount) <= 0.01 then 0
    else coalesce(l.overdue_days, 0)
  end
from bripta_allocation_deltas d
where l.id = d.loan_id
  and not exists (
    select 1 from public.bripta_balance_fix_backup_20260731 b where b.id = l.id
  );

-- Restore the accepted balance that existed before the July 31 correction,
-- then apply only genuine repayments entered after that backup timestamp.
with restored as (
  select
    b.id,
    b.total_paid as original_total_paid,
    b.outstanding_balance as original_balance,
    b.status as original_status,
    b.arrears_amount as original_arrears,
    b.overdue_days as original_overdue_days,
    round(coalesce(sum(r.amount) filter (where r.created_at > b.backed_up_at), 0)::numeric, 2) as later_repayments
  from public.bripta_balance_fix_backup_20260731 b
  left join public.loan_repayments r on r.loan_id = b.id
  group by b.id, b.total_paid, b.outstanding_balance, b.status, b.arrears_amount, b.overdue_days, b.backed_up_at
)
update public.loans l
set
  total_paid = round(coalesce(x.original_total_paid, 0)::numeric + x.later_repayments, 2),
  outstanding_balance = round(greatest(0, coalesce(x.original_balance, 0)::numeric - x.later_repayments), 2),
  status = case
    when greatest(0, coalesce(x.original_balance, 0)::numeric - x.later_repayments) <= 0.01 then 'completed'
    when x.original_status = 'completed' then 'active'
    else x.original_status
  end,
  arrears_amount = case
    when greatest(0, coalesce(x.original_balance, 0)::numeric - x.later_repayments) <= 0.01 then 0
    else least(coalesce(x.original_arrears, 0)::numeric, greatest(0, coalesce(x.original_balance, 0)::numeric - x.later_repayments))
  end,
  overdue_days = case
    when greatest(0, coalesce(x.original_balance, 0)::numeric - x.later_repayments) <= 0.01 then 0
    else coalesce(x.original_overdue_days, 0)
  end,
  registration_fee_due = 0,
  registration_fee_paid = 0,
  processing_fee_paid = 0
from restored x
where l.id = x.id;

-- Disable automatic fee dues for every loan. Processing fee remains available
-- as loan metadata, but repayments are no longer diverted to it.
update public.loans
set registration_fee_due = 0,
    registration_fee_paid = 0,
    processing_fee_paid = 0
where coalesce(registration_fee_due, 0) <> 0
   or coalesce(registration_fee_paid, 0) <> 0
   or coalesce(processing_fee_paid, 0) <> 0;

update public.loan_clients
set account_credit = 0
where coalesce(account_credit, 0) <> 0;

-- Apply only the previously diverted amount to the oldest unpaid schedules.
with schedule_remaining as (
  select
    s.id,
    s.loan_id,
    greatest(0, coalesce(s.total_due, 0)::numeric - coalesce(s.total_paid, 0)::numeric) as remaining_due,
    coalesce(sum(greatest(0, coalesce(s.total_due, 0)::numeric - coalesce(s.total_paid, 0)::numeric)) over (
      partition by s.loan_id
      order by s.due_date, s.installment_no, s.id
      rows between unbounded preceding and 1 preceding
    ), 0) as remaining_before
  from public.loan_schedules s
  join bripta_allocation_deltas d on d.loan_id = s.loan_id
), schedule_apply as (
  select
    sr.id,
    least(sr.remaining_due, greatest(0, d.diverted_amount - sr.remaining_before)) as apply_amount
  from schedule_remaining sr
  join bripta_allocation_deltas d on d.loan_id = sr.loan_id
)
update public.loan_schedules s
set
  total_paid = round(coalesce(s.total_paid, 0)::numeric + a.apply_amount, 2),
  status = case
    when coalesce(s.total_paid, 0)::numeric + a.apply_amount >= coalesce(s.total_due, 0)::numeric - 0.01 then 'paid'
    when coalesce(s.total_paid, 0)::numeric + a.apply_amount > 0 then 'partial'
    else s.status
  end,
  paid_at = case
    when coalesce(s.total_paid, 0)::numeric + a.apply_amount >= coalesce(s.total_due, 0)::numeric - 0.01
      then coalesce(s.paid_at, now())
    else s.paid_at
  end
from schedule_apply a
where s.id = a.id
  and a.apply_amount > 0;

-- The earlier balance correction may have marked every schedule as paid when
-- it temporarily closed a loan. Rebuild schedules for restored loans using the
-- restored total_paid, oldest instalment first.
with restored_schedule_targets as (
  select
    s.id,
    s.due_date,
    coalesce(s.total_due, 0)::numeric as total_due,
    least(
      coalesce(s.total_due, 0)::numeric,
      greatest(
        0,
        greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(l.outstanding_balance, 0)::numeric)
        - coalesce(sum(coalesce(s.total_due, 0)::numeric) over (
            partition by s.loan_id
            order by s.due_date, s.installment_no, s.id
            rows between unbounded preceding and 1 preceding
          ), 0)
      )
    ) as target_paid
  from public.loan_schedules s
  join public.loans l on l.id = s.loan_id
  join public.bripta_balance_fix_backup_20260731 b on b.id = l.id
)
update public.loan_schedules s
set
  total_paid = round(t.target_paid, 2),
  status = case
    when t.target_paid >= t.total_due - 0.01 then 'paid'
    when t.target_paid > 0 then 'partial'
    when t.due_date < current_date then 'overdue'
    else 'pending'
  end,
  paid_at = case
    when t.target_paid >= t.total_due - 0.01 then coalesce(s.paid_at, now())
    else null
  end
from restored_schedule_targets t
where s.id = t.id;

commit;

select
  'Bripta balances restored; automatic fee diversion disabled' as result,
  (select count(*) from public.bripta_safe_rollback_loans_20260802) as loans_backed_up,
  (select count(*) from public.bripta_safe_rollback_repayments_20260802) as affected_payments_restored,
  (select count(*) from public.bripta_balance_fix_backup_20260731) as july_31_balances_restored;

select 'payments_still_diverted_to_fees' as check_name, count(*)::bigint as rows_remaining
from public.loan_repayments
where coalesce(registration_fee_portion, 0) <> 0
   or coalesce(processing_fee_portion, 0) <> 0
   or coalesce(credit_portion, 0) <> 0
   or abs(coalesce(loan_portion, amount)::numeric - coalesce(amount, 0)::numeric) > 0.01
union all
select 'automatic_balance_trigger_still_active', count(*)::bigint
from pg_trigger
where tgname = 'bripta_sync_loan_balance_trigger'
  and not tgisinternal
union all
select 'negative_loan_balances', count(*)::bigint
from public.loans
where coalesce(outstanding_balance, 0) < 0;
