-- Bripta / Loanflow: accurate loan balances + dismissible suspense entries
-- Run this entire file once in the Bripta Supabase SQL Editor.
-- It preserves repayment history and creates a backup before correcting loans.

begin;

-- 1) Give both suspense sources a proper audited dismissal state.
alter table if exists public.mpesa_callback_queue
  add column if not exists dismissed boolean not null default false,
  add column if not exists dismissed_at timestamptz,
  add column if not exists dismissed_by text;

alter table if exists public.unmatched_payments
  add column if not exists dismissed boolean not null default false,
  add column if not exists dismissed_at timestamptz,
  add column if not exists dismissed_by text;

create index if not exists mpesa_callback_queue_pending_suspense_idx
  on public.mpesa_callback_queue (dismissed, confirmed, created_at desc);

create index if not exists unmatched_payments_pending_suspense_idx
  on public.unmatched_payments (business_id, dismissed, resolved, created_at desc);

-- 2) Back up every loan whose stored totals disagree with its repayment ledger.
create table if not exists public.bripta_balance_fix_backup_20260731 as
with repayment_totals as (
  select loan_id, round(coalesce(sum(amount), 0)::numeric, 2) as repayment_total
  from public.loan_repayments
  group by loan_id
)
select
  l.*,
  coalesce(rt.repayment_total, 0)::numeric as ledger_repayment_total,
  round(greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric), 2) as ledger_expected_balance,
  now() as backed_up_at
from public.loans l
left join repayment_totals rt on rt.loan_id = l.id
where abs(coalesce(l.total_paid, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) > 0.01
   or abs(
        coalesce(l.outstanding_balance, 0)::numeric
        - greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric)
      ) > 0.01
   or (
        greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) <= 0.01
        and l.status <> 'completed'
      )
   or (
        greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric) > 0.01
        and l.status = 'completed'
      );

-- 3) Correct all existing mismatched loans, including loan 385693 if affected.
with repayment_totals as (
  select loan_id, round(coalesce(sum(amount), 0)::numeric, 2) as repayment_total
  from public.loan_repayments
  group by loan_id
),
corrected as (
  select
    l.id,
    coalesce(rt.repayment_total, 0)::numeric as repayment_total,
    round(greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric), 2) as expected_balance
  from public.loans l
  left join repayment_totals rt on rt.loan_id = l.id
)
update public.loans l
set
  total_paid = c.repayment_total,
  outstanding_balance = c.expected_balance,
  status = case
    when c.expected_balance <= 0.01 then 'completed'
    when l.status = 'completed' then 'active'
    else l.status
  end,
  arrears_amount = case
    when c.expected_balance <= 0.01 then 0
    else least(greatest(0, coalesce(l.arrears_amount, 0)::numeric), c.expected_balance)
  end,
  overdue_days = case when c.expected_balance <= 0.01 then 0 else coalesce(l.overdue_days, 0) end
from corrected c
where l.id = c.id
  and (
    abs(coalesce(l.total_paid, 0)::numeric - c.repayment_total) > 0.01
    or abs(coalesce(l.outstanding_balance, 0)::numeric - c.expected_balance) > 0.01
    or (c.expected_balance <= 0.01 and l.status <> 'completed')
    or (c.expected_balance > 0.01 and l.status = 'completed')
  );

update public.loan_schedules s
set
  total_paid = greatest(coalesce(s.total_paid, 0), coalesce(s.total_due, 0)),
  status = 'paid',
  paid_at = coalesce(s.paid_at, now())
from public.loans l
where s.loan_id = l.id
  and l.status = 'completed'
  and coalesce(l.outstanding_balance, 0) <= 0.01
  and (
    s.status <> 'paid'
    or coalesce(s.total_paid, 0) < coalesce(s.total_due, 0)
  );

-- 4) Keep loan totals synchronized whenever a repayment is inserted, edited,
-- moved to another loan, or deleted.
create or replace function public.bripta_sync_loan_balance_from_repayments()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  new_loan_id public.loans.id%type;
  old_loan_id public.loans.id%type;
begin
  if tg_op <> 'DELETE' then
    new_loan_id := new.loan_id;
  end if;
  if tg_op <> 'INSERT' then
    old_loan_id := old.loan_id;
  end if;

  if new_loan_id is not null then
    update public.loans l
    set
      total_paid = x.repayment_total,
      outstanding_balance = x.expected_balance,
      status = case
        when x.expected_balance <= 0.01 then 'completed'
        when l.status = 'completed' then 'active'
        else l.status
      end,
      arrears_amount = case
        when x.expected_balance <= 0.01 then 0
        else least(greatest(0, coalesce(l.arrears_amount, 0)::numeric), x.expected_balance)
      end,
      overdue_days = case when x.expected_balance <= 0.01 then 0 else coalesce(l.overdue_days, 0) end
    from (
      select
        round(coalesce(sum(r.amount), 0)::numeric, 2) as repayment_total,
        round(greatest(
          0,
          coalesce((select total_payable from public.loans where id = new_loan_id), 0)::numeric
          - coalesce(sum(r.amount), 0)::numeric
        ), 2) as expected_balance
      from public.loan_repayments r
      where r.loan_id = new_loan_id
    ) x
    where l.id = new_loan_id;
  end if;

  if old_loan_id is not null and old_loan_id is distinct from new_loan_id then
    update public.loans l
    set
      total_paid = x.repayment_total,
      outstanding_balance = x.expected_balance,
      status = case
        when x.expected_balance <= 0.01 then 'completed'
        when l.status = 'completed' then 'active'
        else l.status
      end,
      arrears_amount = case
        when x.expected_balance <= 0.01 then 0
        else least(greatest(0, coalesce(l.arrears_amount, 0)::numeric), x.expected_balance)
      end,
      overdue_days = case when x.expected_balance <= 0.01 then 0 else coalesce(l.overdue_days, 0) end
    from (
      select
        round(coalesce(sum(r.amount), 0)::numeric, 2) as repayment_total,
        round(greatest(
          0,
          coalesce((select total_payable from public.loans where id = old_loan_id), 0)::numeric
          - coalesce(sum(r.amount), 0)::numeric
        ), 2) as expected_balance
      from public.loan_repayments r
      where r.loan_id = old_loan_id
    ) x
    where l.id = old_loan_id;
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;

drop trigger if exists bripta_sync_loan_balance_trigger on public.loan_repayments;
create trigger bripta_sync_loan_balance_trigger
after insert or update or delete
on public.loan_repayments
for each row
execute function public.bripta_sync_loan_balance_from_repayments();

commit;

-- 5) Final report. balance_difference should be 0.00 for every returned row.
with repayment_totals as (
  select loan_id, round(coalesce(sum(amount), 0)::numeric, 2) as repayment_total
  from public.loan_repayments
  group by loan_id
)
select
  c.full_name,
  c.phone,
  l.loan_no,
  l.status,
  l.total_payable,
  l.total_paid,
  coalesce(rt.repayment_total, 0) as repayment_total,
  l.outstanding_balance,
  round(
    l.outstanding_balance
    - greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric),
    2
  ) as balance_difference
from public.loans l
join public.loan_clients c on c.id = l.client_id
left join repayment_totals rt on rt.loan_id = l.id
where l.loan_no = '385693'
   or right(regexp_replace(coalesce(c.phone, ''), '\D', '', 'g'), 9) = '707707311'
   or abs(
        coalesce(l.outstanding_balance, 0)::numeric
        - greatest(0, coalesce(l.total_payable, 0)::numeric - coalesce(rt.repayment_total, 0)::numeric)
      ) > 0.01
order by c.full_name, l.loan_no;
