-- Bripta: repair repeated rollovers, cap arrears, and enforce one rollover per loan.
-- Run this entire file once in the Supabase SQL Editor.

begin;

-- Classify identical same-day rows as one concurrent event. A later distinct
-- date/amount/reason is a separate rollover event and must be reversed once.
create temporary table bripta_rollover_repair_rows on commit drop as
select
  p.id,
  p.loan_id,
  p.penalty_amount,
  p.reason,
  p.date_charged,
  coalesce(p.is_waived, false) as is_waived,
  row_number() over (
    partition by p.loan_id
    order by p.date_charged asc nulls last, p.id asc
  ) as overall_row_no,
  dense_rank() over (
    partition by p.loan_id
    order by p.date_charged asc nulls last,
      coalesce(p.reason, ''), round(coalesce(p.penalty_amount, 0)::numeric, 6)
  ) as event_no,
  row_number() over (
    partition by p.loan_id, p.date_charged, coalesce(p.reason, ''),
      round(coalesce(p.penalty_amount, 0)::numeric, 6)
    order by p.id asc
  ) as event_row_no
from public.loan_penalties p
where lower(coalesce(p.reason, '')) like '%rollover%';

-- Only a later distinct active event changed the loan balance again. Identical
-- concurrent rows share one balance update, so they must not be subtracted.
create temporary table bripta_rollover_financial_adjustments on commit drop as
select
  loan_id,
  round(sum(penalty_amount)::numeric, 2) as amount_to_reverse
from bripta_rollover_repair_rows
where event_no > 1
  and event_row_no = 1
  and not is_waived
group by loan_id;

update public.loans l
set
  total_payable = round(greatest(0, coalesce(l.total_payable, 0) - a.amount_to_reverse), 2),
  outstanding_balance = round(greatest(0, coalesce(l.outstanding_balance, 0) - a.amount_to_reverse), 2),
  arrears_amount = round(least(
    greatest(0, coalesce(l.arrears_amount, 0)),
    greatest(0, coalesce(l.outstanding_balance, 0) - a.amount_to_reverse)
  ), 2),
  status = case
    when greatest(0, coalesce(l.outstanding_balance, 0) - a.amount_to_reverse) <= 0.01
      then 'completed'
    else l.status
  end
from bripta_rollover_financial_adjustments a
where l.id = a.loan_id;

-- Preserve the rows for audit, but make every row after the first clearly
-- non-billable and remove the marker used by the permanent unique guard.
update public.loan_penalties p
set
  is_waived = true,
  waived_reason = concat(
    'System repair 2026-09-17: duplicate/extra one-time penalty removed. Original reason: ',
    coalesce(p.reason, '(blank)')
  ),
  reason = 'Removed duplicate or extra penalty - system repair 2026-09-17'
from bripta_rollover_repair_rows r
where p.id = r.id
  and r.overall_row_no > 1;

-- Remove exact duplicate schedules created by concurrent browser sessions.
with ranked_schedules as (
  select
    s.id,
    row_number() over (
      partition by s.loan_id, s.installment_no, s.due_date,
        round(coalesce(s.principal_due, 0)::numeric, 6),
        round(coalesce(s.interest_due, 0)::numeric, 6),
        round(coalesce(s.total_due, 0)::numeric, 6),
        round(coalesce(s.total_paid, 0)::numeric, 6),
        coalesce(s.status, '')
      order by s.id asc
    ) as duplicate_row_no
  from public.loan_schedules s
  where exists (
    select 1
    from bripta_rollover_repair_rows r
    where r.loan_id = s.loan_id and r.overall_row_no > 1
  )
)
delete from public.loan_schedules s
using ranked_schedules r
where s.id = r.id
  and r.duplicate_row_no > 1;

-- A later distinct rollover replaced the first rollover schedule. Remove that
-- invalid replacement and send the remaining balance directly to arrears/NPL.
with later_event_loans as (
  select distinct loan_id
  from bripta_rollover_repair_rows
  where event_no > 1
), first_rollover as (
  select loan_id, min(date_charged)::date as first_rollover_date
  from bripta_rollover_repair_rows
  group by loan_id
)
delete from public.loan_schedules s
using later_event_loans x
where s.loan_id = x.loan_id
  and s.status in ('pending', 'partial', 'overdue');

with later_event_loans as (
  select distinct loan_id
  from bripta_rollover_repair_rows
  where event_no > 1
), first_rollover as (
  select loan_id, min(date_charged)::date as first_rollover_date
  from bripta_rollover_repair_rows
  group by loan_id
)
update public.loans l
set
  maturity_date = f.first_rollover_date + 21,
  arrears_amount = case
    when coalesce(l.outstanding_balance, 0) <= 0.01 then 0
    else round(greatest(0, l.outstanding_balance), 2)
  end,
  overdue_days = case
    when coalesce(l.outstanding_balance, 0) <= 0.01 then 0
    else greatest(0, current_date - (f.first_rollover_date + 21))
  end
from later_event_loans x
join first_rollover f on f.loan_id = x.loan_id
where l.id = x.loan_id;

-- Arrears can never exceed what the client still owes.
update public.loans
set arrears_amount = round(least(
  greatest(0, coalesce(arrears_amount, 0)),
  greatest(0, coalesce(outstanding_balance, 0))
), 2)
where coalesce(arrears_amount, 0) < 0
   or coalesce(arrears_amount, 0) > greatest(0, coalesce(outstanding_balance, 0));

-- Permanent database guard: one rollover marker in the lifetime of a loan.
create unique index if not exists loan_penalties_one_rollover_per_loan_uidx
  on public.loan_penalties (loan_id)
  where lower(coalesce(reason, '')) like '%rollover%';

-- Run the complete rollover operation inside one transaction. FOR UPDATE
-- serializes simultaneous sessions; the unique index is the final safeguard.
create or replace function public.bripta_apply_my_rollover_penalties()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_business_id text;
  v_penalty_pct numeric := 15;
  v_loan record;
  v_penalty numeric;
  v_new_balance numeric;
  v_new_total numeric;
  v_weekly numeric;
  v_base_installment integer;
  v_rollovers integer := 0;
  v_moved_to_arrears integer := 0;
begin
  select s.business_id into v_business_id
  from public.loan_staff s
  where s.auth_user_id = auth.uid()
    and coalesce(s.is_active, true) = true
  limit 1;

  if v_business_id is null then
    raise exception 'Active Bripta staff account not found';
  end if;

  select coalesce(nullif(ls.default_late_penalty_pct, 0), 15)
  into v_penalty_pct
  from public.loan_settings ls
  where ls.business_id = v_business_id
  limit 1;
  v_penalty_pct := coalesce(v_penalty_pct, 15);

  for v_loan in
    select l.*, c.full_name
    from public.loans l
    left join public.loan_clients c on c.id = l.client_id
    where l.business_id = v_business_id
      and l.status = 'active'
    order by l.id
    for update of l
  loop
    if coalesce(v_loan.outstanding_balance, 0) <= 0.01 then
      update public.loans
      set outstanding_balance = 0, arrears_amount = 0,
          overdue_days = 0, status = 'completed'
      where id = v_loan.id;
      update public.loan_schedules
      set penalty_charged = 0, status = 'paid'
      where loan_id = v_loan.id
        and status in ('pending', 'partial', 'overdue');
      continue;
    end if;

    if lower(coalesce(v_loan.full_name, '')) similar to
      '%(judith nyaranga|fredrick onyango|lensa apiyo|irine odinya|trizah atieno)%'
    then
      continue;
    end if;

    if v_loan.maturity_date is null
       or current_date - v_loan.maturity_date::date <= 2 then
      continue;
    end if;

    if exists (
      select 1 from public.loan_penalties p
      where p.loan_id = v_loan.id
        and lower(coalesce(p.reason, '')) like '%rollover%'
    ) then
      update public.loans
      set
        arrears_amount = round(greatest(0, coalesce(outstanding_balance, 0)), 2),
        overdue_days = greatest(0, current_date - maturity_date::date)
      where id = v_loan.id;
      v_moved_to_arrears := v_moved_to_arrears + 1;
      continue;
    end if;

    v_penalty := round(coalesce(v_loan.outstanding_balance, 0) * v_penalty_pct / 100, 2);
    v_new_balance := round(coalesce(v_loan.outstanding_balance, 0) + v_penalty, 2);
    v_new_total := round(coalesce(v_loan.total_payable, 0) + v_penalty, 2);
    v_weekly := v_new_balance / 3;

    insert into public.loan_penalties (
      loan_id, business_id, penalty_amount, reason, date_charged, is_waived
    ) values (
      v_loan.id, v_business_id, v_penalty,
      'Rollover Penalty (' || v_penalty_pct || '%) - ' || current_date,
      current_date, false
    );

    delete from public.loan_schedules
    where loan_id = v_loan.id
      and status in ('pending', 'partial', 'overdue');

    select coalesce(max(installment_no), 0)
    into v_base_installment
    from public.loan_schedules
    where loan_id = v_loan.id;

    update public.loans
    set
      outstanding_balance = v_new_balance,
      total_payable = v_new_total,
      arrears_amount = 0,
      overdue_days = 0,
      maturity_date = current_date + 21
    where id = v_loan.id;

    insert into public.loan_schedules (
      loan_id, business_id, installment_no, due_date,
      principal_due, interest_due, total_due,
      principal_paid, interest_paid, total_paid, penalty_charged, status
    )
    select
      v_loan.id, v_business_id, v_base_installment + n,
      current_date + (n * 7), v_weekly, 0, v_weekly,
      0, 0, 0, 0, 'pending'
    from generate_series(1, 3) as n;

    v_rollovers := v_rollovers + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'new_rollovers', v_rollovers,
    'existing_rollovers_moved_to_arrears', v_moved_to_arrears
  );
end;
$$;

revoke all on function public.bripta_apply_my_rollover_penalties() from public, anon;
grant execute on function public.bripta_apply_my_rollover_penalties() to authenticated;

commit;

-- Verification output. All counts should be zero, and Lilian's arrears must
-- be no higher than her outstanding balance.
select
  'loans_with_multiple_rollover_markers' as check_name,
  count(*)::numeric as result
from (
  select loan_id
  from public.loan_penalties
  where lower(coalesce(reason, '')) like '%rollover%'
  group by loan_id
  having count(*) > 1
) x
union all
select
  'active_loans_arrears_above_balance',
  count(*)::numeric
from public.loans
where status = 'active'
  and coalesce(arrears_amount, 0) > greatest(0, coalesce(outstanding_balance, 0)) + 0.01
union all
select
  'lilian_akoth_akumu_outstanding',
  coalesce(max(l.outstanding_balance), 0)::numeric
from public.loans l
join public.loan_clients c on c.id = l.client_id
where lower(c.full_name) = 'lilian akoth akumu'
  and l.status = 'active'
union all
select
  'lilian_akoth_akumu_arrears',
  coalesce(max(l.arrears_amount), 0)::numeric
from public.loans l
join public.loan_clients c on c.id = l.client_id
where lower(c.full_name) = 'lilian akoth akumu'
  and l.status = 'active';
