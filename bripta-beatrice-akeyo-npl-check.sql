-- Bripta: read-only NPL check for Beatrice Akeyo
-- This query does not update balances, repayments, schedules or loan status.

with matching_clients as (
  select c.id, c.business_id, c.full_name, c.phone
  from public.loan_clients c
  where c.full_name ilike '%Beatrice%'
    and c.full_name ilike '%Akeyo%'
), loan_checks as (
  select
    c.id as client_id,
    c.business_id,
    c.full_name,
    c.phone,
    l.id as loan_id,
    l.loan_no,
    l.status as loan_status,
    l.disbursement_date,
    l.maturity_date,
    coalesce(l.outstanding_balance, 0)::numeric as outstanding_balance,
    coalesce(l.arrears_amount, 0)::numeric as stored_arrears,
    coalesce(l.overdue_days, 0)::integer as stored_overdue_days,
    s.oldest_unpaid_due,
    coalesce(s.schedule_arrears, 0)::numeric as schedule_arrears,
    case
      when s.oldest_unpaid_due is not null then current_date - s.oldest_unpaid_due::date
      when l.maturity_date is not null and l.maturity_date::date < current_date
        then current_date - l.maturity_date::date
      else 0
    end::integer as calculated_overdue_days,
    coalesce(p.rollover_count, 0)::integer as rollover_count,
    p.latest_rollover_date
  from matching_clients c
  join public.loans l on l.client_id = c.id and l.business_id = c.business_id
  left join lateral (
    select
      min(ls.due_date::date) filter (
        where ls.due_date::date < current_date
          and greatest(0, coalesce(ls.total_due, 0) - coalesce(ls.total_paid, 0)) > 0.01
      ) as oldest_unpaid_due,
      coalesce(sum(
        greatest(0, coalesce(ls.total_due, 0) - coalesce(ls.total_paid, 0))
      ) filter (where ls.due_date::date < current_date), 0) as schedule_arrears
    from public.loan_schedules ls
    where ls.loan_id = l.id
  ) s on true
  left join lateral (
    select
      count(*) filter (where lp.reason ilike '%rollover%') as rollover_count,
      max(lp.date_charged) filter (where lp.reason ilike '%rollover%') as latest_rollover_date
    from public.loan_penalties lp
    where lp.loan_id = l.id
  ) p on true
)
select
  1 as section_order,
  'Beatrice Akeyo NPL check' as section,
  coalesce(jsonb_agg(jsonb_build_object(
    'client_id', client_id,
    'client_name', full_name,
    'phone', phone,
    'loan_id', loan_id,
    'loan_no', loan_no,
    'loan_status', loan_status,
    'disbursement_date', disbursement_date,
    'maturity_date', maturity_date,
    'outstanding_balance', outstanding_balance,
    'stored_arrears', stored_arrears,
    'schedule_arrears', schedule_arrears,
    'stored_overdue_days', stored_overdue_days,
    'oldest_unpaid_installment', oldest_unpaid_due,
    'calculated_overdue_days', calculated_overdue_days,
    'should_be_npl', loan_status = 'active'
      and outstanding_balance > 0.01
      and schedule_arrears > 0.01
      and calculated_overdue_days >= 180,
    'rollover_count', rollover_count,
    'latest_rollover_date', latest_rollover_date,
    'browser_rollover_exception_found', true
  ) order by disbursement_date desc nulls last), '[]'::jsonb) as result
from loan_checks;
