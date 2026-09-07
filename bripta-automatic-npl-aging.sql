-- Bripta automatic arrears and NPL aging
-- Run once in the Bripta Supabase SQL Editor.
-- NPL begins at 180 overdue days. This changes only arrears/aging metadata;
-- balances, repayments, schedules and historical financial values are untouched.

begin;

create or replace function public.bripta_refresh_aging_for_business(p_business_id text)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_updated integer := 0;
  v_npl integer := 0;
begin
  with schedule_aging as (
    select
      l.id as loan_id,
      least(
        greatest(0, coalesce(l.outstanding_balance, 0)),
        greatest(0, coalesce(sum(greatest(0, coalesce(s.total_due, 0) - coalesce(s.total_paid, 0)))
          filter (where s.due_date < current_date), 0))
      )::numeric as calculated_arrears,
      min(s.due_date) filter (
        where s.due_date < current_date
          and greatest(0, coalesce(s.total_due, 0) - coalesce(s.total_paid, 0)) > 0.01
      ) as oldest_unpaid_due
    from public.loans l
    left join public.loan_schedules s on s.loan_id = l.id
    where l.business_id = p_business_id
      and l.status = 'active'
      and coalesce(l.outstanding_balance, 0) > 0.01
    group by l.id, l.outstanding_balance
  ), refreshed as (
    update public.loans l
    set
      arrears_amount = round(a.calculated_arrears, 2),
      overdue_days = case
        when a.calculated_arrears <= 0.01 or a.oldest_unpaid_due is null then 0
        else greatest(0, current_date - a.oldest_unpaid_due)
      end
    from schedule_aging a
    where l.id = a.loan_id
      and (
        l.arrears_amount is distinct from round(a.calculated_arrears, 2)
        or l.overdue_days is distinct from case
          when a.calculated_arrears <= 0.01 or a.oldest_unpaid_due is null then 0
          else greatest(0, current_date - a.oldest_unpaid_due)
        end
      )
    returning l.id
  )
  select count(*) into v_updated from refreshed;

  select count(*) into v_npl
  from public.loans
  where business_id = p_business_id
    and status = 'active'
    and coalesce(outstanding_balance, 0) > 0.01
    and coalesce(arrears_amount, 0) > 0.01
    and coalesce(overdue_days, 0) >= 180;

  return jsonb_build_object(
    'ok', true,
    'business_id', p_business_id,
    'aging_rows_updated', v_updated,
    'npl_accounts', v_npl,
    'npl_threshold_days', 180,
    'financial_values_changed', false
  );
end;
$$;

revoke all on function public.bripta_refresh_aging_for_business(text) from public, anon, authenticated;

create or replace function public.bripta_refresh_my_loan_aging()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_business_id text;
begin
  select s.business_id into v_business_id
  from public.loan_staff s
  where s.auth_user_id = auth.uid()
    and coalesce(s.is_active, true) = true
  limit 1;

  if v_business_id is null then
    raise exception 'Active Bripta staff account not found';
  end if;

  return public.bripta_refresh_aging_for_business(v_business_id);
end;
$$;

revoke all on function public.bripta_refresh_my_loan_aging() from public, anon;
grant execute on function public.bripta_refresh_my_loan_aging() to authenticated;

-- Refresh all businesses for the daily scheduler. It is not callable by app users.
create or replace function public.bripta_refresh_all_loan_aging()
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_business record;
begin
  for v_business in
    select distinct business_id
    from public.loans
    where status = 'active' and business_id is not null
  loop
    perform public.bripta_refresh_aging_for_business(v_business.business_id);
  end loop;
end;
$$;

revoke all on function public.bripta_refresh_all_loan_aging() from public, anon, authenticated;
grant execute on function public.bripta_refresh_all_loan_aging() to service_role;

create index if not exists loan_schedules_aging_lookup_idx
  on public.loan_schedules (loan_id, due_date)
  include (total_due, total_paid);

-- Supabase provides pg_cron. Replace any older job with the same name so this
-- file remains safe to run more than once.
create extension if not exists pg_cron with schema extensions;
do $$
declare
  v_job_id bigint;
begin
  select jobid into v_job_id from cron.job where jobname = 'bripta-daily-loan-aging' limit 1;
  if v_job_id is not null then
    perform cron.unschedule(v_job_id);
  end if;
  perform cron.schedule(
    'bripta-daily-loan-aging',
    '10 0 * * *',
    'select public.bripta_refresh_all_loan_aging();'
  );
end;
$$;

commit;

-- Run once now and return a clear verification report.
select public.bripta_refresh_all_loan_aging();

select
  1 as section_order,
  'automatic_npl_aging' as section,
  jsonb_build_object(
    'result', 'Bripta automatic arrears and NPL aging is ready',
    'npl_threshold_days', 180,
    'active_npl_accounts', count(*) filter (
      where l.status = 'active'
        and coalesce(l.outstanding_balance, 0) > 0.01
        and coalesce(l.arrears_amount, 0) > 0.01
        and coalesce(l.overdue_days, 0) >= 180
    ),
    'daily_job_installed', exists (
      select 1 from cron.job where jobname = 'bripta-daily-loan-aging'
    ),
    'loan_balances_changed', false,
    'repayments_changed', false
  ) as result
from public.loans l;
