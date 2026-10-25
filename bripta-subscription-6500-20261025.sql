-- Bripta subscription price from November 2026. Run in the Bripta Supabase
-- project before deploying the matching payment functions and frontend.
-- Existing billing rows, receipts, amounts and payment statuses are untouched.

create or replace function public.bripta_subscription_amount(
  p_billing_month date,
  p_business_id text
)
returns numeric
language sql immutable
set search_path = public
as $$
  select case
    when date_trunc('month', p_billing_month)::date < date '2026-11-01' then 3000::numeric
    when p_business_id = 'BIZ-B3F5E5D9' then 6500::numeric
    else 7500::numeric
  end
$$;

-- Keep the original one-argument function for any legacy callers. The billing
-- trigger now uses the business-aware overload to avoid repricing other firms.
create or replace function public.bripta_enforce_subscription_amount()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  v_month date;
  v_required numeric;
begin
  -- Existing paid periods retain the amounts that were actually received.
  if tg_op = 'UPDATE' then
    if old.status in ('paid', 'completed') then
      return new;
    end if;
  end if;

  v_month := coalesce(new.billing_month, current_date);
  v_required := public.bripta_subscription_amount(v_month, new.business_id);
  if coalesce(new.status, 'pending') not in ('paid', 'completed') then
    new.amount := v_required;
  elsif coalesce(new.amount, 0) < v_required then
    raise exception 'Subscription payment is below the required amount for %', v_month;
  end if;
  return new;
end
$$;

do $$
begin
  if to_regclass('public.loan_billing_cycles') is not null then
    drop trigger if exists bripta_subscription_amount_trg on public.loan_billing_cycles;
    create trigger bripta_subscription_amount_trg
      before insert or update of amount, status, billing_month
      on public.loan_billing_cycles
      for each row execute function public.bripta_enforce_subscription_amount();
  end if;
end
$$;

-- Read-only verification; no billing rows are changed by this migration.
select 'Bripta October 2026' as billing_period,
       public.bripta_subscription_amount(date '2026-10-01', 'BIZ-B3F5E5D9') as amount_kes
union all
select 'Bripta November 2026',
       public.bripta_subscription_amount(date '2026-11-01', 'BIZ-B3F5E5D9')
union all
select 'Other business November 2026',
       public.bripta_subscription_amount(date '2026-11-01', 'OTHER_BUSINESS');
