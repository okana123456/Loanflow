-- Bripta / Loanflow
-- Low-egress incremental synchronization support.
--
-- This file does not delete, archive, reclassify, recalculate, or move money.
-- It only adds updated_at tracking and indexes so the app can request records
-- changed since the last successful sync on each device.

begin;

create or replace function public.bripta_touch_updated_at()
returns trigger
language plpgsql
set search_path = public
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

do $$
declare
  v_table_name text;
  has_created_at boolean;
  has_business_id boolean;
begin
  foreach v_table_name in array array[
    'loan_clients',
    'loans',
    'loan_schedules',
    'loan_repayments',
    'loan_applications',
    'loan_staff',
    'loan_products',
    'loan_penalties',
    'bripta_excess_ledger',
    'bripta_charges',
    'unmatched_payments'
  ]
  loop
    if to_regclass('public.' || v_table_name) is null then
      continue;
    end if;

    execute format(
      'alter table public.%I add column if not exists updated_at timestamptz',
      v_table_name
    );

    select exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and information_schema.columns.table_name = v_table_name
        and column_name = 'created_at'
    ) into has_created_at;

    if has_created_at then
      execute format(
        'update public.%I set updated_at = coalesce(updated_at, created_at, now()) where updated_at is null',
        v_table_name
      );
    else
      execute format(
        'update public.%I set updated_at = coalesce(updated_at, now()) where updated_at is null',
        v_table_name
      );
    end if;

    execute format(
      'alter table public.%I alter column updated_at set default now()',
      v_table_name
    );
    execute format(
      'alter table public.%I alter column updated_at set not null',
      v_table_name
    );

    execute format(
      'drop trigger if exists bripta_touch_updated_at_trigger on public.%I',
      v_table_name
    );
    execute format(
      'create trigger bripta_touch_updated_at_trigger before update on public.%I for each row execute function public.bripta_touch_updated_at()',
      v_table_name
    );

    select exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and information_schema.columns.table_name = v_table_name
        and column_name = 'business_id'
    ) into has_business_id;

    if has_business_id then
      execute format(
        'create index if not exists %I on public.%I (business_id, updated_at desc)',
        v_table_name || '_business_updated_idx',
        v_table_name
      );
    else
      execute format(
        'create index if not exists %I on public.%I (updated_at desc)',
        v_table_name || '_updated_idx',
        v_table_name
      );
    end if;
  end loop;
end;
$$;

commit;

select
  'Bripta incremental synchronization is ready' as result,
  count(*) as tracked_tables,
  false as business_data_deleted,
  false as financial_values_changed
from information_schema.columns
where table_schema = 'public'
  and column_name = 'updated_at'
  and table_name = any(array[
    'loan_clients',
    'loans',
    'loan_schedules',
    'loan_repayments',
    'loan_applications',
    'loan_staff',
    'loan_products',
    'loan_penalties',
    'bripta_excess_ledger',
    'bripta_charges',
    'unmatched_payments'
  ]);
