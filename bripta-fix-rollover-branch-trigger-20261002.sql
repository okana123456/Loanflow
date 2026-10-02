-- Fix error 42703: record "new" has no field "client_id" during rollover.
-- The same branch trigger serves tables with different column sets. Check
-- the table name before reading a table-specific NEW field.
-- Existing data and financial amounts are not changed. Safe to rerun.

begin;

create or replace function public.bripta_assign_branch()
returns trigger
language plpgsql
security definer
set search_path=public
as $$
declare v_branch uuid;
begin
  if new.branch_id is not null then return new; end if;

  if tg_table_name='mpesa_callback_queue' then
    select id into v_branch
    from public.bripta_branches
    where business_id='SYSTEM' and is_head_office
    limit 1;
    new.branch_id:=v_branch;
    return new;
  end if;

  if tg_table_name in ('loan_applications','loans') then
    if new.client_id is not null then
      select branch_id into v_branch
      from public.loan_clients where id=new.client_id;
    end if;
  elsif tg_table_name in ('loan_schedules','loan_repayments','loan_penalties') then
    if new.loan_id is not null then
      select branch_id into v_branch
      from public.loans where id=new.loan_id;
    end if;
  end if;

  if v_branch is null then
    select branch_id into v_branch
    from public.loan_staff where auth_user_id=auth.uid() limit 1;
  end if;
  if v_branch is null then
    select id into v_branch from public.bripta_branches
    where id='00000000-0000-4000-8000-000000000001';
  end if;
  new.branch_id:=v_branch;
  return new;
end $$;

commit;

select 'branch_trigger_repaired' as check_name,
  position('and new.client_id' in lower(pg_get_functiondef('public.bripta_assign_branch()'::regprocedure)))=0 as result;
