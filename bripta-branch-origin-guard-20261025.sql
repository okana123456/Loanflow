-- Run in the Bripta Supabase project only. This changes branch attribution on
-- future inserts; it does not update an existing record or financial amount.
begin;

create or replace function public.bripta_set_origin_branch()
returns trigger
language plpgsql security definer set search_path=public
as $$
declare
  v_branch uuid;
  v_business text;
  v_staff public.loan_staff%rowtype;
begin
  if new.business_id <> 'BIZ-B3F5E5D9' then
    return new;
  end if;

  if tg_table_name = 'loan_clients' then
    select * into v_staff from public.loan_staff
    where auth_user_id = auth.uid() and business_id = new.business_id
      and coalesce(is_active,true)
    limit 1;
    if v_staff.id is not null
       and 'loan_officer' = any(string_to_array(replace(coalesce(v_staff.role,''),' ',''),','))
       and not ('admin' = any(string_to_array(replace(coalesce(v_staff.role,''),' ',''),',')))
       and not ('branch_manager' = any(string_to_array(replace(coalesce(v_staff.role,''),' ',''),','))) then
      new.branch_id := v_staff.branch_id;
    end if;
    if new.branch_id is null then raise exception 'Client branch is required'; end if;
    return new;
  end if;

  if tg_table_name in ('loan_applications','loans') then
    select branch_id,business_id into v_branch,v_business
    from public.loan_clients where id = new.client_id;
  elsif tg_table_name in ('loan_schedules','loan_repayments') then
    select branch_id,business_id into v_branch,v_business
    from public.loans where id = new.loan_id;
  elsif tg_table_name = 'bripta_charges' then
    if new.loan_id is not null then
      select branch_id,business_id into v_branch,v_business
      from public.loans where id = new.loan_id;
    else
      select branch_id,business_id into v_branch,v_business
      from public.loan_clients where id = new.client_id;
    end if;
  end if;
  if v_branch is null or v_business is distinct from new.business_id then
    raise exception 'Cannot determine a matching Bripta branch for %',tg_table_name;
  end if;
  new.branch_id := v_branch;
  return new;
end $$;

-- Transfers change the selected clients' current assignment only. Existing
-- disbursed loans, repayments, schedules and their ledger entries keep their
-- original branch and officer history. Pending applications follow the client.
create or replace function public.bripta_transfer_portfolio(
  p_from uuid,p_to uuid,p_client_ids uuid[] default null,p_move_all boolean default false
)
returns jsonb language plpgsql security definer set search_path=public as $$
declare
  v_actor public.loan_staff%rowtype;
  v_source public.loan_staff%rowtype;
  v_target public.loan_staff%rowtype;
  v_count integer;
begin
  select * into v_actor from public.loan_staff
    where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1;
  if v_actor.id is null or not public.bripta_has_permission('transfer_portfolios') then
    raise exception 'Portfolio transfer is not permitted';
  end if;
  select * into v_source from public.loan_staff
    where id=p_from and business_id=v_actor.business_id;
  select * into v_target from public.loan_staff
    where id=p_to and business_id=v_actor.business_id and coalesce(is_active,true);
  if v_source.id is null or v_target.id is null or v_target.branch_id is null then
    raise exception 'Source or target officer is unavailable in this business';
  end if;
  if not p_move_all and coalesce(array_length(p_client_ids,1),0)=0 then
    raise exception 'Select at least one client';
  end if;
  with moved as (
    update public.loan_clients c
    set loan_officer_id=p_to,branch_id=v_target.branch_id,updated_at=now()
    where c.business_id=v_actor.business_id
      and c.loan_officer_id=p_from
      and public.bripta_can_access_branch(c.branch_id)
      and (p_move_all or c.id=any(p_client_ids))
    returning c.id
  ), pending_apps as (
    update public.loan_applications a
    set loan_officer_id=p_to,branch_id=v_target.branch_id,updated_at=now()
    where a.business_id=v_actor.business_id
      and a.client_id in (select id from moved)
      and a.status in ('draft','submitted','under_review','approved')
    returning a.id
  )
  select count(*) into v_count from moved;
  insert into public.bripta_domain_audit(
    business_id,branch_id,actor_staff_id,action,entity_type,new_value
  ) values (
    v_actor.business_id,v_target.branch_id,v_actor.id,
    'portfolio_transferred','loan_clients',
    jsonb_build_object('from',p_from,'to',p_to,'clients',v_count,
      'historical_loans_preserved',true)
  );
  return jsonb_build_object('ok',true,'clients_moved',v_count);
end $$;

do $$
declare v_table text;
begin
  foreach v_table in array array[
    'loan_clients','loan_applications','loans','loan_schedules',
    'loan_repayments','bripta_charges'
  ] loop
    execute format('drop trigger if exists bripta_set_origin_branch_trg on public.%I',v_table);
    execute format('create trigger bripta_set_origin_branch_trg before insert on public.%I for each row execute function public.bripta_set_origin_branch()',v_table);
  end loop;
end $$;

revoke all on function public.bripta_set_origin_branch() from public,anon;
commit;
