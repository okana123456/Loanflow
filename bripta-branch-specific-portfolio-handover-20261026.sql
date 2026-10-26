-- Bripta only. Install a branch-specific staff handover. Installing this
-- function moves no records. An admin must later choose a source officer,
-- the source portfolio branch, and an active officer IN THAT SAME BRANCH.
-- Active loans and pending applications follow the selected clients; branch,
-- repayment, schedule, accounting and financial values stay unchanged.

create or replace function public.bripta_handover_branch_portfolio(
  p_from uuid, p_to uuid, p_branch uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor public.loan_staff%rowtype;
  v_source public.loan_staff%rowtype;
  v_target public.loan_staff%rowtype;
  v_expected_clients integer;
  v_client_count integer;
  v_loan_count integer;
  v_application_count integer;
  v_loans_before jsonb;
  v_loans_after jsonb;
begin
  select * into v_actor from public.loan_staff
  where auth_user_id=auth.uid() and business_id='BIZ-B3F5E5D9'
    and is_active=true limit 1;
  if v_actor.id is null or not public.bripta_has_permission('transfer_portfolios') then
    raise exception 'Bripta portfolio transfer permission is required';
  end if;
  if p_from is null or p_to is null or p_branch is null or p_from=p_to then
    raise exception 'Choose a source officer, a different target officer and a branch';
  end if;
  if not exists(select 1 from public.bripta_branches
    where id=p_branch and business_id='BIZ-B3F5E5D9' and is_active=true) then
    raise exception 'The selected Bripta branch is unavailable';
  end if;
  select * into v_source from public.loan_staff
  where id=p_from and business_id='BIZ-B3F5E5D9';
  select * into v_target from public.loan_staff
  where id=p_to and business_id='BIZ-B3F5E5D9' and is_active=true
    and branch_id=p_branch;
  if v_source.id is null or v_target.id is null then
    raise exception 'Source or same-branch target officer is unavailable';
  end if;
  if not ('loan_officer'=any(regexp_split_to_array(lower(coalesce(v_source.role,'')),'\s*,\s*')))
     or 'admin'=any(regexp_split_to_array(lower(coalesce(v_source.role,'')),'\s*,\s*'))
     or 'branch_manager'=any(regexp_split_to_array(lower(coalesce(v_source.role,'')),'\s*,\s*')) then
    raise exception 'The source must be a loan officer';
  end if;
  if not ('loan_officer'=any(regexp_split_to_array(lower(coalesce(v_target.role,'')),'\s*,\s*')))
     or 'admin'=any(regexp_split_to_array(lower(coalesce(v_target.role,'')),'\s*,\s*'))
     or 'branch_manager'=any(regexp_split_to_array(lower(coalesce(v_target.role,'')),'\s*,\s*')) then
    raise exception 'The target must be a loan officer in the selected branch';
  end if;

  perform pg_advisory_xact_lock(hashtext('bripta_handover_'||p_from::text||'_'||p_branch::text));
  select count(*) into v_expected_clients from public.loan_clients
  where business_id='BIZ-B3F5E5D9' and branch_id=p_branch
    and loan_officer_id=p_from;
  if v_expected_clients=0 then
    raise exception 'No clients are assigned to the source officer in this branch';
  end if;
  if exists(
    select 1 from public.loans l join public.loan_clients c on c.id=l.client_id
    where c.business_id='BIZ-B3F5E5D9' and c.branch_id=p_branch
      and c.loan_officer_id=p_from and l.business_id=c.business_id
      and l.status='active' and (l.branch_id is distinct from p_branch
        or l.loan_officer_id is distinct from p_from)
  ) then
    raise exception 'A selected client has an active loan in another branch or assigned to another officer; review before transfer';
  end if;

  select coalesce(jsonb_object_agg(l.id,to_jsonb(l)-'loan_officer_id'-'updated_at'),'{}'::jsonb)
  into v_loans_before
  from public.loans l join public.loan_clients c on c.id=l.client_id
  where c.business_id='BIZ-B3F5E5D9' and c.branch_id=p_branch
    and c.loan_officer_id=p_from and l.business_id=c.business_id
    and l.branch_id=p_branch and l.status='active';

  with moved as (
    update public.loan_clients c
    set loan_officer_id=p_to,updated_at=now()
    where c.business_id='BIZ-B3F5E5D9' and c.branch_id=p_branch
      and c.loan_officer_id=p_from
    returning c.id
  ), moved_loans as (
    update public.loans l
    set loan_officer_id=p_to,updated_at=now()
    where l.business_id='BIZ-B3F5E5D9' and l.branch_id=p_branch
      and l.status='active' and l.loan_officer_id=p_from
      and l.client_id in(select id from moved)
    returning l.id
  ), moved_apps as (
    update public.loan_applications a
    set loan_officer_id=p_to,updated_at=now()
    where a.business_id='BIZ-B3F5E5D9' and a.branch_id=p_branch
      and a.status in('draft','submitted','under_review','approved')
      and (a.loan_officer_id=p_from or a.loan_officer_id is null)
      and a.client_id in(select id from moved)
    returning a.id
  )
  select (select count(*) from moved),
         (select count(*) from moved_loans),
         (select count(*) from moved_apps)
  into v_client_count,v_loan_count,v_application_count;
  if v_client_count<>v_expected_clients then
    raise exception 'The source portfolio changed during handover; all changes rolled back';
  end if;

  select coalesce(jsonb_object_agg(l.id,to_jsonb(l)-'loan_officer_id'-'updated_at'),'{}'::jsonb)
  into v_loans_after
  from public.loans l join public.loan_clients c on c.id=l.client_id
  where c.business_id='BIZ-B3F5E5D9' and c.branch_id=p_branch
    and c.loan_officer_id=p_to and l.business_id=c.business_id
    and l.branch_id=p_branch and l.status='active'
    and l.id::text in(select key from jsonb_each(v_loans_before));
  if v_loans_after is distinct from v_loans_before then
    raise exception 'A loan value changed during handover; all changes rolled back';
  end if;

  insert into public.bripta_domain_audit(
    business_id,branch_id,actor_staff_id,action,entity_type,new_value
  ) values (
    'BIZ-B3F5E5D9',p_branch,v_actor.id,'branch_portfolio_handover','loan_clients',
    jsonb_build_object('from',p_from,'to',p_to,'branch',p_branch,
      'clients',v_client_count,'active_loans',v_loan_count,
      'pending_applications',v_application_count,'financial_values_changed',false)
  );
  return jsonb_build_object('ok',true,'clients_moved',v_client_count,
    'active_loans_moved',v_loan_count,'pending_applications_moved',v_application_count);
end
$$;

revoke all on function public.bripta_handover_branch_portfolio(uuid,uuid,uuid)
  from public,anon;
grant execute on function public.bripta_handover_branch_portfolio(uuid,uuid,uuid)
  to authenticated;
