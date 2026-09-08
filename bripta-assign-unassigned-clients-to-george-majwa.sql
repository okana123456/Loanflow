-- Bripta: reliable portfolio transfers and one-time assignment of genuinely
-- unassigned clients to George Majwa.
-- Existing portfolios, historical loans, balances and repayments are preserved.

begin;

alter table public.loan_clients
  add column if not exists loan_officer_id uuid;

create index if not exists loan_clients_business_officer_idx
  on public.loan_clients (business_id, loan_officer_id);

create or replace function public.bripta_assign_client_portfolio(
  p_client_id uuid,
  p_officer_id uuid,
  p_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_client public.loan_clients%rowtype;
  v_officer public.loan_staff%rowtype;
  v_actor public.loan_staff%rowtype;
  v_loans_updated integer := 0;
  v_apps_updated integer := 0;
  v_notes text;
begin
  select * into v_client
  from public.loan_clients where id = p_client_id for update;
  if v_client.id is null then raise exception 'Client was not found'; end if;

  select * into v_officer
  from public.loan_staff where id = p_officer_id;
  if v_officer.id is null or coalesce(v_officer.is_active, true) = false then
    raise exception 'The selected officer is not active';
  end if;
  if v_officer.business_id is distinct from v_client.business_id then
    raise exception 'Client and officer belong to different businesses';
  end if;

  if auth.uid() is not null and auth.role() <> 'service_role' then
    select * into v_actor
    from public.loan_staff
    where auth_user_id = auth.uid()
      and business_id = v_client.business_id
      and coalesce(is_active, true) = true
    limit 1;
    if v_actor.id is null or not (
      coalesce(v_actor.role, '') ilike '%admin%'
      or coalesce(v_actor.role, '') ilike '%branch_manager%'
    ) then
      raise exception 'Admin or manager access is required';
    end if;
  end if;

  v_notes := trim(regexp_replace(
    coalesce(v_client.notes, ''), '\s*\[OFFICER:[^]]+\]', '', 'gi'
  ));
  v_notes := trim(v_notes || ' [OFFICER:' || p_officer_id::text || ']'
    || ' [TRANSFER ' || current_date::text || ': assigned to '
    || coalesce(v_officer.name, p_officer_id::text)
    || case when nullif(trim(coalesce(p_reason, '')), '') is not null
      then '. Reason: ' || trim(p_reason) else '' end || ']');

  update public.loan_clients
  set loan_officer_id = p_officer_id, notes = v_notes
  where id = p_client_id;

  update public.loans
  set loan_officer_id = p_officer_id
  where client_id = p_client_id
    and business_id = v_client.business_id
    and status = 'active';
  get diagnostics v_loans_updated = row_count;

  update public.loan_applications
  set loan_officer_id = p_officer_id
  where client_id = p_client_id
    and business_id = v_client.business_id
    and coalesce(status, '') not in ('disbursed', 'rejected', 'cancelled');
  get diagnostics v_apps_updated = row_count;

  return jsonb_build_object(
    'ok', true,
    'client_id', p_client_id,
    'officer_id', p_officer_id,
    'officer_name', v_officer.name,
    'active_loans_updated', v_loans_updated,
    'open_applications_updated', v_apps_updated
  );
end;
$$;

revoke all on function public.bripta_assign_client_portfolio(uuid, uuid, text)
  from public, anon;
grant execute on function public.bripta_assign_client_portfolio(uuid, uuid, text)
  to authenticated, service_role;

-- Backfill the explicit client portfolio owner from existing assignment data.
-- This makes registration-fee attribution work even before a client has a loan.
with assignment_candidates as (
  select c.id as client_id, c.business_id, l.loan_officer_id as officer_id,
         1 as priority, coalesce(l.disbursement_date::text, l.created_at::text, '') as assignment_date
  from public.loan_clients c
  join public.loans l on l.client_id = c.id and l.business_id = c.business_id
  where l.status = 'active' and l.loan_officer_id is not null
  union all
  select c.id, c.business_id, l.loan_officer_id, 2,
         coalesce(l.disbursement_date::text, l.created_at::text, '')
  from public.loan_clients c
  join public.loans l on l.client_id = c.id and l.business_id = c.business_id
  where l.loan_officer_id is not null
  union all
  select c.id, c.business_id,
         (regexp_match(c.notes, '\[OFFICER:([0-9a-fA-F-]{36})\]'))[1]::uuid,
         3, ''
  from public.loan_clients c
  where coalesce(c.notes, '') ~ '\[OFFICER:[0-9a-fA-F-]{36}\]'
  union all
  select c.id, c.business_id, a.loan_officer_id, 4,
         coalesce(a.created_at::text, '')
  from public.loan_clients c
  join public.loan_applications a
    on a.client_id = c.id and a.business_id = c.business_id
  where a.loan_officer_id is not null
), valid_candidates as (
  select candidate.*,
         row_number() over (
           partition by candidate.client_id
           order by candidate.priority, candidate.assignment_date desc
         ) as choice
  from assignment_candidates candidate
  join public.loan_staff staff
    on staff.id = candidate.officer_id
   and staff.business_id = candidate.business_id
   and coalesce(staff.is_active, true) = true
)
update public.loan_clients c
set loan_officer_id = source.officer_id
from valid_candidates source
where c.id = source.client_id
  and c.loan_officer_id is null
  and source.choice = 1;

do $$
declare
  v_george_id uuid;
  v_george_business text;
  v_george_count integer;
  v_client record;
begin
  select count(*) into v_george_count
  from public.loan_staff
  where name ilike '%George%'
    and name ilike '%Majwa%'
    and coalesce(is_active, true) = true;

  if v_george_count = 0 then
    raise exception 'Active staff account for George Majwa was not found';
  elsif v_george_count > 1 then
    raise exception 'More than one active George Majwa account was found; no clients were changed';
  end if;

  select id, business_id into v_george_id, v_george_business
  from public.loan_staff
  where name ilike '%George%'
    and name ilike '%Majwa%'
    and coalesce(is_active, true) = true
  limit 1;

  for v_client in
    select c.id
    from public.loan_clients c
    where c.business_id = v_george_business
      and c.loan_officer_id is null
      and coalesce(c.notes, '') !~* '\[OFFICER:[^]]+\]'
      and not exists (
        select 1 from public.loans l
        where l.client_id = c.id
          and l.business_id = c.business_id
          and l.loan_officer_id is not null
      )
      and not exists (
        select 1 from public.loan_applications a
        where a.client_id = c.id
          and a.business_id = c.business_id
          and a.loan_officer_id is not null
      )
  loop
    perform public.bripta_assign_client_portfolio(
      v_client.id, v_george_id,
      'Assigned from the unassigned-client portfolio'
    );
  end loop;
end;
$$;

commit;

select
  1 as section_order,
  'George Majwa portfolio assignment' as section,
  jsonb_build_object(
    'result', 'Bripta unassigned-client transfer is ready',
    'george_officer_id', s.id,
    'george_name', s.name,
    'clients_now_assigned', (
      select count(*) from public.loan_clients c
      where c.business_id = s.business_id and c.loan_officer_id = s.id
    ),
    'active_loans_now_assigned', (
      select count(*) from public.loans l
      where l.business_id = s.business_id
        and l.loan_officer_id = s.id and l.status = 'active'
    ),
    'genuinely_unassigned_clients_remaining', (
      select count(*) from public.loan_clients c
      where c.business_id = s.business_id
        and c.loan_officer_id is null
        and coalesce(c.notes, '') !~* '\[OFFICER:[^]]+\]'
        and not exists (
          select 1 from public.loans l
          where l.client_id = c.id and l.business_id = c.business_id
            and l.loan_officer_id is not null
        )
        and not exists (
          select 1 from public.loan_applications a
          where a.client_id = c.id and a.business_id = c.business_id
            and a.loan_officer_id is not null
        )
    ),
    'loan_balances_changed', false,
    'repayments_changed', false
  ) as result
from public.loan_staff s
where s.name ilike '%George%'
  and s.name ilike '%Majwa%'
  and coalesce(s.is_active, true) = true;
