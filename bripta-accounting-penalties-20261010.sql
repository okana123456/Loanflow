-- Read-only charged penalties in Bripta Accounting; no income postings.
begin;
create or replace function public.bripta_accounting_penalties(p_start date,p_end date,p_branch uuid default null,p_after uuid default null)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; roles text[]; branch uuid; records jsonb;
begin
  if auth.uid() is null then raise exception 'Sign in required' using errcode='42501'; end if;
  if p_start is null or p_end is null or p_start>p_end then raise exception 'Choose a valid date range' using errcode='22023'; end if;
  select s.* into actor from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if actor.id is null or not ('admin'=any(roles) or 'branch_manager'=any(roles) or exists(
    select 1 from public.bripta_staff_permissions p where p.staff_id=actor.id and p.business_id=actor.business_id and p.view_accounting
  )) then raise exception 'Accounting permission required' using errcode='42501'; end if;
  if 'admin'=any(roles) then branch:=p_branch;
  else
    if actor.branch_id is null or (p_branch is not null and p_branch<>actor.branch_id) then
      raise exception 'Branch access is not permitted' using errcode='42501'; end if;
    branch:=actor.branch_id;
  end if;
  if branch is not null and not exists(select 1 from public.bripta_branches b where b.id=branch and b.business_id=actor.business_id) then
    raise exception 'Invalid Bripta branch' using errcode='42501'; end if;


  select coalesce(jsonb_agg(to_jsonb(page) order by page.id),'[]') into records from (
    select p.*,l.loan_no,c.full_name as client_name,l.branch_id as loan_branch_id,
      coalesce(p.date_charged,((to_jsonb(p)->>'created_at')::timestamptz at time zone 'Africa/Nairobi')::date) as charged_on
    from public.loan_penalties p join public.loans l on l.id=p.loan_id
      left join public.loan_clients c on c.id=l.client_id and c.business_id=l.business_id
    where l.business_id=actor.business_id and (branch is null or l.branch_id=branch)
      and coalesce(p.date_charged,((to_jsonb(p)->>'created_at')::timestamptz at time zone 'Africa/Nairobi')::date) between p_start and p_end
      and (p_after is null or p.id>p_after)
    order by p.id limit 500
  ) page;
  return jsonb_build_object('rows',records,'next_after',case when jsonb_array_length(records)=500 then records->499->>'id' else null end);
end $$;
revoke all on function public.bripta_accounting_penalties(date,date,uuid,uuid) from public,anon;
grant execute on function public.bripta_accounting_penalties(date,date,uuid,uuid) to authenticated;
notify pgrst,'reload schema';
commit;
select to_regprocedure('public.bripta_accounting_penalties(date,date,uuid,uuid)') is not null as accounting_penalties_installed;
