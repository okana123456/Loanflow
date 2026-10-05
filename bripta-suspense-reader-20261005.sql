-- Bripta only. Restore pending callback visibility and branch metadata.
-- No repayments are created; amounts, balances, SMS and accounting are untouched.
begin;

create or replace function public.bripta_is_suspense_business(p_code text)
returns boolean language sql stable security definer set search_path=pg_catalog as $$
  select trim(p_code)='BIZ-B3F5E5D9' or (
    nullif(trim(p_code),'') is not null
    and exists(select 1 from public.loan_settings s where s.business_id='BIZ-B3F5E5D9'
      and trim(s.mpesa_shortcode::text)=trim(p_code))
    and not exists(select 1 from public.loan_settings s where s.business_id is distinct from 'BIZ-B3F5E5D9'
      and trim(s.mpesa_shortcode::text)=trim(p_code))
  )
$$;
revoke all on function public.bripta_is_suspense_business(text) from public,anon;
grant execute on function public.bripta_is_suspense_business(text) to authenticated,service_role;

create or replace function public.bripta_suspense_branch_guard()
returns trigger language plpgsql security definer set search_path=pg_catalog as $$
declare own_business boolean; head uuid;
begin
  if tg_table_name='mpesa_callback_queue' then
    own_business:=public.bripta_is_suspense_business(new.business_short_code::text);
  else own_business:=new.business_id='BIZ-B3F5E5D9'; end if;
  if not coalesce(own_business,false) then return new; end if;
  if not exists(select 1 from public.bripta_branches b where b.id=new.branch_id and b.business_id='BIZ-B3F5E5D9') then
    select b.id into head from public.bripta_branches b where b.business_id='BIZ-B3F5E5D9' and b.is_head_office order by b.id limit 1;
    if head is null then raise exception 'Bripta Head Office branch is missing'; end if;
    new.branch_id:=head;
  end if;
  return new;
end $$;
drop trigger if exists zz_bripta_suspense_branch_guard on public.mpesa_callback_queue;
create trigger zz_bripta_suspense_branch_guard before insert or update of branch_id,business_short_code
on public.mpesa_callback_queue for each row execute function public.bripta_suspense_branch_guard();
drop trigger if exists zz_bripta_suspense_branch_guard on public.unmatched_payments;
create trigger zz_bripta_suspense_branch_guard before insert or update of branch_id,business_id
on public.unmatched_payments for each row execute function public.bripta_suspense_branch_guard();

-- Fire the branch guard only on pending Bripta rows whose branch is missing/wrong.
update public.mpesa_callback_queue q set branch_id=q.branch_id
where public.bripta_is_suspense_business(q.business_short_code::text)
  and not coalesce(q.confirmed,false) and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
  and not exists(select 1 from public.bripta_branches b where b.id=q.branch_id and b.business_id='BIZ-B3F5E5D9');
update public.unmatched_payments u set branch_id=u.branch_id
where u.business_id='BIZ-B3F5E5D9' and not coalesce(u.resolved,false)
  and not coalesce((to_jsonb(u)->>'dismissed')::boolean,false)
  and not exists(select 1 from public.bripta_branches b where b.id=u.branch_id and b.business_id='BIZ-B3F5E5D9');

create or replace function public.bripta_suspense_page(
  p_source text,p_branch uuid default null,p_after uuid default null,p_limit integer default 500
) returns jsonb language plpgsql stable security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; roles text[]; branch uuid; result_rows jsonb;
  codes text[]; n integer:=least(greatest(coalesce(p_limit,500),1),500);
begin
  if auth.uid() is null then raise exception 'Sign in required' using errcode='42501'; end if;
  select s.* into actor from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if actor.id is null or not roles && array['admin','branch_manager','cashier','loan_officer'] then
    raise exception 'Suspense access is not permitted' using errcode='42501'; end if;
  if 'admin'=any(roles) then branch:=p_branch;
  else
    if actor.branch_id is null or (p_branch is not null and p_branch<>actor.branch_id) then
      raise exception 'Branch access is not permitted' using errcode='42501'; end if;
    branch:=actor.branch_id;
  end if;
  if branch is not null and not exists(select 1 from public.bripta_branches b where b.id=branch and b.business_id=actor.business_id) then
    raise exception 'Invalid Bripta branch' using errcode='42501'; end if;
  select array_agg(code) into codes from (
    select 'BIZ-B3F5E5D9'::text code union
    select trim(s.mpesa_shortcode::text) from public.loan_settings s
      where s.business_id='BIZ-B3F5E5D9' and public.bripta_is_suspense_business(s.mpesa_shortcode::text)
  ) c;
  if p_source='queue' then
    select coalesce(jsonb_agg(to_jsonb(page) order by page.id),'[]') into result_rows from (
      select q.* from public.mpesa_callback_queue q
      where trim(q.business_short_code::text)=any(codes)
        and not coalesce(q.confirmed,false) and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
        and (branch is null or q.branch_id=branch) and (p_after is null or q.id>p_after)
        and not exists(select 1 from public.loan_repayments r where r.business_id=actor.business_id
          and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id))
        and not exists(select 1 from public.unmatched_payments u where u.business_id=actor.business_id
          and u.mpesa_reference=q.trans_id and (coalesce(u.resolved,false) or coalesce((to_jsonb(u)->>'dismissed')::boolean,false)))
      order by q.id limit n
    ) page;
  elsif p_source='manual' then
    select coalesce(jsonb_agg(to_jsonb(page) order by page.id),'[]') into result_rows from (
      select u.* from public.unmatched_payments u
      where u.business_id=actor.business_id and not coalesce(u.resolved,false)
        and not coalesce((to_jsonb(u)->>'dismissed')::boolean,false)
        and (branch is null or u.branch_id=branch) and (p_after is null or u.id>p_after)
        and not exists(select 1 from public.loan_repayments r where r.business_id=actor.business_id
          and (r.payment_reference=u.mpesa_reference or r.receipt_no=u.mpesa_reference))
        and not exists(select 1 from public.mpesa_callback_queue q where trim(q.business_short_code::text)=any(codes)
          and q.trans_id=u.mpesa_reference and (coalesce(q.confirmed,false) or coalesce((to_jsonb(q)->>'dismissed')::boolean,false)))
      order by u.id limit n
    ) page;
  else raise exception 'Unsupported Suspense source' using errcode='22023'; end if;
  return jsonb_build_object('rows',result_rows,'next_after',case when jsonb_array_length(result_rows)=n then result_rows->(n-1)->>'id' else null end);
end $$;
revoke all on function public.bripta_suspense_page(text,uuid,uuid,integer) from public,anon;
grant execute on function public.bripta_suspense_page(text,uuid,uuid,integer) to authenticated;
notify pgrst,'reload schema';
commit;
select to_regprocedure('public.bripta_suspense_page(text,uuid,uuid,integer)') is not null as suspense_reader_installed,
  (select count(*) from public.mpesa_callback_queue q where public.bripta_is_suspense_business(q.business_short_code::text)
    and not coalesce(q.confirmed,false) and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)) as pending_callback_rows;
