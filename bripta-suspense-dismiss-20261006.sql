-- Bripta only: checked, atomic single/bulk dismissal. Keeps original receipts.
-- Does not delete records, create repayments/SMS or change money amounts.
begin;
alter table public.mpesa_callback_queue add column if not exists dismissed boolean default false,
  add column if not exists dismissed_at timestamptz,add column if not exists dismissed_by text;
alter table public.unmatched_payments add column if not exists dismissed boolean default false,
  add column if not exists dismissed_at timestamptz,add column if not exists dismissed_by text;

create or replace function public.bripta_dismiss_suspense(
  p_queue_ids uuid[] default '{}',p_manual_ids uuid[] default '{}',p_reason text default 'Dismissed by authorized staff'
) returns jsonb language plpgsql security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; roles text[]; qids uuid[]; mids uuid[]; refs text[];
  row_data record; previous jsonb; changed jsonb; requested integer; found integer; affected integer:=0;
  already integer:=0; dismissal_time timestamptz:=now();
begin
  if auth.uid() is null then raise exception 'Sign in required' using errcode='42501'; end if;
  select s.* into actor from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if actor.id is null or not (roles && array['admin','branch_manager','cashier']) then
    raise exception 'Your Suspense access is view only' using errcode='42501'; end if;
  if not ('admin'=any(roles)) and actor.branch_id is null then raise exception 'Staff branch is missing' using errcode='42501'; end if;
  select coalesce(array_agg(distinct x),'{}') into qids from unnest(coalesce(p_queue_ids,'{}')) x where x is not null;
  select coalesce(array_agg(distinct x),'{}') into mids from unnest(coalesce(p_manual_ids,'{}')) x where x is not null;
  requested:=cardinality(qids)+cardinality(mids);
  if requested<1 or requested>500 then raise exception 'Select between 1 and 500 Suspense records' using errcode='22023'; end if;
  select count(*) into found from public.mpesa_callback_queue q where q.id=any(qids)
    and public.bripta_is_suspense_business(q.business_short_code::text)
    and ('admin'=any(roles) or q.branch_id=actor.branch_id);
  if found<>cardinality(qids) then raise exception 'Payment missing or outside your business/branch' using errcode='42501'; end if;
  select count(*) into found from public.unmatched_payments u where u.id=any(mids)
    and u.business_id=actor.business_id and ('admin'=any(roles) or u.branch_id=actor.branch_id);
  if found<>cardinality(mids) then raise exception 'Payment missing or outside your business/branch' using errcode='42501'; end if;

  -- A receipt sometimes has an old fallback copy in the other register. Mark
  -- those copies together so they cannot reappear or be replayed as payments.
  select array_agg(distinct reference) into refs from (
    select nullif(q.trans_id,'') reference from public.mpesa_callback_queue q where q.id=any(qids)
    union select nullif(u.mpesa_reference,'') from public.unmatched_payments u where u.id=any(mids)
  ) x where reference is not null;
  select coalesce(array_agg(q.id),'{}') into qids from public.mpesa_callback_queue q
    where (q.id=any(qids) or q.trans_id=any(refs)) and public.bripta_is_suspense_business(q.business_short_code::text)
      and ('admin'=any(roles) or q.branch_id=actor.branch_id);
  select coalesce(array_agg(u.id),'{}') into mids from public.unmatched_payments u
    where (u.id=any(mids) or u.mpesa_reference=any(refs)) and u.business_id=actor.business_id
      and ('admin'=any(roles) or u.branch_id=actor.branch_id);

  for row_data in select q.* from public.mpesa_callback_queue q where q.id=any(qids) order by q.id for update loop
    previous:=to_jsonb(row_data);
    if not public.bripta_is_suspense_business(row_data.business_short_code::text)
      or (not ('admin'=any(roles)) and row_data.branch_id is distinct from actor.branch_id) then
      raise exception 'Payment branch changed; reload Suspense' using errcode='42501'; end if;
    if coalesce(row_data.dismissed,false) then already:=already+1;continue; end if;
    if coalesce(row_data.confirmed,false) or nullif(previous->>'repayment_id','') is not null or exists(
      select 1 from public.loan_repayments r where r.business_id=actor.business_id
        and (r.payment_reference=row_data.trans_id or r.receipt_no=row_data.trans_id)
    ) then raise exception 'Payment % is already confirmed or recorded; it cannot be dismissed',row_data.trans_id; end if;
    update public.mpesa_callback_queue set confirmed=true,dismissed=true,dismissed_at=dismissal_time,dismissed_by=actor.id::text
      where id=row_data.id returning to_jsonb(mpesa_callback_queue.*) into changed;
    insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
      values(actor.business_id,row_data.branch_id,actor.id,'suspense_dismissed','mpesa_callback_queue',row_data.id::text,previous,
        changed||jsonb_build_object('dismissal_reason',left(coalesce(nullif(trim(p_reason),''),'Dismissed by authorized staff'),1000)));
    affected:=affected+1;
  end loop;
  for row_data in select u.* from public.unmatched_payments u where u.id=any(mids) order by u.id for update loop
    previous:=to_jsonb(row_data);
    if row_data.business_id is distinct from actor.business_id
      or (not ('admin'=any(roles)) and row_data.branch_id is distinct from actor.branch_id) then
      raise exception 'Payment branch changed; reload Suspense' using errcode='42501'; end if;
    if coalesce(row_data.dismissed,false) then already:=already+1;continue; end if;
    if coalesce(row_data.resolved,false) or nullif(previous->>'repayment_id','') is not null or exists(
      select 1 from public.loan_repayments r where r.business_id=actor.business_id
        and (r.payment_reference=row_data.mpesa_reference or r.receipt_no=row_data.mpesa_reference)
    ) then raise exception 'Payment % is already matched or recorded; it cannot be dismissed',row_data.mpesa_reference; end if;
    update public.unmatched_payments set dismissed=true,dismissed_at=dismissal_time,dismissed_by=actor.id::text
      where id=row_data.id returning to_jsonb(unmatched_payments.*) into changed;
    insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
      values(actor.business_id,row_data.branch_id,actor.id,'suspense_dismissed','unmatched_payments',row_data.id::text,previous,
        changed||jsonb_build_object('dismissal_reason',left(coalesce(nullif(trim(p_reason),''),'Dismissed by authorized staff'),1000)));
    affected:=affected+1;
  end loop;
  return jsonb_build_object('ok',true,'requested',requested,'dismissed_rows',affected,'already_dismissed_rows',already);
end $$;
revoke all on function public.bripta_dismiss_suspense(uuid[],uuid[],text) from public,anon;
grant execute on function public.bripta_dismiss_suspense(uuid[],uuid[],text) to authenticated;
notify pgrst,'reload schema';
commit;
select to_regprocedure('public.bripta_dismiss_suspense(uuid[],uuid[],text)') is not null as suspense_dismiss_installed;
