-- Bripta only: one pending Suspense register, visible from every branch.
-- Retains each receipt's branch attribution and existing write permissions.
-- Safe to rerun. Does not move, dismiss, match, or delete payments.
begin;

create or replace function public.bripta_suspense_page(
  p_source text,p_branch uuid default null,p_after uuid default null,p_limit integer default 500
) returns jsonb language plpgsql stable security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; roles text[]; result_rows jsonb;
  codes text[]; n integer:=least(greatest(coalesce(p_limit,500),1),500);
begin
  if auth.uid() is null then raise exception 'Sign in required' using errcode='42501'; end if;
  select s.* into actor from public.loan_staff s
  where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null
      and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
  order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if actor.id is null or not roles && array['admin','branch_manager','cashier','loan_officer'] then
    raise exception 'Suspense access is not permitted' using errcode='42501'; end if;
  -- p_branch remains in the signature for older callers, but Suspense is one
  -- company-wide register. Branch_id remains on each record for accounting
  -- and for the separate, branch-restricted write operations.
  select array_agg(code) into codes from (
    select 'BIZ-B3F5E5D9'::text code union
    select trim(s.mpesa_shortcode::text) from public.loan_settings s
    where s.business_id='BIZ-B3F5E5D9'
      and public.bripta_is_suspense_business(s.mpesa_shortcode::text)
  ) c;
  if p_source='queue' then
    select coalesce(jsonb_agg(to_jsonb(page) order by page.id),'[]') into result_rows from (
      select q.* from public.mpesa_callback_queue q
      where trim(q.business_short_code::text)=any(codes)
        and not coalesce(q.confirmed,false)
        and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
        and (p_after is null or q.id>p_after)
        and not exists(select 1 from public.loan_repayments r
          where r.business_id=actor.business_id
            and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id))
        and not exists(select 1 from public.unmatched_payments u
          where u.business_id=actor.business_id and u.mpesa_reference=q.trans_id
            and (coalesce(u.resolved,false)
              or coalesce((to_jsonb(u)->>'dismissed')::boolean,false)))
      order by q.id limit n
    ) page;
  elsif p_source='manual' then
    select coalesce(jsonb_agg(to_jsonb(page) order by page.id),'[]') into result_rows from (
      select u.* from public.unmatched_payments u
      where u.business_id=actor.business_id and not coalesce(u.resolved,false)
        and not coalesce((to_jsonb(u)->>'dismissed')::boolean,false)
        and (p_after is null or u.id>p_after)
        and not exists(select 1 from public.loan_repayments r
          where r.business_id=actor.business_id
            and (r.payment_reference=u.mpesa_reference or r.receipt_no=u.mpesa_reference))
        and not exists(select 1 from public.mpesa_callback_queue q
          where trim(q.business_short_code::text)=any(codes)
            and q.trans_id=u.mpesa_reference
            and (coalesce(q.confirmed,false)
              or coalesce((to_jsonb(q)->>'dismissed')::boolean,false)))
      order by u.id limit n
    ) page;
  else raise exception 'Unsupported Suspense source' using errcode='22023'; end if;
  return jsonb_build_object('rows',result_rows,'next_after',
    case when jsonb_array_length(result_rows)=n then result_rows->(n-1)->>'id' else null end);
end $$;

revoke all on function public.bripta_suspense_page(text,uuid,uuid,integer) from public,anon;
grant execute on function public.bripta_suspense_page(text,uuid,uuid,integer) to authenticated;
notify pgrst,'reload schema';
commit;

select to_regprocedure('public.bripta_suspense_page(text,uuid,uuid,integer)') is not null
  as shared_suspense_reader_installed;
