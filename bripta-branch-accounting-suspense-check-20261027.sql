-- Bripta only. Read-only check after the routing SQL and website update.
with portfolio as (
  select b.name as branch,'current_loan_officers'::text as check_name,
    count(s.id)::numeric as result,
    coalesce(string_agg(s.name,', ' order by s.name),'-') as details
  from public.bripta_branches b left join public.loan_staff s
    on s.business_id=b.business_id and s.branch_id=b.id and coalesce(s.is_active,true)
    and 'loan_officer'=any(regexp_split_to_array(lower(coalesce(s.role,'')),'\s*,\s*'))
    and not ('admin'=any(regexp_split_to_array(lower(coalesce(s.role,'')),'\s*,\s*')))
    and not ('branch_manager'=any(regexp_split_to_array(lower(coalesce(s.role,'')),'\s*,\s*')))
  where b.business_id='BIZ-B3F5E5D9' group by b.id,b.name
), queue as (
  select b.name as branch,'pending_callback_suspense'::text as check_name,
    count(q.id)::numeric as result,
    'KES '||coalesce(sum(q.trans_amount),0)::text as details
  from public.bripta_branches b left join public.mpesa_callback_queue q
    on q.branch_id=b.id and public.bripta_is_suspense_business(q.business_short_code::text)
    and not coalesce(q.confirmed,false)
    and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
    and not exists(select 1 from public.loan_repayments r
      where r.business_id=b.business_id
        and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id))
  where b.business_id='BIZ-B3F5E5D9' group by b.id,b.name
), manual as (
  select b.name as branch,'pending_manual_suspense'::text as check_name,
    count(u.id)::numeric as result,
    'KES '||coalesce(sum(u.amount),0)::text as details
  from public.bripta_branches b left join public.unmatched_payments u
    on u.branch_id=b.id and u.business_id=b.business_id and not coalesce(u.resolved,false)
    and not coalesce((to_jsonb(u)->>'dismissed')::boolean,false)
    and not exists(select 1 from public.loan_repayments r
      where r.business_id=b.business_id
        and (r.payment_reference=u.mpesa_reference or r.receipt_no=u.mpesa_reference))
  where b.business_id='BIZ-B3F5E5D9' group by b.id,b.name
)
select * from portfolio union all select * from queue union all select * from manual
order by branch,check_name;
