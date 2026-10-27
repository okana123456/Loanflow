-- Bripta business only. Route suspense by a unique registered PHONE account.
-- Pending records with ambiguous or unknown accounts remain in their current
-- branch for central review. No repayment, balance, amount or SMS is changed.
-- Safe to rerun in the Bripta Supabase SQL Editor.
begin;
set local lock_timeout='10s';

do $$ begin
  if to_regprocedure('public.bripta_is_suspense_business(text)') is null then
    raise exception 'Install the existing Bripta suspense reader first; no change made';
  end if;
  if not exists(select 1 from public.bripta_branches
    where business_id='BIZ-B3F5E5D9' and is_head_office and lower(trim(name))='migori')
    or not exists(select 1 from public.bripta_branches
    where business_id='BIZ-B3F5E5D9' and lower(trim(name)) like 'mbita%') then
    raise exception 'Expected Bripta Migori and Mbita branches were not found; no change made';
  end if;
end $$;

create or replace function public.bripta_route_pending_suspense_branch()
returns trigger language plpgsql security definer set search_path=public as $$
declare v_account text; v_branch uuid; v_matches integer;
begin
  if tg_table_name='mpesa_callback_queue' then
    if coalesce(new.confirmed,false) or coalesce((to_jsonb(new)->>'dismissed')::boolean,false)
      or not public.bripta_is_suspense_business(new.business_short_code::text) then return new; end if;
    v_account:=regexp_replace(coalesce(new.bill_ref_number,''),'\D','','g');
  else
    if new.business_id<>'BIZ-B3F5E5D9' or coalesce(new.resolved,false)
      or coalesce((to_jsonb(new)->>'dismissed')::boolean,false) then return new; end if;
    v_account:=regexp_replace(coalesce(new.account_number,''),'\D','','g');
  end if;
  if length(v_account) not between 9 and 12 then return new; end if;
  select count(*),min(c.branch_id::text)::uuid into v_matches,v_branch
  from public.loan_clients c join public.bripta_branches b on b.id=c.branch_id
  where c.business_id='BIZ-B3F5E5D9' and b.business_id=c.business_id and b.is_active
    and length(regexp_replace(coalesce(c.phone,''),'\D','','g')) between 9 and 12
    and right(regexp_replace(c.phone,'\D','','g'),9)=right(v_account,9);
  if v_matches=1 then new.branch_id:=v_branch; end if;
  return new;
end $$;

drop trigger if exists zzzz_bripta_route_pending_suspense_branch on public.mpesa_callback_queue;
create trigger zzzz_bripta_route_pending_suspense_branch
before insert or update of business_short_code,branch_id,bill_ref_number
on public.mpesa_callback_queue for each row
execute function public.bripta_route_pending_suspense_branch();
drop trigger if exists zzzz_bripta_route_pending_suspense_branch on public.unmatched_payments;
create trigger zzzz_bripta_route_pending_suspense_branch
before insert or update of business_id,branch_id,account_number
on public.unmatched_payments for each row
execute function public.bripta_route_pending_suspense_branch();

-- Historical pending callbacks have no posted repayment. Correct only rows
-- whose Paybill account identifies exactly one Bripta client.
with matched as (
  select q.id,min(c.branch_id::text)::uuid as branch_id
  from public.mpesa_callback_queue q
  join public.loan_clients c on c.business_id='BIZ-B3F5E5D9'
    and length(regexp_replace(coalesce(c.phone,''),'\D','','g')) between 9 and 12
    and right(regexp_replace(c.phone,'\D','','g'),9)=
      right(regexp_replace(coalesce(q.bill_ref_number,''),'\D','','g'),9)
  join public.bripta_branches b on b.id=c.branch_id and b.business_id=c.business_id and b.is_active
  where public.bripta_is_suspense_business(q.business_short_code::text)
    and length(regexp_replace(coalesce(q.bill_ref_number,''),'\D','','g')) between 9 and 12
    and not coalesce(q.confirmed,false)
    and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
    and not exists(select 1 from public.loan_repayments r
      where r.business_id='BIZ-B3F5E5D9'
        and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id))
  group by q.id having count(*)=1
)
update public.mpesa_callback_queue q set branch_id=m.branch_id
from matched m where q.id=m.id and q.branch_id is distinct from m.branch_id;

-- A manual suspense receipt can already have paired journal entries. Move
-- their branch attribution with the receipt while preserving each amount.
with matched as (
  select u.id,min(c.branch_id::text)::uuid as branch_id
  from public.unmatched_payments u
  join public.loan_clients c on c.business_id=u.business_id
    and length(regexp_replace(coalesce(c.phone,''),'\D','','g')) between 9 and 12
    and right(regexp_replace(c.phone,'\D','','g'),9)=
      right(regexp_replace(coalesce(u.account_number,''),'\D','','g'),9)
  join public.bripta_branches b on b.id=c.branch_id and b.business_id=c.business_id and b.is_active
  where u.business_id='BIZ-B3F5E5D9' and not coalesce(u.resolved,false)
    and not coalesce((to_jsonb(u)->>'dismissed')::boolean,false)
    and length(regexp_replace(coalesce(u.account_number,''),'\D','','g')) between 9 and 12
    and not exists(select 1 from public.loan_repayments r
      where r.business_id=u.business_id
        and (r.payment_reference=u.mpesa_reference or r.receipt_no=u.mpesa_reference))
  group by u.id having count(*)=1
), moved as (
  update public.unmatched_payments u set branch_id=m.branch_id
  from matched m where u.id=m.id and u.branch_id is distinct from m.branch_id
  returning u.id,u.branch_id
)
update public.bripta_accounting_entries e set branch_id=m.branch_id
from moved m where e.business_id='BIZ-B3F5E5D9'
  and e.source_table='unmatched_payments' and e.source_id=m.id::text
  and e.branch_id is distinct from m.branch_id;

notify pgrst,'reload schema';
commit;

-- Read-only result: compare pending suspense by actual branch.
select b.name as branch, 'callback_queue' as source,count(q.id) as pending_count,
  coalesce(sum(q.trans_amount),0) as amount
from public.bripta_branches b left join public.mpesa_callback_queue q
  on q.branch_id=b.id and public.bripta_is_suspense_business(q.business_short_code::text)
  and not coalesce(q.confirmed,false)
  and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
where b.business_id='BIZ-B3F5E5D9' group by b.id,b.name
union all
select b.name,'manual_suspense',count(u.id),coalesce(sum(u.amount),0)
from public.bripta_branches b left join public.unmatched_payments u
  on u.branch_id=b.id and u.business_id=b.business_id and not coalesce(u.resolved,false)
  and not coalesce((to_jsonb(u)->>'dismissed')::boolean,false)
where b.business_id='BIZ-B3F5E5D9' group by b.id,b.name
order by branch,source;
