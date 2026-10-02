-- BRIPTA ONLY. Makes pending M-Pesa callbacks visible under the existing
-- Head Office (now named Migori). No repayment, SMS, balance or amount changes.
-- Safe to rerun in the Bripta Supabase SQL Editor.
begin;
set local lock_timeout='10s';

do $$ begin
  if not exists (
    select 1 from public.bripta_branches
    where id='00000000-0000-4000-8000-000000000001'::uuid
      and business_id='BIZ-B3F5E5D9' and is_head_office
  ) then raise exception 'Bripta Head Office branch is missing; no changes made'; end if;
end $$;

-- A separate callback-only trigger avoids altering the shared branch trigger
-- used by loans, repayments, other businesses and historic records.
create or replace function public.bripta_assign_callback_suspense_branch()
returns trigger language plpgsql security definer set search_path=public as $$
declare v_head uuid := '00000000-0000-4000-8000-000000000001'::uuid;
begin
  if new.business_short_code::text='BIZ-B3F5E5D9'
     and (new.branch_id is null or not exists (
       select 1 from public.bripta_branches b
       where b.id=new.branch_id and b.business_id='BIZ-B3F5E5D9'
     )) then new.branch_id:=v_head; end if;
  return new;
end $$;

drop trigger if exists bripta_callback_suspense_branch_trg on public.mpesa_callback_queue;
create trigger bripta_callback_suspense_branch_trg
before insert or update of business_short_code,branch_id
on public.mpesa_callback_queue for each row
execute function public.bripta_assign_callback_suspense_branch();

-- Only pending Bripta callbacks with no valid Bripta branch need repair.
-- In particular, leave existing valid branch assignments and all confirmed
-- repayments alone. Existing timestamps, references and amounts stay intact.
update public.mpesa_callback_queue q
set branch_id='00000000-0000-4000-8000-000000000001'::uuid
where q.business_short_code::text='BIZ-B3F5E5D9'
  and not coalesce(q.confirmed,false)
  and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
  and (q.branch_id is null or not exists (
    select 1 from public.bripta_branches b
    where b.id=q.branch_id and b.business_id='BIZ-B3F5E5D9'
  ));

commit;

-- Check both branch visibility and that the four recently reported payments
-- remain pending. Other old pending callbacks may also be present.
select q.trans_id as transaction_code,q.trans_amount as amount,
  q.branch_id,b.name as branch_name,q.confirmed,q.repayment_id
from public.mpesa_callback_queue q
left join public.bripta_branches b on b.id=q.branch_id
where q.business_short_code::text='BIZ-B3F5E5D9'
  and not coalesce(q.confirmed,false)
  and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
order by q.created_at desc;
