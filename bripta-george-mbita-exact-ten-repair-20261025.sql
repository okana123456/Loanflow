-- SUPERSEDED for future use. On 26 October, the owner clarified that only
-- Tabitha, Judith and Rose Akeyo of these ten belong to Mbita. The other
-- seven are corrected by bripta-return-seven-george-loans-to-migori-20261026.sql.
-- Never rerun the earlier all-ten move after that correction.
-- Run ONLY in the Bripta Supabase project, after
-- bripta-branch-origin-guard-20261025.sql.
-- This moves branch attribution for the ten loan IDs confirmed as Mbita by
-- the business owner. It never changes an amount, balance, payment, or status.
-- All other loans, including George's older Migori loans, keep their branch.
begin;
do $$ begin
  if exists(select 1 from public.bripta_domain_audit
    where business_id='BIZ-B3F5E5D9'
      and action='george_seven_migori_correction_20261026') then
    raise exception 'The all-ten Mbita repair is superseded; use the seven-loan Migori correction instead';
  end if;
end $$;
drop table if exists pg_temp.bripta_moved_loans;
drop table if exists pg_temp.bripta_financial_before;

create temporary table bripta_moved_loans on commit preserve rows as
select l.id,l.client_id,l.application_id,l.loan_no
from public.loans l
where l.id=any(array[
  '5f44fc32-41f7-48c3-8eec-b71c6584c294', -- 900041 Tabitha
  'f8106a47-bba1-4585-8dff-5f7bd7fff99d', -- 018991 Judith
  '33e3a535-7922-4624-82bf-7364642ed71b', -- 397558 Rose Akeyo
  '0e9de4dc-fc33-41ea-bde6-99dfafb2a31b', -- 403414 Rose Awino
  '9bcf0987-10a0-4c24-b021-82de4c751d64', -- 748263 Janet
  '1252fba7-0ff8-42ba-8eed-238aa0214843', -- 939013 Rosemary
  '3fd2c264-da0b-49b5-ac80-b74e09613d61', -- 839022 Millicent
  'c038c0bf-1ce5-423c-9ec3-f82e259e123b', -- 966284 Rispa
  '3fce500c-5fb4-4d08-8aca-d73f81a7b1f3', -- 679652 Alice
  '76e1912c-85ec-4339-a783-270016b991ea'  -- 720774 Suslia
]::uuid[]);
create unique index on bripta_moved_loans(id);
create temporary table bripta_financial_before on commit preserve rows as
select l.id,l.total_paid,l.outstanding_balance,l.status,l.arrears_amount,l.overdue_days
from public.loans l join bripta_moved_loans m on m.id=l.id;

do $$
declare
  v_business constant text:='BIZ-B3F5E5D9';
  v_george constant uuid:='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99';
  v_migori constant uuid:='00000000-0000-4000-8000-000000000001';
  v_mbita constant uuid:='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338';
  v_table text;
  v_count integer;
  v_balance_trigger_enabled boolean;
begin
  perform pg_advisory_xact_lock(hashtext('bripta_george_mbita_exact_ten'));
  select count(*) into v_count from bripta_moved_loans;
  if v_count<>10 then raise exception 'Expected ten exact loan IDs, found %',v_count; end if;
  if not exists(select 1 from public.bripta_branches where id=v_mbita and business_id=v_business and name ilike '%mbita%')
    or not exists(select 1 from public.bripta_branches where id=v_migori and business_id=v_business)
  then raise exception 'The expected Bripta branches were not found'; end if;
  if not exists(select 1 from public.loan_staff where id=v_george and business_id=v_business and branch_id in(v_migori,v_mbita))
  then raise exception 'George staff record is missing or already in another branch'; end if;
  if exists(
    select 1 from public.loans l join bripta_moved_loans m on m.id=l.id
    where l.business_id<>v_business or l.loan_officer_id<>v_george
      or l.branch_id not in(v_migori,v_mbita)
      or l.created_at < timestamptz '2026-10-19 10:09:18+00'
  ) then raise exception 'One of the ten loans no longer matches the reviewed Mbita evidence'; end if;
  if (select count(distinct client_id) from bripta_moved_loans)<>10
  then raise exception 'Expected ten distinct clients; review before moving client branches'; end if;
  if exists(
    select 1 from public.loan_clients c join bripta_moved_loans m on m.client_id=c.id
    where c.business_id<>v_business or c.loan_officer_id<>v_george
      or c.branch_id not in(v_migori,v_mbita)
  ) then raise exception 'An affected client now belongs to another business, officer, or branch'; end if;

  -- Current staff and selected clients move to Mbita. Their earlier loans do not.
  update public.loan_staff set branch_id=v_mbita
    where id=v_george and business_id=v_business and branch_id is distinct from v_mbita;
  update public.bripta_staff_permissions set branch_id=v_mbita
    where staff_id=v_george and business_id=v_business and branch_id is distinct from v_mbita;
  update public.loan_clients c set branch_id=v_mbita,updated_at=now()
    where c.id in(select client_id from bripta_moved_loans)
      and c.business_id=v_business and c.branch_id is distinct from v_mbita;

  -- Only the approved ten loans and their related records change branch.
  update public.loans l set branch_id=v_mbita,updated_at=now()
    where l.id in(select id from bripta_moved_loans)
      and l.business_id=v_business and l.branch_id is distinct from v_mbita;
  update public.loan_applications a set branch_id=v_mbita,updated_at=now()
    where a.business_id=v_business
      and (a.id in(select application_id from bripta_moved_loans where application_id is not null)
        or (a.client_id in(select client_id from bripta_moved_loans)
          and a.status in('draft','submitted','under_review','approved')))
      and a.branch_id is distinct from v_mbita;

  -- Some installations still have a repayment UPDATE trigger that recalculates
  -- loan balances. Disable only that trigger for branch-only updates, then
  -- restore its original enabled state before committing.
  select exists(select 1 from pg_trigger
    where tgrelid='public.loan_repayments'::regclass
      and tgname='bripta_sync_loan_balance_trigger' and tgenabled='O')
  into v_balance_trigger_enabled;
  if v_balance_trigger_enabled then
    execute 'alter table public.loan_repayments disable trigger bripta_sync_loan_balance_trigger';
  end if;
  foreach v_table in array array[
    'loan_schedules','loan_repayments','loan_penalties',
    'bripta_charges','bripta_excess_ledger','journal_entries'
  ] loop
    if to_regclass('public.'||v_table) is not null
      and exists(select 1 from information_schema.columns where table_schema='public' and table_name=v_table and column_name='loan_id')
      and exists(select 1 from information_schema.columns where table_schema='public' and table_name=v_table and column_name='loan_id' and udt_name='uuid')
      and exists(select 1 from information_schema.columns where table_schema='public' and table_name=v_table and column_name='branch_id')
      and exists(select 1 from information_schema.columns where table_schema='public' and table_name=v_table and column_name='business_id')
    then
      execute format(
        'update public.%I x set branch_id=$1 where x.business_id=$2 and x.loan_id in(select id from bripta_moved_loans) and x.branch_id is distinct from $1',
        v_table
      ) using v_mbita,v_business;
    end if;
  end loop;
  if v_balance_trigger_enabled then
    execute 'alter table public.loan_repayments enable trigger bripta_sync_loan_balance_trigger';
  end if;

  update public.bripta_accounting_entries e set branch_id=v_mbita
    where e.business_id=v_business and e.branch_id is distinct from v_mbita
      and (e.loan_id in(select id from bripta_moved_loans)
        or (e.source_table='loans' and e.source_id in(select id::text from bripta_moved_loans))
        or (e.source_table='loan_repayments' and e.source_id in(
          select r.id::text from public.loan_repayments r
          where r.loan_id in(select id from bripta_moved_loans))));

  if to_regclass('public.bripta_excess_allocations') is not null then
    update public.bripta_excess_allocations a set branch_id=v_mbita
      from public.bripta_excess_ledger e
      where a.excess_id=e.id and a.business_id=v_business
        and e.loan_id in(select id from bripta_moved_loans)
        and a.branch_id is distinct from v_mbita;
  end if;
  update public.mpesa_callback_queue q set branch_id=v_mbita
    where q.branch_id is distinct from v_mbita
      and (q.loan_id in(select id from bripta_moved_loans)
        or q.repayment_id in(select r.id from public.loan_repayments r
          where r.loan_id in(select id from bripta_moved_loans)));

  if exists(
    select 1 from bripta_financial_before b join public.loans l on l.id=b.id
    where (l.total_paid,l.outstanding_balance,l.status,l.arrears_amount,l.overdue_days)
      is distinct from
      (b.total_paid,b.outstanding_balance,b.status,b.arrears_amount,b.overdue_days)
  ) then raise exception 'A financial loan value changed; the whole branch repair was rolled back'; end if;

  if not exists(
    select 1 from public.bripta_domain_audit
    where business_id=v_business and action='george_mbita_exact_ten_repair_20261025'
  ) then
    insert into public.bripta_domain_audit(
      business_id,branch_id,actor_staff_id,action,entity_type,new_value
    ) values (
      v_business,v_mbita,null,'george_mbita_exact_ten_repair_20261025','loans',
      jsonb_build_object('loan_ids',(select jsonb_agg(id order by id) from bripta_moved_loans),
        'reason','Owner confirmed all ten loans opened after Mbita branch creation belong to Mbita',
        'amounts_changed',false)
    );
  end if;
end $$;

commit;

-- These results should show all ten selected loans in Mbita and George there.
select 'george_staff_branch' as check_name,count(*)::numeric as result
from public.loan_staff where id='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'
  and branch_id='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'
union all select 'selected_loans_in_mbita',count(*)
from public.loans where id in(select id from bripta_moved_loans)
  and branch_id='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'
union all select 'selected_clients_in_mbita',count(*)
from public.loan_clients where id in(select client_id from bripta_moved_loans)
  and branch_id='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'
union all select 'older_george_loans_in_migori',count(*)
from public.loans where business_id='BIZ-B3F5E5D9'
  and loan_officer_id='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'
  and id not in(select id from bripta_moved_loans)
  and branch_id='00000000-0000-4000-8000-000000000001'
union all select 'selected_schedules_wrong_branch',count(*)
from public.loan_schedules where loan_id in(select id from bripta_moved_loans)
  and branch_id is distinct from '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'::uuid
union all select 'selected_repayments_wrong_branch',count(*)
from public.loan_repayments where loan_id in(select id from bripta_moved_loans)
  and branch_id is distinct from '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'::uuid
union all select 'selected_accounting_wrong_branch',count(*)
from public.bripta_accounting_entries where loan_id in(select id from bripta_moved_loans)
  and branch_id is distinct from '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'::uuid
order by check_name;
