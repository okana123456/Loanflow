-- Bripta only. The owner corrected the earlier ten-loan branch instruction:
-- Tabitha 13657272, Judith 13657265 and Rose Akeyo 27365745 belong to
-- Mbita; the other seven belong to Migori. Caroline and Bearyl, added on
-- 26 October, also belong to Mbita and are not selected here.
-- Changes only branch attribution for these seven loans and their linked
-- records. No payment amount, loan balance or status changes. Rispa's client
-- record had no officer although her selected loan belongs to George; it is
-- temporarily assigned to George so it appears in the Migori handover group.
-- Safe to rerun after success; stops if any reviewed fact has changed.

begin;

drop table if exists pg_temp.bripta_seven_migori;
create temporary table bripta_seven_migori on commit drop as
select l.id, l.client_id, l.application_id, l.loan_no,
       c.id_number, c.loan_officer_id as client_officer_id
from (values
  ('0e9de4dc-fc33-41ea-bde6-99dfafb2a31b'::uuid,'30983310'), -- Rose Awino
  ('9bcf0987-10a0-4c24-b021-82de4c751d64'::uuid,'11557687'), -- Janet
  ('1252fba7-0ff8-42ba-8eed-238aa0214843'::uuid,'31358107'), -- Rosemary
  ('3fd2c264-da0b-49b5-ac80-b74e09613d61'::uuid,'29912940'), -- Millicent
  ('c038c0bf-1ce5-423c-9ec3-f82e259e123b'::uuid,'32159891'), -- Rispa
  ('3fce500c-5fb4-4d08-8aca-d73f81a7b1f3'::uuid,'322429663'), -- Alice
  ('76e1912c-85ec-4339-a783-270016b991ea'::uuid,'6896810')  -- Suslia
) v(loan_id,expected_id_number)
join public.loans l on l.id=v.loan_id and l.business_id='BIZ-B3F5E5D9'
join public.loan_clients c on c.id=l.client_id and c.business_id=l.business_id
where regexp_replace(coalesce(c.id_number,''),'\D','','g')=v.expected_id_number;
create unique index on bripta_seven_migori(id);

drop table if exists pg_temp.bripta_seven_loan_before;
create temporary table bripta_seven_loan_before on commit drop as
select l.id, to_jsonb(l)-'branch_id'-'updated_at' as unchanged_values
from public.loans l join bripta_seven_migori m on m.id=l.id;

drop table if exists pg_temp.bripta_seven_repayment_before;
create temporary table bripta_seven_repayment_before on commit drop as
select r.id, to_jsonb(r)-'branch_id'-'updated_at' as unchanged_values
from public.loan_repayments r join bripta_seven_migori m on m.id=r.loan_id;

drop table if exists pg_temp.bripta_seven_schedule_before;
create temporary table bripta_seven_schedule_before on commit drop as
select s.id, to_jsonb(s)-'branch_id'-'updated_at' as unchanged_values
from public.loan_schedules s join bripta_seven_migori m on m.id=s.loan_id;

do $$
declare
  v_business constant text := 'BIZ-B3F5E5D9';
  v_george constant uuid := '74b5e8ca-fabf-4890-b17b-5f6fc80e0d99';
  v_migori constant uuid := '00000000-0000-4000-8000-000000000001';
  v_mbita constant uuid := '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338';
  v_table text;
  v_trigger_enabled boolean;
begin
  perform pg_advisory_xact_lock(hashtext('bripta_george_seven_to_migori_20261026'));
  if (select count(*) from bripta_seven_migori) <> 7
     or (select count(distinct client_id) from bripta_seven_migori) <> 7 then
    raise exception 'Expected exactly seven reviewed loans and clients; no changes made';
  end if;
  if not exists(select 1 from public.bripta_branches
                where id=v_migori and business_id=v_business and is_active=true)
     or not exists(select 1 from public.bripta_branches
                where id=v_mbita and business_id=v_business and is_active=true) then
    raise exception 'Bripta branch IDs have changed; no changes made';
  end if;
  if exists(
    select 1 from bripta_seven_migori m
    join public.loans l on l.id=m.id
    join public.loan_clients c on c.id=m.client_id
    where l.branch_id not in(v_migori,v_mbita)
       or c.branch_id not in(v_migori,v_mbita)
       or l.loan_officer_id is distinct from v_george
       or (c.loan_officer_id is distinct from v_george
           and not (m.id='c038c0bf-1ce5-423c-9ec3-f82e259e123b'::uuid
                    and c.loan_officer_id is null))
  ) then
    raise exception 'One of the seven reviewed rows changed since the audit; no changes made';
  end if;
  if exists(
    select 1 from public.loans l
    join bripta_seven_migori m on m.client_id=l.client_id and m.id<>l.id
    where l.business_id=v_business and l.branch_id=v_mbita
  ) then
    raise exception 'A selected client has another Mbita loan; review before moving client branch';
  end if;
  if (select count(*) from public.loan_clients
      where business_id=v_business and id_number in('13657272','13657265','27365745','30179891','36439143')
        and branch_id=v_mbita) <> 5 then
    raise exception 'The five confirmed Mbita clients are not all in Mbita; no changes made';
  end if;

  update public.loan_clients c
  set branch_id=v_migori,
      loan_officer_id=coalesce(c.loan_officer_id,v_george),
      updated_at=now()
  where c.id in(select client_id from bripta_seven_migori)
    and c.business_id=v_business
    and (c.branch_id is distinct from v_migori or c.loan_officer_id is null);
  update public.loans l set branch_id=v_migori,updated_at=now()
  where l.id in(select id from bripta_seven_migori)
    and l.business_id=v_business and l.branch_id is distinct from v_migori;
  update public.loan_applications a set branch_id=v_migori,updated_at=now()
  where a.business_id=v_business and a.branch_id is distinct from v_migori
    and (a.id in(select application_id from bripta_seven_migori where application_id is not null)
      or (a.client_id in(select client_id from bripta_seven_migori)
        and a.status in('draft','submitted','under_review','approved')));

  -- Some deployments recalculate loan balances on repayment UPDATE. Suppress
  -- that trigger only during branch-only repayment changes, then restore it.
  select exists(select 1 from pg_trigger
    where tgrelid='public.loan_repayments'::regclass
      and tgname='bripta_sync_loan_balance_trigger' and tgenabled='O')
  into v_trigger_enabled;
  if v_trigger_enabled then
    alter table public.loan_repayments disable trigger bripta_sync_loan_balance_trigger;
  end if;
  foreach v_table in array array[
    'loan_schedules','loan_repayments','loan_penalties',
    'bripta_charges','bripta_excess_ledger','journal_entries'
  ] loop
    if to_regclass('public.'||v_table) is not null
      and exists(select 1 from information_schema.columns
        where table_schema='public' and table_name=v_table
          and column_name='loan_id' and udt_name='uuid')
      and exists(select 1 from information_schema.columns
        where table_schema='public' and table_name=v_table and column_name='branch_id')
      and exists(select 1 from information_schema.columns
        where table_schema='public' and table_name=v_table and column_name='business_id')
    then
      execute format(
        'update public.%I x set branch_id=$1 where x.business_id=$2 and x.loan_id in(select id from bripta_seven_migori) and x.branch_id is distinct from $1',
        v_table
      ) using v_migori,v_business;
    end if;
  end loop;
  if v_trigger_enabled then
    alter table public.loan_repayments enable trigger bripta_sync_loan_balance_trigger;
  end if;

  update public.bripta_accounting_entries e set branch_id=v_migori
  where e.business_id=v_business and e.branch_id is distinct from v_migori
    and (e.loan_id in(select id from bripta_seven_migori)
      or (e.source_table='loans' and e.source_id in(select id::text from bripta_seven_migori))
      or (e.source_table='loan_repayments' and e.source_id in(
        select r.id::text from public.loan_repayments r
        where r.loan_id in(select id from bripta_seven_migori))));
  if to_regclass('public.bripta_excess_allocations') is not null then
    update public.bripta_excess_allocations a set branch_id=v_migori
    from public.bripta_excess_ledger e
    where a.excess_id=e.id and a.business_id=v_business
      and e.loan_id in(select id from bripta_seven_migori)
      and a.branch_id is distinct from v_migori;
  end if;
  update public.mpesa_callback_queue q set branch_id=v_migori
  where q.branch_id is distinct from v_migori
    and (q.loan_id in(select id from bripta_seven_migori)
      or q.repayment_id in(select r.id from public.loan_repayments r
        where r.loan_id in(select id from bripta_seven_migori)));

  if exists(select 1 from bripta_seven_loan_before b join public.loans l on l.id=b.id
    where (to_jsonb(l)-'branch_id'-'updated_at') is distinct from b.unchanged_values)
    or exists(select 1 from bripta_seven_repayment_before b
      join public.loan_repayments r on r.id=b.id
      where (to_jsonb(r)-'branch_id'-'updated_at') is distinct from b.unchanged_values)
    or exists(select 1 from bripta_seven_schedule_before b
      join public.loan_schedules s on s.id=b.id
      where (to_jsonb(s)-'branch_id'-'updated_at') is distinct from b.unchanged_values)
  then
    raise exception 'A non-branch loan, repayment or schedule value changed; all changes rolled back';
  end if;
  if exists(select 1 from bripta_seven_migori m
    join public.loan_clients c on c.id=m.client_id
    join public.loans l on l.id=m.id
    where c.branch_id<>v_migori or l.branch_id<>v_migori
      or c.loan_officer_id is distinct from v_george)
  then
    raise exception 'A selected client or loan did not move to Migori; all changes rolled back';
  end if;

  if not exists(select 1 from public.bripta_domain_audit
    where business_id=v_business and action='george_seven_migori_correction_20261026') then
    insert into public.bripta_domain_audit(
      business_id,branch_id,actor_staff_id,action,entity_type,new_value
    ) values (
      v_business,v_migori,null,'george_seven_migori_correction_20261026','loans',
      jsonb_build_object('loan_ids',(select jsonb_agg(id order by id) from bripta_seven_migori),
        'reason','Owner corrected earlier branch instruction: only Tabitha, Judith and Rose Akeyo of the original ten belong to Mbita; Caroline and Bearyl added today also belong to Mbita',
        'financial_values_changed',false,
        'rispa_missing_client_officer_restored_to_george',true)
    );
  end if;
end
$$;

select 'reviewed_loans_now_in_migori' as check_name,count(*)::numeric as result
from public.loans where id in(select id from bripta_seven_migori)
  and branch_id='00000000-0000-4000-8000-000000000001'
union all select 'reviewed_clients_now_in_migori',count(*)
from public.loan_clients where id in(select client_id from bripta_seven_migori)
  and branch_id='00000000-0000-4000-8000-000000000001'
union all select 'reviewed_repayments_in_migori',count(*)
from public.loan_repayments where loan_id in(select id from bripta_seven_migori)
  and branch_id='00000000-0000-4000-8000-000000000001'
union all select 'confirmed_mbita_clients_still_in_mbita',count(*)
from public.loan_clients
where business_id='BIZ-B3F5E5D9' and id_number in('13657272','13657265','27365745','30179891','36439143')
  and branch_id='1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'
order by check_name;

commit;
