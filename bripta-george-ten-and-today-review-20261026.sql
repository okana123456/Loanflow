-- READ ONLY. Identify the three confirmed Mbita clients by ID number,
-- review the seven others, and find records George added on 26 October 2026
-- in Kenya time. No client, loan, repayment, ledger or SMS row is changed.
with selected(id) as (
  select unnest(array[
    '5f44fc32-41f7-48c3-8eec-b71c6584c294',
    'f8106a47-bba1-4585-8dff-5f7bd7fff99d',
    '33e3a535-7922-4624-82bf-7364642ed71b',
    '0e9de4dc-fc33-41ea-bde6-99dfafb2a31b',
    '9bcf0987-10a0-4c24-b021-82de4c751d64',
    '1252fba7-0ff8-42ba-8eed-238aa0214843',
    '3fd2c264-da0b-49b5-ac80-b74e09613d61',
    'c038c0bf-1ce5-423c-9ec3-f82e259e123b',
    '3fce500c-5fb4-4d08-8aca-d73f81a7b1f3',
    '76e1912c-85ec-4339-a783-270016b991ea'
  ]::uuid[])
), reviewed as (
  select 'original_ten'::text as source,
         c.id as client_id, c.full_name, c.id_number,
         c.created_at as client_created_at_utc, c.created_by,
         c.branch_id as client_branch_id, c.loan_officer_id as client_officer_id,
         l.id as loan_id, l.loan_no, l.created_at as loan_created_at_utc,
         l.status as loan_status, l.branch_id as loan_branch_id,
         l.loan_officer_id as loan_officer_id,
         l.application_id,
         case when regexp_replace(coalesce(c.id_number,''), '\D','','g')
                   in ('13657272','13657265','27365745')
              then 'owner_confirmed_mbita' else 'review_for_migori' end as owner_instruction
  from selected x
  join public.loans l on l.id=x.id and l.business_id='BIZ-B3F5E5D9'
  join public.loan_clients c on c.id=l.client_id and c.business_id=l.business_id

  union all

  select 'today_loan',c.id,c.full_name,c.id_number,c.created_at,c.created_by,
         c.branch_id,c.loan_officer_id,l.id,l.loan_no,l.created_at,l.status,
         l.branch_id,l.loan_officer_id,l.application_id,'confirm_today_branch'
  from public.loans l
  join public.loan_clients c on c.id=l.client_id and c.business_id=l.business_id
  where l.business_id='BIZ-B3F5E5D9'
    and l.loan_officer_id='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'
    and (l.created_at at time zone 'Africa/Nairobi')::date=date '2026-10-26'
    and l.id not in(select id from selected)

  union all

  select 'today_client',c.id,c.full_name,c.id_number,c.created_at,c.created_by,
         c.branch_id,c.loan_officer_id,null::uuid,null::text,null::timestamptz,
         null::text,null::uuid,null::uuid,null::uuid,'confirm_today_branch'
  from public.loan_clients c
  where c.business_id='BIZ-B3F5E5D9'
    and (c.loan_officer_id='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'
         or c.created_by='74b5e8ca-fabf-4890-b17b-5f6fc80e0d99')
    and (c.created_at at time zone 'Africa/Nairobi')::date=date '2026-10-26'
)
select r.source,
       count(*) filter (where r.source='original_ten') over () as original_ten_found,
       r.owner_instruction,r.full_name,r.id_number,r.client_id,
       cb.name as client_branch,r.client_officer_id,
       r.loan_no,r.loan_id,r.loan_status,lb.name as loan_branch,
       r.loan_officer_id,r.application_id,
       r.client_created_at_utc,r.loan_created_at_utc,
       (select count(*) from public.loans x where x.client_id=r.client_id
         and x.id is distinct from r.loan_id) as other_loans_for_client,
       (select count(*) from public.loan_schedules x where x.loan_id=r.loan_id) as schedules,
       (select count(*) from public.loan_repayments x where x.loan_id=r.loan_id) as repayments,
       (select count(*) from public.bripta_accounting_entries x where x.loan_id=r.loan_id) as accounting_entries
from reviewed r
left join public.bripta_branches cb on cb.id=r.client_branch_id
left join public.bripta_branches lb on lb.id=r.loan_branch_id
order by r.source,r.client_created_at_utc,r.full_name,r.loan_no;
