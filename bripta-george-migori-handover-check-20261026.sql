-- READ ONLY. Assess George's remaining Migori clients before choosing an
-- officer. No assignment, loan, amount, repayment or branch is changed.
with ids as (
  select 'BIZ-B3F5E5D9'::text as business_id,
         '74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'::uuid as george_id,
         '00000000-0000-4000-8000-000000000001'::uuid as migori_id,
         '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'::uuid as mbita_id
), stranded as (
  select c.id
  from public.loan_clients c cross join ids i
  where c.business_id = i.business_id
    and c.branch_id = i.migori_id
    and c.loan_officer_id = i.george_id
), active_for_stranded as (
  select l.id, l.client_id, l.branch_id, l.loan_officer_id
  from public.loans l join stranded c on c.id = l.client_id
  cross join ids i
  where l.business_id = i.business_id and l.status = 'active'
), migori_candidates as (
  select s.id, s.name,
         (select count(*) from public.loan_clients c
          where c.business_id = s.business_id and c.branch_id = s.branch_id
            and c.loan_officer_id = s.id) as assigned_clients,
         (select count(*) from public.loans l
          where l.business_id = s.business_id and l.branch_id = s.branch_id
            and l.loan_officer_id = s.id and l.status = 'active') as active_loans
  from public.loan_staff s cross join ids i
  where s.business_id = i.business_id and s.branch_id = i.migori_id
    and s.is_active = true
    and 'loan_officer' = any(regexp_split_to_array(lower(coalesce(s.role,'')), '\s*,\s*'))
    and not ('admin' = any(regexp_split_to_array(lower(coalesce(s.role,'')), '\s*,\s*')))
    and not ('branch_manager' = any(regexp_split_to_array(lower(coalesce(s.role,'')), '\s*,\s*')))
)
select
  (select count(*) from stranded) as george_clients_remaining_in_migori,
  (select count(*) from active_for_stranded where branch_id = (select migori_id from ids)) as their_active_migori_loans,
  (select count(*) from active_for_stranded
   where branch_id = (select migori_id from ids)
     and loan_officer_id = (select george_id from ids)) as active_migori_loans_still_assigned_to_george,
  (select count(distinct client_id) from active_for_stranded
   where branch_id = (select mbita_id from ids)) as stranded_clients_with_active_mbita_loans,
  (select count(*) from public.loan_clients c cross join ids i
   where c.business_id = i.business_id and c.branch_id = i.migori_id
     and c.loan_officer_id is null) as migori_clients_with_no_officer,
  (select count(*) from public.loan_clients c cross join ids i
   left join public.loan_staff s on s.id = c.loan_officer_id
   where c.business_id = i.business_id and c.branch_id = i.migori_id
     and c.loan_officer_id is not null
     and (s.id is null or s.branch_id is distinct from i.migori_id
          or s.is_active is distinct from true)) as migori_clients_with_unavailable_officer,
  (select coalesce(jsonb_agg(to_jsonb(o) order by o.name), '[]'::jsonb)
   from migori_candidates o) as available_migori_officers;
