-- READ ONLY. Show the actual branch rows and George's current assignment.
-- Run in the same Supabase project as the James Opiyo login check.
select 'branch' as source,
       b.business_id,
       b.id as branch_id,
       b.code as branch_code,
       b.name as name,
       b.is_active,
       (select count(*) from public.loan_staff s where s.branch_id = b.id) as staff_count
from public.bripta_branches b

union all

select 'george_staff' as source,
       s.business_id,
       s.branch_id,
       null::text as branch_code,
       s.name,
       s.is_active,
       null::bigint as staff_count
from public.loan_staff s
where lower(regexp_replace(coalesce(s.name, ''), '\s+', ' ', 'g')) like '%george%majwa%'

order by source, business_id, name;
