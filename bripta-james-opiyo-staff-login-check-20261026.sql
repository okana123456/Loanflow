-- READ ONLY. Check James Opiyo's Bripta staff row and matching Auth login.
-- Run only in the Bripta Supabase project. No password or token is returned.
with candidates as (
  select 'staff_name' as source, s.id as staff_id, s.name as staff_name,
         s.email as staff_email, s.auth_user_id, s.role, s.is_active,
         u.id as auth_id, u.email as auth_email, u.email_confirmed_at,
         u.created_at as auth_created_at
  from public.loan_staff s
  left join auth.users u on u.id = s.auth_user_id
                        or lower(u.email) = lower(s.email)
  where s.business_id = 'BIZ-B3F5E5D9'
    and lower(regexp_replace(coalesce(s.name,''), '\s+', ' ', 'g')) like '%james%opiyo%'

  union all

  select 'auth_name' as source, s.id, s.name, s.email, s.auth_user_id,
         s.role, s.is_active, u.id, u.email, u.email_confirmed_at, u.created_at
  from auth.users u
  left join public.loan_staff s on s.business_id = 'BIZ-B3F5E5D9'
                               and (s.auth_user_id = u.id or lower(s.email) = lower(u.email))
  where lower(regexp_replace(coalesce(u.raw_user_meta_data->>'full_name',''), '\s+', ' ', 'g')) like '%james%opiyo%'
)
select distinct source, staff_id, staff_name, staff_email, auth_user_id,
       role, is_active, auth_id, auth_email, email_confirmed_at, auth_created_at
from candidates
order by staff_name nulls last, auth_created_at desc nulls last;
