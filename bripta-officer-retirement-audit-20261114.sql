-- Read-only. Run in the Bripta Supabase project's SQL Editor before retiring
-- William. No staff, authentication, loan, or accounting row is changed.
with people as (
  select s.id, s.name, s.email, s.role, s.branch_id, s.is_active,
         s.auth_user_id, b.name as branch
  from public.loan_staff s
  left join public.bripta_branches b on b.id=s.branch_id
  where s.business_id='BIZ-B3F5E5D9'
    and lower(regexp_replace(trim(s.name),'\s+',' ','g')) in
      ('brian ochieng','george majwa','william omuya')
)
select p.name, p.email, p.role, p.branch, p.is_active, p.id as staff_id,
       p.auth_user_id, (u.id is not null) as auth_login_exists,
       (select count(*) from public.loan_clients c
          where c.business_id='BIZ-B3F5E5D9' and c.loan_officer_id=p.id) as assigned_clients,
       (select count(*) from public.loans l
          where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=p.id
            and l.status='active') as active_loans,
       (select count(*) from public.loan_applications a
          where a.business_id='BIZ-B3F5E5D9' and a.loan_officer_id=p.id
            and a.status in ('pending','approved','submitted','under_review')) as open_applications,
       (select count(*) from public.loans l
          where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=p.id) as historical_loans,
       (select count(*) from public.loan_staff other
          where other.auth_user_id=p.auth_user_id and other.id<>p.id
            and p.auth_user_id is not null) as other_staff_using_login
from people p
left join auth.users u on u.id=p.auth_user_id
order by p.name, p.email;
