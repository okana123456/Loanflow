-- Read-only verification after deploying and using Remove Access for William.
-- Brian and George should remain active; William should be inactive with no Auth login.
select s.name, s.email, s.is_active, s.auth_user_id,
       (u.id is not null) as auth_login_exists,
       (select count(*) from public.loans l
        where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=s.id
          and l.status='active') as active_loans,
       (select count(*) from public.loan_clients c
        where c.business_id='BIZ-B3F5E5D9' and c.loan_officer_id=s.id) as assigned_clients
from public.loan_staff s
left join auth.users u on u.id=s.auth_user_id
where s.business_id='BIZ-B3F5E5D9'
  and lower(regexp_replace(trim(s.name),'\s+',' ','g')) in
    ('brian ochieng','george majwa','william omuya')
order by s.name;
