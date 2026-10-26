-- Bripta only. A signed-in staff member records their own last activity via
-- a narrowly scoped server function, avoiding the loan_staff RLS UPDATE error.
-- Existing accounts, passwords, roles, branches and financial rows are unchanged.

create or replace function public.bripta_record_staff_login()
returns timestamptz
language plpgsql
security definer
set search_path = public
as $$
declare
  v_seen timestamptz;
begin
  if auth.uid() is null then
    raise exception 'Authentication required';
  end if;
  update public.loan_staff
  set last_login = now()
  where business_id = 'BIZ-B3F5E5D9'
    and auth_user_id = auth.uid()
    and coalesce(is_active,true)
  returning last_login into v_seen;
  if v_seen is null then
    raise exception 'No active Bripta staff account matches this login';
  end if;
  return v_seen;
end
$$;

revoke all on function public.bripta_record_staff_login() from public, anon;
grant execute on function public.bripta_record_staff_login() to authenticated;

-- James already signed in after his account was repaired. Copy only the
-- verified Auth sign-in timestamp to his staff row if it is newer.
update public.loan_staff s
set last_login = u.last_sign_in_at
from auth.users u
where s.business_id = 'BIZ-B3F5E5D9'
  and s.auth_user_id = 'e407a976-5b44-46e1-817a-95da54103aca'
  and u.id = s.auth_user_id
  and lower(u.email) = 'opiyoj128@gmail.com'
  and u.last_sign_in_at is not null
  and (s.last_login is null or s.last_login < u.last_sign_in_at);

select s.name, s.email, s.role, b.name as branch,
       s.last_login as staff_last_login,
       u.last_sign_in_at as auth_last_sign_in,
       (s.last_login is not null) as last_login_visible
from public.loan_staff s
join auth.users u on u.id = s.auth_user_id
left join public.bripta_branches b on b.id = s.branch_id
where s.business_id = 'BIZ-B3F5E5D9'
  and s.auth_user_id = 'e407a976-5b44-46e1-817a-95da54103aca';
