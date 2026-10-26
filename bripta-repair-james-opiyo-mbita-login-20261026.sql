-- Repair only James Opiyo's previously confirmed Auth login. No password,
-- financial record, existing staff row, or other business is changed.
-- Run in Bripta's Supabase SQL Editor after reviewing the exact Auth ID/email.

begin;

do $$
declare
  v_auth_id constant uuid := 'e407a976-5b44-46e1-817a-95da54103aca';
  v_email constant text := 'opiyoj128@gmail.com';
  v_business constant text := 'BIZ-B3F5E5D9';
  v_branch_id uuid;
  v_staff_id uuid;
  v_other_staff_count integer;
  v_existing public.loan_staff%rowtype;
begin
  if not exists (
    select 1 from auth.users
    where id = v_auth_id
      and lower(email) = v_email
      and email_confirmed_at is not null
  ) then
    raise exception 'James Auth ID/email is missing, changed, or unconfirmed; no repair made';
  end if;

  select id into v_branch_id
  from public.bripta_branches
  where id = '1bef00d9-7b1c-40e2-9cbc-54b1f7ca8338'
    and business_id = v_business
    and lower(regexp_replace(trim(name), '\s+', ' ', 'g')) = 'mbita branch'
    and is_active = true;
  if v_branch_id is null then
    raise exception 'Active Mbita branch was not found for Bripta; no repair made';
  end if;

  select count(*) into v_other_staff_count
  from public.loan_staff
  where business_id is distinct from v_business
    and (auth_user_id = v_auth_id or lower(email) = v_email);
  if v_other_staff_count > 0 then
    raise exception 'This login/email is already connected to another business; no repair made';
  end if;

  select * into v_existing
  from public.loan_staff
  where business_id = v_business
    and (auth_user_id = v_auth_id or lower(email) = v_email)
  limit 1;

  if v_existing.id is not null then
    if v_existing.auth_user_id is distinct from v_auth_id
       or lower(v_existing.email) is distinct from v_email
       or v_existing.branch_id is distinct from v_branch_id
       or v_existing.role is distinct from 'loan_officer'
       or v_existing.is_active is distinct from true then
      raise exception 'An existing James staff row needs individual review; no repair made';
    end if;
    v_staff_id := v_existing.id;
  else
    insert into public.loan_staff (
      business_id, auth_user_id, name, email, role, is_active, branch_id
    ) values (
      v_business, v_auth_id, 'James Opiyo', v_email, 'loan_officer', true, v_branch_id
    ) returning id into v_staff_id;
  end if;

  insert into public.bripta_staff_permissions (staff_id, business_id, branch_id)
  values (v_staff_id, v_business, v_branch_id)
  on conflict (staff_id) do nothing;
end
$$;

select s.name, s.email, s.role, b.name as branch,
       s.is_active, s.auth_user_id,
       (u.email_confirmed_at is not null) as login_email_confirmed
from public.loan_staff s
join public.bripta_branches b on b.id = s.branch_id
join auth.users u on u.id = s.auth_user_id
where s.business_id = 'BIZ-B3F5E5D9'
  and s.auth_user_id = 'e407a976-5b44-46e1-817a-95da54103aca';

commit;
