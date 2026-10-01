-- Allow the authenticated user to bootstrap their existing staff record.
-- This changes policies only and does not update any staff or financial data.

begin;

alter table public.loan_staff enable row level security;

drop policy if exists bripta_staff_login_bootstrap on public.loan_staff;

create policy bripta_staff_login_bootstrap
on public.loan_staff
as permissive
for select
to authenticated
using (
  auth_user_id=auth.uid()
  or lower(coalesce(email,''))=lower(coalesce(auth.jwt()->>'email',''))
);

create or replace function public.bripta_link_current_staff()
returns uuid
language plpgsql
security definer
set search_path=public
as $$
declare
  v_id uuid;
  v_email text:=lower(coalesce(auth.jwt()->>'email',''));
begin
  if auth.uid() is null or v_email='' then
    raise exception 'Authenticated email is required';
  end if;

  update public.loan_staff
  set auth_user_id=auth.uid(),last_login=now()
  where business_id='BIZ-B3F5E5D9'
    and lower(coalesce(email,''))=v_email
    and coalesce(is_active,true)
  returning id into v_id;

  if v_id is null then
    raise exception 'No active Bripta staff account matches this email';
  end if;
  return v_id;
end $$;

revoke all on function public.bripta_link_current_staff() from public,anon;
grant execute on function public.bripta_link_current_staff() to authenticated;

commit;

select policyname,permissive,roles,cmd,qual,with_check
from pg_policies
where schemaname='public'
  and tablename='loan_staff'
  and policyname in (
    'bripta_staff_branch_boundary',
    'bripta_staff_login_bootstrap'
  )
order by policyname;
