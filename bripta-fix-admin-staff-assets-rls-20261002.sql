-- Restore Bripta admin recognition for Staff and Company Assets.
-- Functions only: no staff, financial, SMS or asset rows are updated.

begin;

create or replace function public.bripta_current_staff()
returns public.loan_staff
language sql
stable
security definer
set search_path=public
as $$
  select s
  from public.loan_staff s
  where s.business_id='BIZ-B3F5E5D9'
    and coalesce(s.is_active,true)
    and (
      s.auth_user_id=auth.uid()
      or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email',''))
    )
  order by case when s.auth_user_id=auth.uid() then 0 else 1 end
  limit 1
$$;

create or replace function public.bripta_has_role(p_role text)
returns boolean
language sql
stable
security definer
set search_path=public
as $$
  select exists(
    select 1
    from public.loan_staff s
    where s.business_id='BIZ-B3F5E5D9'
      and coalesce(s.is_active,true)
      and (
        s.auth_user_id=auth.uid()
        or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email',''))
      )
      and lower(trim(p_role))=any(
        regexp_split_to_array(lower(coalesce(s.role,'')),'\s*,\s*')
      )
  )
$$;

create or replace function public.bripta_has_permission(p_permission text)
returns boolean
language plpgsql
stable
security definer
set search_path=public
as $$
declare v boolean:=false;
begin
  if public.bripta_has_role('admin') then return true; end if;
  execute format(
    'select coalesce(%I,false)
       from public.bripta_staff_permissions p
       join public.loan_staff s on s.id=p.staff_id
      where s.business_id=''BIZ-B3F5E5D9''
        and (s.auth_user_id=$1 or lower(s.email)=lower($2))',
    p_permission
  ) into v using auth.uid(),coalesce(auth.jwt()->>'email','');
  return coalesce(v,false);
end $$;

commit;

select p.proname as installed_function
from pg_proc p
join pg_namespace n on n.oid=p.pronamespace
where n.nspname='public'
  and p.proname in ('bripta_current_staff','bripta_has_role','bripta_has_permission')
order by p.proname;
