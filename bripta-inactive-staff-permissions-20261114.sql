-- Bripta only. Idempotent permission fix; changes no staff or financial rows.
-- An inactive staff member must not retain explicit permissions through an
-- already-issued access token.
begin;

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
        and coalesce(s.is_active,true)
        and (s.auth_user_id=$1 or lower(s.email)=lower($2))',
    p_permission
  ) into v using auth.uid(),coalesce(auth.jwt()->>'email','');
  return coalesce(v,false);
end $$;

commit;
