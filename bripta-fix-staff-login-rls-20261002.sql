-- Fix the staff-login bootstrap loop introduced by the branch restriction.
-- This changes only the loan_staff access policy; it changes no staff record.

begin;

alter table public.loan_staff enable row level security;

drop policy if exists bripta_staff_branch_boundary on public.loan_staff;

create policy bripta_staff_branch_boundary
on public.loan_staff
as restrictive
for all
to authenticated
using (
  public.bripta_has_role('admin')
  or id=(public.bripta_current_staff()).id
  or (
    public.bripta_has_role('branch_manager')
    and branch_id=(public.bripta_current_staff()).branch_id
  )
  or lower(coalesce(email,''))=lower(coalesce(auth.jwt()->>'email',''))
)
with check (
  public.bripta_has_role('admin')
  or id=(public.bripta_current_staff()).id
  or (
    public.bripta_has_role('branch_manager')
    and branch_id=(public.bripta_current_staff()).branch_id
  )
  or lower(coalesce(email,''))=lower(coalesce(auth.jwt()->>'email',''))
);

commit;

select policyname,permissive,roles,cmd,qual,with_check
from pg_policies
where schemaname='public'
  and tablename='loan_staff'
  and policyname='bripta_staff_branch_boundary';
