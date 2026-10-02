-- Rename only Bripta's existing Head Office. No financial or membership updates.
begin;
do $$
begin
  if not exists (
    select 1 from public.bripta_branches
    where id='00000000-0000-4000-8000-000000000001'::uuid
      and business_id='BIZ-B3F5E5D9' and is_head_office
  ) then
    raise exception 'Expected Bripta Head Office was not found; nothing changed.';
  end if;
  update public.bripta_branches set name='Migori',updated_at=now()
  where id='00000000-0000-4000-8000-000000000001'::uuid
    and business_id='BIZ-B3F5E5D9' and is_head_office
    and name is distinct from 'Migori';
end $$;
select id,business_id,name,is_head_office from public.bripta_branches
where business_id='BIZ-B3F5E5D9';
commit;
