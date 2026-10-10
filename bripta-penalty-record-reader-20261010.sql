-- Read-only visibility fix for existing Bripta penalty records.
-- No penalty calculation, posting, branch backfill or financial updates.
begin;
create or replace function public.bripta_loan_penalty_records(p_loan_id uuid)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; target public.loans%rowtype; roles text[]; records jsonb;
begin
  if auth.uid() is null then raise exception 'Sign in required' using errcode='42501'; end if;
  select s.* into actor from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if actor.id is null or not (roles && array['admin','branch_manager','cashier','loan_officer']) then
    raise exception 'Loan penalty access is not permitted' using errcode='42501'; end if;
  select l.* into target from public.loans l where l.id=p_loan_id and l.business_id=actor.business_id;
  if target.id is null then raise exception 'Loan not found in your business' using errcode='42501'; end if;
  if not ('admin'=any(roles)) then
    if actor.branch_id is null or target.branch_id is distinct from actor.branch_id then
      raise exception 'Loan is outside your branch' using errcode='42501'; end if;
    if not (roles && array['branch_manager','cashier']) and target.loan_officer_id is distinct from actor.id then
      raise exception 'Loan is outside your assigned portfolio' using errcode='42501'; end if;
  end if;
  -- Loan ownership controls access, including older records with missing
  -- penalty branch metadata. The parent loan must pass the checks above.
  select coalesce(jsonb_agg(to_jsonb(p) order by p.date_charged nulls last,p.id),'[]') into records
    from public.loan_penalties p where p.loan_id=target.id;
  return jsonb_build_object('loan_id',target.id,'rows',records);
end $$;
revoke all on function public.bripta_loan_penalty_records(uuid) from public,anon;
grant execute on function public.bripta_loan_penalty_records(uuid) to authenticated;
notify pgrst,'reload schema';
commit;
select to_regprocedure('public.bripta_loan_penalty_records(uuid)') is not null as penalty_record_reader_installed;
