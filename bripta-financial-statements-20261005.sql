-- Read-only Bripta financial statements. No postings, backfill or balance changes.
begin;
create or replace function public.bripta_financial_statements(p_start date,p_end date,p_branch uuid default null)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog as $$
declare actor public.loan_staff%rowtype; roles text[]; branch uuid; accounts jsonb; categories jsonb;
  expenses numeric; missing jsonb; unbalanced integer; opening boolean;
begin
  if auth.uid() is null then raise exception 'Sign in required' using errcode='42501'; end if;
  if p_start is null or p_end is null or p_start>p_end then raise exception 'Choose a valid date range' using errcode='22023'; end if;
  select s.* into actor from public.loan_staff s where s.business_id='BIZ-B3F5E5D9' and coalesce(s.is_active,true)
    and (s.auth_user_id=auth.uid() or (nullif(trim(auth.jwt()->>'email'),'') is not null and lower(trim(s.email))=lower(trim(auth.jwt()->>'email'))))
    order by (s.auth_user_id=auth.uid()) desc nulls last,s.id limit 1;
  roles:=regexp_split_to_array(lower(trim(coalesce(actor.role,''))),'\s*,\s*');
  if actor.id is null or not ('admin'=any(roles) or 'branch_manager'=any(roles) or exists(
    select 1 from public.bripta_staff_permissions p where p.staff_id=actor.id and p.business_id=actor.business_id and p.view_accounting
  )) then raise exception 'Accounting permission required' using errcode='42501'; end if;
  if 'admin'=any(roles) then branch:=p_branch;
  else
    if actor.branch_id is null or (p_branch is not null and p_branch<>actor.branch_id) then
      raise exception 'Branch access is not permitted' using errcode='42501'; end if;
    branch:=actor.branch_id;
  end if;
  if branch is not null and not exists(select 1 from public.bripta_branches b where b.id=branch and b.business_id=actor.business_id) then
    raise exception 'Invalid Bripta branch' using errcode='42501'; end if;

  -- Expense records are authoritative. Journal copies must not be deducted again.
  select coalesce(sum(e.amount),0) into expenses from public.bripta_expenses e
    where e.business_id=actor.business_id and (branch is null or e.branch_id=branch)
      and e.status in ('approved','paid') and e.expense_date between p_start and p_end;
  select coalesce(jsonb_agg(to_jsonb(c) order by c.category),'[]') into categories from (
    select case when lower(e.category)='custom' then coalesce(nullif(trim(e.custom_category),''),'Custom') else e.category end as category,
      count(*) as expense_count,sum(e.amount) as amount
    from public.bripta_expenses e where e.business_id=actor.business_id and (branch is null or e.branch_id=branch)
      and e.status in ('approved','paid') and e.expense_date between p_start and p_end
    group by 1
  ) c;
  -- A balance sheet is cumulative through its as-of date, never just period movements.
  select coalesce(jsonb_agg(to_jsonb(a) order by a.account_type,a.account_code),'[]') into accounts from (
    select e.account_code,e.account_name,e.account_type,sum(e.debit) as debit,sum(e.credit) as credit,
      sum(e.debit-e.credit) as signed_balance,count(*) as entry_count
    from public.bripta_accounting_entries e where e.business_id=actor.business_id
      and (branch is null or e.branch_id=branch) and e.entry_date<=p_end
    group by e.account_code,e.account_name,e.account_type
  ) a;
  select count(*) into unbalanced from (
    select e.source_table,e.source_id from public.bripta_accounting_entries e
      where e.business_id=actor.business_id and (branch is null or e.branch_id=branch) and e.entry_date<=p_end
      group by e.source_table,e.source_id having abs(sum(e.debit-e.credit))>0.01
  ) x;
  select exists(select 1 from public.bripta_accounting_entries e where e.business_id=actor.business_id
    and (branch is null or e.branch_id=branch) and e.entry_date<=p_end
    and (e.source_table in ('opening_balance','opening_balances') or e.entry_key like 'opening%')) into opening;

  -- Coverage checks expose incomplete historical journals rather than manufacturing balances.
  select jsonb_build_object(
    'loans', (select count(*) from public.loans l where l.business_id=actor.business_id
      and (branch is null or l.branch_id=branch) and l.disbursement_date is not null and l.disbursement_date<=p_end
      and not exists(select 1 from public.bripta_accounting_entries e where e.business_id=actor.business_id
        and (branch is null or e.branch_id=branch) and e.entry_date<=p_end and e.source_table='loans' and e.source_id=l.id::text)),
    'repayments', (select count(*) from public.loan_repayments r where r.business_id=actor.business_id
      and (branch is null or r.branch_id=branch) and (r.payment_date at time zone 'Africa/Nairobi')::date<=p_end
      and not exists(select 1 from public.bripta_accounting_entries e where e.business_id=actor.business_id
        and (branch is null or e.branch_id=branch) and e.entry_date<=p_end and e.source_table='loan_repayments' and e.source_id=r.id::text)),
    'approved_expenses', (select count(*) from public.bripta_expenses x where x.business_id=actor.business_id
      and (branch is null or x.branch_id=branch) and x.status in ('approved','paid') and x.expense_date<=p_end
      and not exists(select 1 from public.bripta_accounting_entries e where e.business_id=actor.business_id
        and (branch is null or e.branch_id=branch) and e.entry_date<=p_end and e.source_table='bripta_expenses' and e.source_id=x.id::text)),
    'asset_purchases', (select count(*) from public.bripta_assets x where x.business_id=actor.business_id
      and (branch is null or x.branch_id=branch) and x.purchase_date<=p_end and x.initial_price>0
      and not exists(select 1 from public.bripta_accounting_entries e where e.business_id=actor.business_id
        and (branch is null or e.branch_id=branch) and e.entry_date<=p_end and e.source_table='bripta_assets' and e.source_id=x.id::text))
  ) into missing;
  return jsonb_build_object('period_start',p_start,'as_of',p_end,'branch_id',branch,
    'approved_expenses',expenses,'expense_categories',categories,'accounts',accounts,
    'opening_balances_identified',opening,'missing_source_postings',missing,'unbalanced_sources',unbalanced);
end $$;
revoke all on function public.bripta_financial_statements(date,date,uuid) from public,anon;
grant execute on function public.bripta_financial_statements(date,date,uuid) to authenticated;
notify pgrst,'reload schema';
commit;
select to_regprocedure('public.bripta_financial_statements(date,date,uuid)') is not null as financial_statements_installed;
