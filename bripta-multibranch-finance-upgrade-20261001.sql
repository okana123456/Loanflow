-- Bripta multi-branch, expenses, accounting, assets and permissions upgrade.
-- Idempotent: safe to run again. Existing financial source rows are not changed.

begin;

create extension if not exists pgcrypto;

create table if not exists public.bripta_branches (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  code text not null,
  name text not null,
  address text,
  phone text,
  email text,
  is_head_office boolean not null default false,
  is_active boolean not null default true,
  created_by uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (business_id, code),
  unique (business_id, name)
);

insert into public.bripta_branches (id,business_id,code,name,is_head_office,is_active)
values ('00000000-0000-4000-8000-000000000001','SYSTEM','HO','Head Office',true,true)
on conflict (id) do update set business_id='SYSTEM',name='Head Office',is_head_office=true,is_active=true;

-- Create a Head Office for every populated legacy business scope. Rows with a
-- blank business_id use the neutral SYSTEM scope instead of being rejected.
insert into public.bripta_branches(business_id,code,name,is_head_office,is_active)
select distinct s.business_id,'HO','Head Office',true,true
from public.loan_staff s where nullif(trim(s.business_id),'') is not null
on conflict(business_id,code) do update set is_head_office=true,is_active=true;

do $$
declare t text;
begin
  foreach t in array array[
    'loan_clients','loan_applications','loans','loan_schedules','loan_repayments',
    'loan_staff','loan_penalties','unmatched_payments',
    'journal_entries','bripta_charges','bripta_excess_ledger','bripta_excess_allocations'
  ] loop
    if to_regclass('public.'||t) is not null then
      execute format('alter table public.%I add column if not exists branch_id uuid references public.bripta_branches(id)',t);
    end if;
  end loop;
end $$;

-- The M-Pesa callback queue has business_short_code instead of business_id.
-- This Supabase project is Bripta-only, so its existing queue belongs to Head Office.
alter table public.mpesa_callback_queue add column if not exists branch_id uuid references public.bripta_branches(id);
update public.mpesa_callback_queue
set branch_id='00000000-0000-4000-8000-000000000001'
where branch_id is null;
create index if not exists mpesa_callback_queue_branch_idx on public.mpesa_callback_queue(branch_id);

-- Backfill without touching any financial amount or status.
do $$
declare t text;
begin
  foreach t in array array[
    'loan_clients','loan_applications','loans','loan_schedules','loan_repayments',
    'loan_staff','loan_penalties','unmatched_payments',
    'journal_entries','bripta_charges','bripta_excess_ledger','bripta_excess_allocations'
  ] loop
    if to_regclass('public.'||t) is not null then
      execute format($q$
        update public.%I x set branch_id=b.id
        from public.bripta_branches b
        where x.branch_id is null and b.business_id=coalesce(nullif(trim(x.business_id),''),'SYSTEM') and b.is_head_office
      $q$,t);
      execute format('create index if not exists %I on public.%I (business_id,branch_id)',t||'_branch_idx',t);
    end if;
  end loop;
end $$;

create table if not exists public.bripta_staff_permissions (
  staff_id uuid primary key references public.loan_staff(id) on delete cascade,
  business_id text not null,
  branch_id uuid references public.bripta_branches(id),
  view_accounting boolean not null default false,
  view_expenses boolean not null default false,
  initiate_expenses boolean not null default false,
  approve_expenses boolean not null default false,
  manage_assets boolean not null default false,
  manage_branches boolean not null default false,
  transfer_portfolios boolean not null default false,
  adjust_charges_excess boolean not null default false,
  updated_by uuid,
  updated_at timestamptz not null default now()
);

insert into public.bripta_staff_permissions (
  staff_id,business_id,branch_id,view_accounting,view_expenses,initiate_expenses,
  approve_expenses,manage_assets,manage_branches,transfer_portfolios,adjust_charges_excess
)
select s.id,coalesce(nullif(trim(s.business_id),''),'SYSTEM'),s.branch_id,
  position('admin' in coalesce(s.role,''))>0 or position('branch_manager' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0 or position('branch_manager' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0 or position('branch_manager' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0 or position('branch_manager' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0,
  position('admin' in coalesce(s.role,''))>0
from public.loan_staff s
on conflict (staff_id) do nothing;

create table if not exists public.bripta_expenses (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  branch_id uuid not null references public.bripta_branches(id),
  expense_no text not null,
  expense_date date not null default current_date,
  category text not null,
  custom_category text,
  description text not null,
  amount numeric(14,2) not null check(amount>0),
  payment_method text not null default 'cash',
  payment_reference text,
  vendor text,
  receipt_url text,
  status text not null default 'pending' check(status in ('draft','pending','approved','rejected','paid','cancelled')),
  initiated_by uuid not null references public.loan_staff(id),
  approved_by uuid references public.loan_staff(id),
  approved_at timestamptz,
  rejected_by uuid references public.loan_staff(id),
  rejected_at timestamptz,
  rejection_reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(business_id,expense_no)
);
create index if not exists bripta_expenses_branch_date_idx on public.bripta_expenses(business_id,branch_id,expense_date desc);

create table if not exists public.bripta_assets (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  branch_id uuid not null references public.bripta_branches(id),
  asset_no text not null,
  category text not null,
  custom_category text,
  description text not null,
  model_type text,
  serial_registration_no text,
  purchase_date date,
  initial_price numeric(14,2) not null default 0 check(initial_price>=0),
  current_value numeric(14,2) not null default 0 check(current_value>=0),
  custodian_staff_id uuid references public.loan_staff(id),
  condition text not null default 'good',
  status text not null default 'active' check(status in ('active','in_repair','disposed','lost','inactive')),
  notes text,
  disposed_at timestamptz,
  disposal_value numeric(14,2),
  created_by uuid not null references public.loan_staff(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(business_id,asset_no)
);
create index if not exists bripta_assets_branch_idx on public.bripta_assets(business_id,branch_id,status);

create table if not exists public.bripta_asset_movements (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references public.bripta_assets(id) on delete cascade,
  business_id text not null,
  branch_id uuid references public.bripta_branches(id),
  from_branch_id uuid references public.bripta_branches(id),
  to_branch_id uuid references public.bripta_branches(id),
  from_custodian_id uuid references public.loan_staff(id),
  to_custodian_id uuid references public.loan_staff(id),
  action text not null,
  notes text,
  performed_by uuid not null references public.loan_staff(id),
  created_at timestamptz not null default now()
);

create table if not exists public.bripta_accounting_entries (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  branch_id uuid not null references public.bripta_branches(id),
  entry_date date not null,
  account_code text not null,
  account_name text not null,
  account_type text not null check(account_type in ('asset','liability','equity','income','expense')),
  debit numeric(14,2) not null default 0 check(debit>=0),
  credit numeric(14,2) not null default 0 check(credit>=0),
  description text not null,
  source_table text not null,
  source_id text not null,
  entry_key text not null,
  loan_id uuid,
  expense_id uuid references public.bripta_expenses(id),
  asset_id uuid references public.bripta_assets(id),
  created_by uuid,
  created_at timestamptz not null default now(),
  unique(business_id,source_table,source_id,entry_key)
);
create index if not exists bripta_accounting_branch_date_idx on public.bripta_accounting_entries(business_id,branch_id,entry_date desc);

create table if not exists public.bripta_domain_audit (
  id uuid primary key default gen_random_uuid(), business_id text not null,
  branch_id uuid references public.bripta_branches(id), actor_staff_id uuid,
  action text not null, entity_type text not null, entity_id text,
  old_value jsonb, new_value jsonb, created_at timestamptz not null default now()
);

create or replace function public.bripta_current_staff()
returns public.loan_staff language sql stable security definer set search_path=public as $$
  select s from public.loan_staff s where s.auth_user_id=auth.uid() and coalesce(s.is_active,true) limit 1
$$;

create or replace function public.bripta_has_role(p_role text)
returns boolean language sql stable security definer set search_path=public as $$
  select exists(select 1 from public.loan_staff s where s.auth_user_id=auth.uid() and coalesce(s.is_active,true)
    and p_role=any(string_to_array(coalesce(s.role,''),',')))
$$;

create or replace function public.bripta_has_permission(p_permission text)
returns boolean language plpgsql stable security definer set search_path=public as $$
declare v boolean:=false;
begin
  if public.bripta_has_role('admin') then return true; end if;
  execute format('select coalesce(%I,false) from public.bripta_staff_permissions p join public.loan_staff s on s.id=p.staff_id where s.auth_user_id=$1',p_permission)
    into v using auth.uid();
  return coalesce(v,false);
end $$;

create or replace function public.bripta_can_access_branch(p_branch uuid)
returns boolean language sql stable security definer set search_path=public as $$
  select exists(select 1 from public.loan_staff s where s.auth_user_id=auth.uid() and coalesce(s.is_active,true)
    and (public.bripta_has_role('admin') or s.branch_id=p_branch))
$$;

-- Automatically inherit branch from the relevant parent or the acting staff.
create or replace function public.bripta_assign_branch()
returns trigger language plpgsql security definer set search_path=public as $$
declare v_branch uuid;
begin
  if new.branch_id is not null then return new; end if;
  if tg_table_name='mpesa_callback_queue' then
    select id into v_branch from public.bripta_branches where business_id='SYSTEM' and is_head_office limit 1;
    new.branch_id:=v_branch;
    return new;
  end if;
  if tg_table_name in ('loan_applications','loans') and new.client_id is not null then
    select branch_id into v_branch from public.loan_clients where id=new.client_id;
  elsif tg_table_name in ('loan_schedules','loan_repayments','loan_penalties') and new.loan_id is not null then
    select branch_id into v_branch from public.loans where id=new.loan_id;
  end if;
  if v_branch is null then select branch_id into v_branch from public.loan_staff where auth_user_id=auth.uid() limit 1; end if;
  if v_branch is null then select id into v_branch from public.bripta_branches where business_id=coalesce(nullif(trim(new.business_id),''),'SYSTEM') and is_head_office limit 1; end if;
  new.branch_id:=v_branch; return new;
end $$;

do $$ declare t text; begin
  foreach t in array array['loan_clients','loan_applications','loans','loan_schedules','loan_repayments','loan_penalties','unmatched_payments','journal_entries','bripta_charges','bripta_excess_ledger','bripta_excess_allocations','mpesa_callback_queue'] loop
    if to_regclass('public.'||t) is not null then
      execute format('drop trigger if exists bripta_assign_branch_trg on public.%I',t);
      execute format('create trigger bripta_assign_branch_trg before insert on public.%I for each row execute function public.bripta_assign_branch()',t);
    end if;
  end loop;
end $$;

create or replace function public.bripta_post_entry(
  p_business text,p_branch uuid,p_date date,p_code text,p_name text,p_type text,
  p_debit numeric,p_credit numeric,p_description text,p_source_table text,
  p_source_id text,p_key text,p_loan uuid default null,p_expense uuid default null,p_asset uuid default null,p_user uuid default null
) returns void language sql security definer set search_path=public as $$
  insert into public.bripta_accounting_entries(business_id,branch_id,entry_date,account_code,account_name,account_type,debit,credit,description,source_table,source_id,entry_key,loan_id,expense_id,asset_id,created_by)
  values(
    coalesce(nullif(trim(p_business),''),'SYSTEM'),p_branch,p_date,p_code,p_name,p_type,
    round(greatest(coalesce(p_debit,0),0)+greatest(-coalesce(p_credit,0),0),2),
    round(greatest(coalesce(p_credit,0),0)+greatest(-coalesce(p_debit,0),0),2),
    p_description,p_source_table,p_source_id,p_key,p_loan,p_expense,p_asset,p_user
  )
  on conflict(business_id,source_table,source_id,entry_key) do update set debit=excluded.debit,credit=excluded.credit,description=excluded.description,branch_id=excluded.branch_id,entry_date=excluded.entry_date
$$;

create or replace function public.bripta_sync_accounting()
returns jsonb language plpgsql security definer set search_path=public as $$
declare r record; v_count int:=0; v_business text;
begin
  if auth.uid() is not null and not (public.bripta_has_permission('view_accounting') or public.bripta_has_role('admin')) then
    raise exception 'Accounting access is not permitted';
  end if;
  if auth.uid() is not null then v_business:=(public.bripta_current_staff()).business_id; end if;
  -- Loans: disbursement (Dr Loan Receivable / Cr Cash or M-Pesa).
  for r in select * from public.loans where branch_id is not null and (v_business is null or business_id=v_business) loop
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.disbursement_date,'1100','Loan Receivable','asset',r.disbursed_amount,0,'Loan disbursed '||r.loan_no,'loans',r.id::text,'loan_receivable',r.id,null,null,r.loan_officer_id);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.disbursement_date,case when lower(coalesce(r.disbursement_method,'')) like '%mpesa%' then '1010' else '1000' end,case when lower(coalesce(r.disbursement_method,'')) like '%mpesa%' then 'M-Pesa' else 'Cash' end,'asset',0,r.disbursed_amount,'Loan disbursed '||r.loan_no,'loans',r.id::text,'disbursement_cash',r.id,null,null,r.loan_officer_id);
  end loop;
  -- Repayments: cash movement, principal recovery and earned revenue.
  for r in select p.*,l.branch_id,l.loan_no from public.loan_repayments p join public.loans l on l.id=p.loan_id where l.branch_id is not null and (v_business is null or p.business_id=v_business) loop
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,case when lower(coalesce(r.payment_method,'')) like '%mpesa%' then '1010' else '1000' end,case when lower(coalesce(r.payment_method,'')) like '%mpesa%' then 'M-Pesa' else 'Cash' end,'asset',r.amount,0,'Repayment '||coalesce(r.payment_reference,r.receipt_no),'loan_repayments',r.id::text,'cash_received',r.loan_id,null,null,r.collected_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,'1100','Loan Receivable','asset',0,coalesce(r.loan_portion,0)-coalesce(r.interest_portion,0),'Principal recovered '||r.loan_no,'loan_repayments',r.id::text,'principal_recovered',r.loan_id,null,null,r.collected_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,'4000','Interest Income','income',0,coalesce(r.interest_portion,0),'Interest received '||r.loan_no,'loan_repayments',r.id::text,'interest_income',r.loan_id,null,null,r.collected_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,'4010','Penalty Income','income',0,coalesce(r.penalty_portion,0),'Penalty received '||r.loan_no,'loan_repayments',r.id::text,'penalty_income',r.loan_id,null,null,r.collected_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,'4020','Processing Fee Income','income',0,coalesce(r.processing_fee_portion,0),'Processing fee '||r.loan_no,'loan_repayments',r.id::text,'processing_income',r.loan_id,null,null,r.collected_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,'4030','Registration Fee Income','income',0,coalesce(r.registration_fee_portion,0),'Registration fee '||r.loan_no,'loan_repayments',r.id::text,'registration_income',r.loan_id,null,null,r.collected_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.payment_date::date,'2100','Client Credit','liability',0,coalesce(r.credit_portion,0),'Client excess credit '||r.loan_no,'loan_repayments',r.id::text,'client_credit',r.loan_id,null,null,r.collected_by);
  end loop;
  -- Unmatched receipts remain a liability until allocated.
  for r in select * from public.unmatched_payments where branch_id is not null and not coalesce(resolved,false) and (v_business is null or business_id=v_business) loop
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.created_at::date,'1010','M-Pesa','asset',r.amount,0,'Unmatched receipt '||coalesce(r.mpesa_reference,r.invoice_id,r.id::text),'unmatched_payments',r.id::text,'suspense_cash',null,null,null,null);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.created_at::date,'2200','Suspense','liability',0,r.amount,'Unmatched receipt '||coalesce(r.mpesa_reference,r.invoice_id,r.id::text),'unmatched_payments',r.id::text,'suspense_liability',null,null,null,null);
  end loop;
  -- Separate Charges & Excess records.
  for r in select * from public.bripta_charges where branch_id is not null and (v_business is null or business_id=v_business) loop
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.charge_date,'1150','Client Charges Receivable','asset',r.amount,0,r.description,'bripta_charges',r.id::text,'charge_receivable',r.loan_id,null,null,r.created_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.charge_date,'4040','Charges Income','income',0,r.amount,r.description,'bripta_charges',r.id::text,'charge_income',r.loan_id,null,null,r.created_by);
  end loop;
  for r in select * from public.bripta_excess_ledger where branch_id is not null and (v_business is null or business_id=v_business) loop
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.created_at::date,'1010','M-Pesa','asset',r.amount_original,0,'Client excess '||coalesce(r.payment_reference,r.id::text),'bripta_excess_ledger',r.id::text,'excess_cash',r.loan_id,null,null,r.created_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.created_at::date,'2100','Client Credit','liability',0,r.amount_original,'Client excess '||coalesce(r.payment_reference,r.id::text),'bripta_excess_ledger',r.id::text,'excess_liability',r.loan_id,null,null,r.created_by);
  end loop;
  -- Company asset purchases.
  for r in select * from public.bripta_assets where branch_id is not null and purchase_date is not null and initial_price>0 and (v_business is null or business_id=v_business) loop
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.purchase_date,'1200','Company Assets','asset',r.initial_price,0,'Asset purchase '||r.description,'bripta_assets',r.id::text,'asset_acquired',null,null,r.id,r.created_by);
    perform public.bripta_post_entry(r.business_id,r.branch_id,r.purchase_date,'1000','Cash','asset',0,r.initial_price,'Asset purchase '||r.description,'bripta_assets',r.id::text,'asset_payment',null,null,r.id,r.created_by);
  end loop;
  select count(*) into v_count from public.bripta_accounting_entries;
  return jsonb_build_object('ok',true,'ledger_rows',v_count,'source_totals_changed',false);
end $$;

create or replace function public.bripta_submit_expense(p jsonb)
returns uuid language plpgsql security definer set search_path=public as $$
declare s public.loan_staff; v_id uuid:=coalesce((p->>'id')::uuid,gen_random_uuid()); v_branch uuid; oldrow jsonb;
begin
  select * into s from public.loan_staff where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1;
  if s.id is null or not (public.bripta_has_permission('initiate_expenses') or public.bripta_has_role('branch_manager')) then raise exception 'Expense initiation is not permitted'; end if;
  v_branch:=coalesce((p->>'branch_id')::uuid,s.branch_id);
  if not public.bripta_can_access_branch(v_branch) then raise exception 'Branch access denied'; end if;
  select to_jsonb(e) into oldrow from public.bripta_expenses e where e.id=v_id;
  insert into public.bripta_expenses(id,business_id,branch_id,expense_no,expense_date,category,custom_category,description,amount,payment_method,payment_reference,vendor,status,initiated_by)
  values(v_id,coalesce(nullif(trim(s.business_id),''),'SYSTEM'),v_branch,coalesce(p->>'expense_no','EXP-'||to_char(clock_timestamp(),'YYYYMMDDHH24MISSMS')),(p->>'expense_date')::date,p->>'category',p->>'custom_category',p->>'description',(p->>'amount')::numeric,coalesce(p->>'payment_method','cash'),p->>'payment_reference',p->>'vendor','pending',s.id)
  on conflict(id) do update set branch_id=excluded.branch_id,expense_date=excluded.expense_date,category=excluded.category,custom_category=excluded.custom_category,description=excluded.description,amount=excluded.amount,payment_method=excluded.payment_method,payment_reference=excluded.payment_reference,vendor=excluded.vendor,updated_at=now()
  where public.bripta_expenses.status in ('draft','pending');
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value) values(coalesce(nullif(trim(s.business_id),''),'SYSTEM'),v_branch,s.id,case when oldrow is null then 'expense_created' else 'expense_edited' end,'expense',v_id::text,oldrow,p);
  return v_id;
end $$;

create or replace function public.bripta_decide_expense(p_id uuid,p_action text,p_reason text default null)
returns jsonb language plpgsql security definer set search_path=public as $$
declare s public.loan_staff; e public.bripta_expenses;
begin
  select * into s from public.loan_staff where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1;
  select * into e from public.bripta_expenses where id=p_id for update;
  if e.id is null or not public.bripta_can_access_branch(e.branch_id) or not (public.bripta_has_permission('approve_expenses') or public.bripta_has_role('branch_manager')) then raise exception 'Expense approval is not permitted'; end if;
  if e.status not in ('pending','approved') then raise exception 'Expense is not pending'; end if;
  if p_action='approve' then
    update public.bripta_expenses set status='approved',approved_by=s.id,approved_at=now(),rejected_by=null,rejected_at=null,rejection_reason=null,updated_at=now() where id=p_id;
    perform public.bripta_post_entry(e.business_id,e.branch_id,e.expense_date,'5000','Operating Expenses','expense',e.amount,0,e.description,'bripta_expenses',e.id::text,'expense_debit',null,e.id,null,s.id);
    perform public.bripta_post_entry(e.business_id,e.branch_id,e.expense_date,case when lower(e.payment_method) like '%mpesa%' then '1010' else '1000' end,case when lower(e.payment_method) like '%mpesa%' then 'M-Pesa' else 'Cash' end,'asset',0,e.amount,e.description,'bripta_expenses',e.id::text,'expense_payment',null,e.id,null,s.id);
  elsif p_action='reject' then
    update public.bripta_expenses set status='rejected',rejected_by=s.id,rejected_at=now(),rejection_reason=p_reason,updated_at=now() where id=p_id;
    delete from public.bripta_accounting_entries where expense_id=p_id;
  else raise exception 'Use approve or reject'; end if;
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,new_value) values(e.business_id,e.branch_id,s.id,'expense_'||p_action,'expense',e.id::text,jsonb_build_object('reason',p_reason));
  return jsonb_build_object('ok',true,'status',case when p_action='approve' then 'approved' else 'rejected' end);
end $$;

create or replace function public.bripta_delete_expense(p_id uuid)
returns boolean language plpgsql security definer set search_path=public as $$
declare s public.loan_staff; e public.bripta_expenses;
begin
  select * into s from public.loan_staff where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1;
  select * into e from public.bripta_expenses where id=p_id for update;
  if e.id is null then return false; end if;
  if e.status not in ('draft','pending','rejected') or not public.bripta_can_access_branch(e.branch_id) or not (e.initiated_by=s.id or public.bripta_has_role('admin')) then raise exception 'Expense cannot be deleted'; end if;
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value) values(e.business_id,e.branch_id,s.id,'expense_deleted','expense',e.id::text,to_jsonb(e));
  delete from public.bripta_expenses where id=p_id; return true;
end $$;

create or replace function public.bripta_suspense_to_equity(p_source_id text,p_amount numeric,p_description text,p_branch uuid)
returns jsonb language plpgsql security definer set search_path=public as $$
declare s public.loan_staff; v_key text;
begin
  select * into s from public.loan_staff where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1;
  if s.id is null or not public.bripta_has_permission('view_accounting') or not public.bripta_can_access_branch(p_branch) then raise exception 'Accounting transfer is not permitted'; end if;
  if p_amount<=0 then raise exception 'Amount must be positive'; end if;
  v_key:='suspense_equity_'||p_source_id;
  perform public.bripta_post_entry(coalesce(nullif(trim(s.business_id),''),'SYSTEM'),p_branch,current_date,'2200','Suspense','liability',p_amount,0,p_description,'suspense_equity',p_source_id,v_key||'_debit',null,null,null,s.id);
  perform public.bripta_post_entry(coalesce(nullif(trim(s.business_id),''),'SYSTEM'),p_branch,current_date,'3000','Owner Capital / Equity','equity',0,p_amount,p_description,'suspense_equity',p_source_id,v_key||'_credit',null,null,null,s.id);
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,new_value) values(coalesce(nullif(trim(s.business_id),''),'SYSTEM'),p_branch,s.id,'suspense_transferred_to_equity','accounting_transfer',p_source_id,jsonb_build_object('amount',p_amount,'description',p_description));
  return jsonb_build_object('ok',true);
end $$;

create or replace function public.bripta_transfer_portfolio(p_from uuid,p_to uuid,p_client_ids uuid[] default null,p_move_all boolean default false)
returns jsonb language plpgsql security definer set search_path=public as $$
declare s public.loan_staff; target public.loan_staff; n int:=0;
begin
  select * into s from public.loan_staff where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1;
  if s.id is null or not public.bripta_has_permission('transfer_portfolios') then raise exception 'Portfolio transfer is not permitted'; end if;
  select * into target from public.loan_staff where id=p_to and coalesce(is_active,true);
  if target.id is null then raise exception 'Target officer not found'; end if;
  with moved as (
    update public.loan_clients set loan_officer_id=p_to,branch_id=coalesce(target.branch_id,branch_id),updated_at=now()
    where loan_officer_id=p_from and public.bripta_can_access_branch(branch_id) and (p_move_all or id=any(coalesce(p_client_ids,array[]::uuid[]))) returning id
  ) select count(*) into n from moved;
  update public.loan_applications a set loan_officer_id=p_to,branch_id=coalesce(target.branch_id,a.branch_id),updated_at=now() where a.client_id in(select id from public.loan_clients where loan_officer_id=p_to and public.bripta_can_access_branch(branch_id));
  update public.loans l set loan_officer_id=p_to,branch_id=coalesce(target.branch_id,l.branch_id),updated_at=now() where l.client_id in(select id from public.loan_clients where loan_officer_id=p_to and public.bripta_can_access_branch(branch_id));
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,new_value) values(coalesce(nullif(trim(s.business_id),''),'SYSTEM'),target.branch_id,s.id,'portfolio_transferred','loan_clients',jsonb_build_object('from',p_from,'to',p_to,'clients',n));
  return jsonb_build_object('ok',true,'clients_moved',n);
end $$;

-- Billing amount is authoritative on the server by billing month.
create or replace function public.bripta_subscription_amount(p_billing_month date)
returns numeric language sql immutable as $$
  select case when date_trunc('month',p_billing_month)::date >= date '2026-11-01' then 7500::numeric else 3000::numeric end
$$;

-- Prevent browser code or an older Edge Function from creating a future billing
-- request with the wrong price. Paid rows retain the amount actually received.
create or replace function public.bripta_enforce_subscription_amount()
returns trigger language plpgsql set search_path=public as $$
declare v_month date;
begin
  v_month:=coalesce(new.billing_month,current_date);
  if coalesce(new.status,'pending') not in ('paid','completed') then
    new.amount:=public.bripta_subscription_amount(v_month);
  elsif coalesce(new.amount,0)<public.bripta_subscription_amount(v_month) then
    raise exception 'Subscription payment is below the required amount for %',v_month;
  end if;
  return new;
end $$;

do $$ begin
  if to_regclass('public.loan_billing_cycles') is not null then
    drop trigger if exists bripta_subscription_amount_trg on public.loan_billing_cycles;
    create trigger bripta_subscription_amount_trg before insert or update of amount,status,billing_month on public.loan_billing_cycles for each row execute function public.bripta_enforce_subscription_amount();
  end if;
end $$;

-- RLS for new modules. Service-role Edge Functions bypass these policies.
alter table public.bripta_branches enable row level security;
alter table public.bripta_staff_permissions enable row level security;
alter table public.bripta_expenses enable row level security;
alter table public.bripta_assets enable row level security;
alter table public.bripta_asset_movements enable row level security;
alter table public.bripta_accounting_entries enable row level security;
alter table public.bripta_domain_audit enable row level security;

-- Restrictive policies are combined with any older permissive policies, so a
-- legacy policy cannot expose another branch. Service-role callbacks still bypass RLS.
do $$ declare t text; begin
  foreach t in array array['loan_clients','loan_applications','loans','loan_schedules','loan_repayments','loan_penalties','unmatched_payments','journal_entries','bripta_charges','bripta_excess_ledger','bripta_excess_allocations','mpesa_callback_queue'] loop
    if to_regclass('public.'||t) is not null then
      execute format('alter table public.%I enable row level security',t);
      execute format('drop policy if exists bripta_branch_boundary on public.%I',t);
      execute format('create policy bripta_branch_boundary on public.%I as restrictive for all to authenticated using (public.bripta_can_access_branch(branch_id)) with check (public.bripta_can_access_branch(branch_id))',t);
    end if;
  end loop;
end $$;

drop policy if exists bripta_officer_clients on public.loan_clients;
create policy bripta_officer_clients on public.loan_clients as restrictive for all to authenticated
using (not public.bripta_has_role('loan_officer') or public.bripta_has_role('admin') or public.bripta_has_role('branch_manager') or loan_officer_id=(public.bripta_current_staff()).id)
with check (not public.bripta_has_role('loan_officer') or public.bripta_has_role('admin') or public.bripta_has_role('branch_manager') or loan_officer_id=(public.bripta_current_staff()).id);
drop policy if exists bripta_officer_applications on public.loan_applications;
create policy bripta_officer_applications on public.loan_applications as restrictive for all to authenticated
using (not public.bripta_has_role('loan_officer') or public.bripta_has_role('admin') or public.bripta_has_role('branch_manager') or loan_officer_id=(public.bripta_current_staff()).id)
with check (not public.bripta_has_role('loan_officer') or public.bripta_has_role('admin') or public.bripta_has_role('branch_manager') or loan_officer_id=(public.bripta_current_staff()).id);
drop policy if exists bripta_officer_loans on public.loans;
create policy bripta_officer_loans on public.loans as restrictive for all to authenticated
using (not public.bripta_has_role('loan_officer') or public.bripta_has_role('admin') or public.bripta_has_role('branch_manager') or loan_officer_id=(public.bripta_current_staff()).id)
with check (not public.bripta_has_role('loan_officer') or public.bripta_has_role('admin') or public.bripta_has_role('branch_manager') or loan_officer_id=(public.bripta_current_staff()).id);

alter table public.loan_staff enable row level security;
drop policy if exists bripta_staff_branch_boundary on public.loan_staff;
create policy bripta_staff_branch_boundary on public.loan_staff as restrictive for all to authenticated
using (
  public.bripta_has_role('admin') or id=(public.bripta_current_staff()).id
  or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
)
with check (
  public.bripta_has_role('admin') or id=(public.bripta_current_staff()).id
  or (public.bripta_has_role('branch_manager') and branch_id=(public.bripta_current_staff()).branch_id)
);

create or replace function public.bripta_audit_master_change()
returns trigger language plpgsql security definer set search_path=public as $$
declare payload jsonb; actor public.loan_staff; v_business text; v_branch uuid; v_id text;
begin
  payload:=case when tg_op='DELETE' then to_jsonb(old) else to_jsonb(new) end;
  select * into actor from public.loan_staff where auth_user_id=auth.uid() limit 1;
  v_business:=coalesce(nullif(trim(payload->>'business_id'),''),nullif(trim(actor.business_id),''),'SYSTEM');
  v_branch:=coalesce((payload->>'branch_id')::uuid,actor.branch_id);
  v_id:=coalesce(payload->>'id',payload->>'staff_id','unknown');
  insert into public.bripta_domain_audit(business_id,branch_id,actor_staff_id,action,entity_type,entity_id,old_value,new_value)
  values(v_business,v_branch,actor.id,lower(tg_table_name)||'_'||lower(tg_op),tg_table_name,v_id,case when tg_op in('UPDATE','DELETE') then to_jsonb(old) end,case when tg_op in('INSERT','UPDATE') then to_jsonb(new) end);
  return case when tg_op='DELETE' then old else new end;
end $$;

do $$ declare t text; begin
  foreach t in array array['bripta_branches','bripta_staff_permissions','bripta_assets','bripta_asset_movements'] loop
    execute format('drop trigger if exists bripta_master_audit_trg on public.%I',t);
    execute format('create trigger bripta_master_audit_trg after insert or update or delete on public.%I for each row execute function public.bripta_audit_master_change()',t);
  end loop;
end $$;

do $$ declare t text; begin
  foreach t in array array['bripta_staff_permissions','bripta_expenses','bripta_assets','bripta_asset_movements','bripta_accounting_entries','bripta_domain_audit'] loop
    execute format('drop policy if exists %I on public.%I',t||'_read',t);
    execute format('create policy %I on public.%I for select to authenticated using (coalesce(nullif(trim(business_id),''''),''SYSTEM'')=coalesce(nullif(trim((select business_id from public.loan_staff where auth_user_id=auth.uid() and coalesce(is_active,true) limit 1)),''''),''SYSTEM'') and (public.bripta_has_role(''admin'') or branch_id is null or public.bripta_can_access_branch(branch_id)))',t||'_read',t);
  end loop;
end $$;

drop policy if exists bripta_branches_read on public.bripta_branches;
create policy bripta_branches_read on public.bripta_branches for select to authenticated using (
  (public.bripta_has_role('admin') or id=(public.bripta_current_staff()).branch_id)
);

drop policy if exists bripta_branches_admin_write on public.bripta_branches;
create policy bripta_branches_admin_write on public.bripta_branches for all to authenticated using(public.bripta_has_permission('manage_branches')) with check(public.bripta_has_permission('manage_branches'));
drop policy if exists bripta_permissions_admin_write on public.bripta_staff_permissions;
create policy bripta_permissions_admin_write on public.bripta_staff_permissions for all to authenticated using(public.bripta_has_role('admin')) with check(public.bripta_has_role('admin'));
drop policy if exists bripta_assets_manage on public.bripta_assets;
create policy bripta_assets_manage on public.bripta_assets for all to authenticated using(public.bripta_has_permission('manage_assets') and public.bripta_can_access_branch(branch_id)) with check(public.bripta_has_permission('manage_assets') and public.bripta_can_access_branch(branch_id));

-- Safety boundary: legacy loans, repayments, reversals and balances are never
-- imported or recalculated by this structural upgrade. Keeping this RPC as a
-- no-op also makes older cached frontends safe if they still call it.
create or replace function public.bripta_sync_accounting()
returns jsonb language sql security definer set search_path=public as $$
  select jsonb_build_object('ok',true,'skipped',true,'reason','Historical accounting synchronization is disabled to preserve existing figures')
$$;

revoke all on function public.bripta_sync_accounting() from public,anon;
grant execute on function public.bripta_sync_accounting(),public.bripta_submit_expense(jsonb),public.bripta_decide_expense(uuid,text,text),public.bripta_delete_expense(uuid),public.bripta_suspense_to_equity(text,numeric,text,uuid),public.bripta_transfer_portfolio(uuid,uuid,uuid[],boolean),public.bripta_subscription_amount(date) to authenticated;
grant select on public.bripta_branches,public.bripta_staff_permissions,public.bripta_expenses,public.bripta_assets,public.bripta_asset_movements,public.bripta_accounting_entries,public.bripta_domain_audit to authenticated;
grant insert,update on public.bripta_branches,public.bripta_staff_permissions,public.bripta_assets,public.bripta_asset_movements to authenticated;

-- No historical accounting synchronization is run by this migration.

commit;

-- Verification result set.
select 'head_office_branches' check_name,count(*)::numeric result from public.bripta_branches where is_head_office
union all select 'clients_without_branch',count(*) from public.loan_clients where branch_id is null
union all select 'loans_without_branch',count(*) from public.loans where branch_id is null
union all select 'repayments_without_branch',count(*) from public.loan_repayments where branch_id is null
union all select 'staff_without_branch',count(*) from public.loan_staff where branch_id is null
union all select 'ledger_unbalanced_sources',count(*) from (select source_table,source_id,sum(debit) d,sum(credit) c from public.bripta_accounting_entries group by source_table,source_id having abs(sum(debit)-sum(credit))>0.01)x
union all select 'october_subscription_amount',public.bripta_subscription_amount('2026-10-01')
union all select 'november_subscription_amount',public.bripta_subscription_amount('2026-11-01');
