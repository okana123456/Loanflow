-- Bripta future-only Excess & Charges setup
-- Safe scope: creates empty ledgers and does NOT read, convert or alter historical repayments.

begin;

create extension if not exists pgcrypto;

create table if not exists public.bripta_charges (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  client_id uuid not null references public.loan_clients(id) on delete restrict,
  loan_id uuid references public.loans(id) on delete set null,
  charge_type text not null check (charge_type in ('processing_fee','registration_fee','penalty','other')),
  description text not null,
  amount numeric(14,2) not null check (amount > 0),
  amount_paid numeric(14,2) not null default 0 check (amount_paid >= 0),
  status text not null default 'pending' check (status in ('pending','partially_paid','paid','waived')),
  source text not null default 'system' check (source in ('system','manual')),
  charge_date date not null default current_date,
  created_by uuid references public.loan_staff(id) on delete set null,
  settled_at timestamptz,
  settled_by uuid references public.loan_staff(id) on delete set null,
  settlement_note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (amount_paid <= amount)
);

create unique index if not exists bripta_charges_system_loan_type_uidx
  on public.bripta_charges(loan_id, charge_type)
  where loan_id is not null and source = 'system';
create index if not exists bripta_charges_business_status_idx
  on public.bripta_charges(business_id, status, charge_date desc);
create index if not exists bripta_charges_client_idx
  on public.bripta_charges(client_id, created_at desc);

create table if not exists public.bripta_excess_ledger (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  client_id uuid not null references public.loan_clients(id) on delete restrict,
  loan_id uuid references public.loans(id) on delete set null,
  repayment_id uuid references public.loan_repayments(id) on delete set null,
  amount_original numeric(14,2) not null check (amount_original > 0),
  amount_available numeric(14,2) not null check (amount_available >= 0),
  status text not null default 'available' check (status in ('available','partially_used','applied','refunded','carried_forward')),
  source text not null check (source in ('manual_payment','mpesa_callback','suspense_match')),
  payment_reference text,
  notes text,
  created_by uuid references public.loan_staff(id) on delete set null,
  resolved_at timestamptz,
  resolved_by uuid references public.loan_staff(id) on delete set null,
  resolution_note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (amount_available <= amount_original)
);

create unique index if not exists bripta_excess_repayment_uidx
  on public.bripta_excess_ledger(repayment_id)
  where repayment_id is not null;
create index if not exists bripta_excess_business_status_idx
  on public.bripta_excess_ledger(business_id, status, created_at desc);
create index if not exists bripta_excess_client_idx
  on public.bripta_excess_ledger(client_id, created_at desc);

create table if not exists public.bripta_excess_allocations (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  excess_id uuid not null references public.bripta_excess_ledger(id) on delete restrict,
  charge_id uuid references public.bripta_charges(id) on delete set null,
  action text not null check (action in ('charge_payment','loan_payment','refund','carry_forward')),
  amount numeric(14,2) not null check (amount > 0),
  note text,
  created_by uuid references public.loan_staff(id) on delete set null,
  created_at timestamptz not null default now()
);

create index if not exists bripta_excess_allocations_business_idx
  on public.bripta_excess_allocations(business_id, created_at desc);

-- Future repayments are protected at database level. Only the amount needed
-- to clear the current loan can become loan_portion; the remainder is excess.
create or replace function public.bripta_prepare_future_repayment()
returns trigger language plpgsql set search_path = public
as $$
declare v_balance numeric; v_total numeric; v_interest numeric; v_ratio numeric;
declare v_available numeric; v_loan_portion numeric;
begin
  select greatest(0,coalesce(l.outstanding_balance,0)),coalesce(l.total_payable,0),coalesce(l.total_interest,0)
    into v_balance,v_total,v_interest from public.loans l where l.id=new.loan_id;
  if not found then return new; end if;
  v_available := greatest(0,coalesce(new.amount,0)-coalesce(new.penalty_portion,0));
  v_loan_portion := round(least(v_available,v_balance),2);
  new.loan_portion := v_loan_portion;
  new.credit_portion := round(greatest(0,v_available-v_loan_portion),2);
  new.registration_fee_portion := 0;
  new.processing_fee_portion := 0;
  v_ratio := case when v_total>0 and v_interest>0 then v_interest/v_total else 0 end;
  new.interest_portion := round(v_loan_portion*v_ratio,2);
  new.principal_portion := round(v_loan_portion-new.interest_portion,2);
  return new;
end $$;

create or replace function public.bripta_capture_future_excess()
returns trigger language plpgsql security definer set search_path = public
as $$
declare v_loan public.loans%rowtype; v_source text;
begin
  if coalesce(new.credit_portion,0)<=0 then return new; end if;
  select * into v_loan from public.loans where id=new.loan_id;
  v_source := case
    when coalesce(new.notes,'') ilike '%matched from suspense%' then 'suspense_match'
    when coalesce(new.payment_method,'') ilike '%mpesa%' and coalesce(new.mpesa_confirmed,false) then 'mpesa_callback'
    else 'manual_payment' end;
  insert into public.bripta_excess_ledger(
    business_id,client_id,loan_id,repayment_id,amount_original,amount_available,
    source,payment_reference,created_by,notes
  ) values (
    new.business_id,v_loan.client_id,new.loan_id,new.id,new.credit_portion,new.credit_portion,
    v_source,new.payment_reference,new.collected_by,'Future overpayment captured automatically after the loan reached zero.'
  ) on conflict (repayment_id) where repayment_id is not null do nothing;
  return new;
end $$;

drop trigger if exists bripta_prepare_future_repayment_trigger on public.loan_repayments;
create trigger bripta_prepare_future_repayment_trigger before insert on public.loan_repayments
for each row execute function public.bripta_prepare_future_repayment();

drop trigger if exists bripta_capture_future_excess_trigger on public.loan_repayments;
create trigger bripta_capture_future_excess_trigger after insert on public.loan_repayments
for each row execute function public.bripta_capture_future_excess();

alter table public.bripta_charges enable row level security;
alter table public.bripta_excess_ledger enable row level security;
alter table public.bripta_excess_allocations enable row level security;

drop policy if exists bripta_charges_read on public.bripta_charges;
create policy bripta_charges_read on public.bripta_charges for select to authenticated
using (exists (
  select 1 from public.loan_staff s
  where s.auth_user_id = auth.uid() and s.business_id = bripta_charges.business_id
    and s.is_active = true and (s.role ilike '%admin%' or s.role ilike '%branch_manager%')
));

drop policy if exists bripta_charges_insert on public.bripta_charges;
create policy bripta_charges_insert on public.bripta_charges for insert to authenticated
with check (exists (
  select 1 from public.loan_staff s
  where s.auth_user_id = auth.uid() and s.business_id = bripta_charges.business_id and s.is_active = true
));

drop policy if exists bripta_excess_read on public.bripta_excess_ledger;
create policy bripta_excess_read on public.bripta_excess_ledger for select to authenticated
using (exists (
  select 1 from public.loan_staff s
  where s.auth_user_id = auth.uid() and s.business_id = bripta_excess_ledger.business_id
    and s.is_active = true and (s.role ilike '%admin%' or s.role ilike '%branch_manager%')
));

drop policy if exists bripta_excess_allocations_read on public.bripta_excess_allocations;
create policy bripta_excess_allocations_read on public.bripta_excess_allocations for select to authenticated
using (exists (
  select 1 from public.loan_staff s
  where s.auth_user_id = auth.uid() and s.business_id = bripta_excess_allocations.business_id
    and s.is_active = true and (s.role ilike '%admin%' or s.role ilike '%branch_manager%')
));

create or replace function public.bripta_record_future_excess(
  p_business_id text,
  p_client_id uuid,
  p_loan_id uuid,
  p_repayment_id uuid,
  p_amount numeric,
  p_source text,
  p_reference text default null,
  p_created_by uuid default null
) returns uuid
language plpgsql security definer set search_path = public
as $$
declare v_id uuid;
begin
  if coalesce(p_amount,0) <= 0 then return null; end if;
  if p_source not in ('manual_payment','mpesa_callback','suspense_match') then
    raise exception 'Invalid excess source';
  end if;
  if auth.role() <> 'service_role' and not exists (
    select 1 from public.loan_staff s
    where s.auth_user_id = auth.uid() and s.business_id = p_business_id and s.is_active = true
  ) then raise exception 'Not authorized for this business'; end if;
  if not exists (
    select 1 from public.loans l where l.id=p_loan_id and l.client_id=p_client_id and l.business_id=p_business_id
  ) then raise exception 'Loan and client do not belong to this business'; end if;

  insert into public.bripta_excess_ledger(
    business_id,client_id,loan_id,repayment_id,amount_original,amount_available,
    source,payment_reference,created_by,notes
  ) values (
    p_business_id,p_client_id,p_loan_id,p_repayment_id,round(p_amount,2),round(p_amount,2),
    p_source,p_reference,p_created_by,'Future overpayment captured after loan balance reached zero.'
  )
  on conflict (repayment_id) where repayment_id is not null do update
    set payment_reference=excluded.payment_reference
  returning id into v_id;
  return v_id;
end $$;

create or replace function public.bripta_apply_excess_to_charge(
  p_excess_id uuid, p_charge_id uuid, p_amount numeric, p_note text default null
) returns jsonb
language plpgsql security definer set search_path = public
as $$
declare v_excess public.bripta_excess_ledger%rowtype; v_charge public.bripta_charges%rowtype;
declare v_staff uuid; v_apply numeric; v_excess_left numeric; v_charge_paid numeric;
begin
  select * into v_excess from public.bripta_excess_ledger where id=p_excess_id for update;
  select * into v_charge from public.bripta_charges where id=p_charge_id for update;
  if v_excess.id is null or v_charge.id is null then raise exception 'Excess or charge was not found'; end if;
  if v_excess.business_id <> v_charge.business_id or v_excess.client_id <> v_charge.client_id then
    raise exception 'Excess and charge must belong to the same client';
  end if;
  select s.id into v_staff from public.loan_staff s
  where s.auth_user_id=auth.uid() and s.business_id=v_excess.business_id and s.is_active=true
    and (s.role ilike '%admin%' or s.role ilike '%branch_manager%') limit 1;
  if v_staff is null then raise exception 'Only an administrator can allocate excess'; end if;
  v_apply := least(round(coalesce(p_amount,0),2), v_excess.amount_available, v_charge.amount-v_charge.amount_paid);
  if v_apply <= 0 then raise exception 'Enter an amount within the available excess and charge balance'; end if;
  v_excess_left := round(v_excess.amount_available-v_apply,2);
  v_charge_paid := round(v_charge.amount_paid+v_apply,2);

  update public.bripta_excess_ledger set amount_available=v_excess_left,
    status=case when v_excess_left<=0 then 'applied' else 'partially_used' end,
    resolved_at=case when v_excess_left<=0 then now() else null end,
    resolved_by=case when v_excess_left<=0 then v_staff else null end,
    resolution_note=coalesce(p_note,'Applied to charge'),updated_at=now() where id=p_excess_id;
  update public.bripta_charges set amount_paid=v_charge_paid,
    status=case when v_charge_paid>=amount then 'paid' else 'partially_paid' end,
    settled_at=case when v_charge_paid>=amount then now() else null end,
    settled_by=case when v_charge_paid>=amount then v_staff else null end,
    settlement_note=coalesce(p_note,'Paid from client excess'),updated_at=now() where id=p_charge_id;
  insert into public.bripta_excess_allocations(business_id,excess_id,charge_id,action,amount,note,created_by)
    values(v_excess.business_id,p_excess_id,p_charge_id,'charge_payment',v_apply,p_note,v_staff);
  return jsonb_build_object('ok',true,'amount_applied',v_apply,'excess_remaining',v_excess_left,'charge_paid',v_charge_paid);
end $$;

create or replace function public.bripta_resolve_excess(
  p_excess_id uuid, p_action text, p_amount numeric, p_note text default null
) returns jsonb
language plpgsql security definer set search_path = public
as $$
declare v_excess public.bripta_excess_ledger%rowtype; v_staff uuid; v_use numeric; v_left numeric;
begin
  if p_action not in ('refund','carry_forward') then raise exception 'Invalid excess action'; end if;
  select * into v_excess from public.bripta_excess_ledger where id=p_excess_id for update;
  if v_excess.id is null then raise exception 'Excess record was not found'; end if;
  select s.id into v_staff from public.loan_staff s
  where s.auth_user_id=auth.uid() and s.business_id=v_excess.business_id and s.is_active=true
    and (s.role ilike '%admin%' or s.role ilike '%branch_manager%') limit 1;
  if v_staff is null then raise exception 'Only an administrator can resolve excess'; end if;
  v_use := least(round(coalesce(p_amount,0),2),v_excess.amount_available);
  if v_use<=0 then raise exception 'Enter a valid amount'; end if;
  v_left := round(v_excess.amount_available-v_use,2);
  update public.bripta_excess_ledger set amount_available=v_left,
    status=case when v_left>0 then 'partially_used' when p_action='refund' then 'refunded' else 'carried_forward' end,
    resolved_at=case when v_left<=0 then now() else null end,
    resolved_by=case when v_left<=0 then v_staff else null end,
    resolution_note=p_note,updated_at=now() where id=p_excess_id;
  insert into public.bripta_excess_allocations(business_id,excess_id,action,amount,note,created_by)
    values(v_excess.business_id,p_excess_id,p_action,v_use,p_note,v_staff);
  return jsonb_build_object('ok',true,'amount_resolved',v_use,'remaining',v_left,'action',p_action);
end $$;

create or replace function public.bripta_settle_charge(
  p_charge_id uuid, p_action text, p_note text default null
) returns jsonb
language plpgsql security definer set search_path = public
as $$
declare v_charge public.bripta_charges%rowtype; v_staff uuid;
begin
  if p_action not in ('paid','waived') then raise exception 'Invalid charge action'; end if;
  select * into v_charge from public.bripta_charges where id=p_charge_id for update;
  if v_charge.id is null then raise exception 'Charge was not found'; end if;
  select s.id into v_staff from public.loan_staff s
  where s.auth_user_id=auth.uid() and s.business_id=v_charge.business_id and s.is_active=true
    and (s.role ilike '%admin%' or s.role ilike '%branch_manager%') limit 1;
  if v_staff is null then raise exception 'Only an administrator can settle charges'; end if;
  update public.bripta_charges set amount_paid=case when p_action='paid' then amount else amount_paid end,
    status=p_action,settled_at=now(),settled_by=v_staff,settlement_note=p_note,updated_at=now()
  where id=p_charge_id;
  return jsonb_build_object('ok',true,'status',p_action);
end $$;

create or replace function public.bripta_apply_excess_to_loan(
  p_excess_id uuid, p_loan_id uuid, p_amount numeric, p_note text default null
) returns jsonb
language plpgsql security definer set search_path = public
as $$
declare v_excess public.bripta_excess_ledger%rowtype; v_loan public.loans%rowtype;
declare v_staff uuid; v_apply numeric; v_left numeric; v_balance numeric; v_repayment uuid;
declare v_remaining numeric; v_sched record; v_sched_apply numeric; v_total_paid numeric;
begin
  select * into v_excess from public.bripta_excess_ledger where id=p_excess_id for update;
  select * into v_loan from public.loans where id=p_loan_id for update;
  if v_excess.id is null or v_loan.id is null then raise exception 'Excess or loan was not found'; end if;
  if v_excess.business_id<>v_loan.business_id or v_excess.client_id<>v_loan.client_id then
    raise exception 'Excess and loan must belong to the same client';
  end if;
  select s.id into v_staff from public.loan_staff s
  where s.auth_user_id=auth.uid() and s.business_id=v_excess.business_id and s.is_active=true
    and (s.role ilike '%admin%' or s.role ilike '%branch_manager%') limit 1;
  if v_staff is null then raise exception 'Only an administrator can apply excess'; end if;
  v_balance:=greatest(0,coalesce(v_loan.outstanding_balance,0));
  v_apply:=least(round(coalesce(p_amount,0),2),v_excess.amount_available,v_balance);
  if v_apply<=0 then raise exception 'Enter an amount within the available excess and loan balance'; end if;

  insert into public.loan_repayments(
    loan_id,receipt_no,amount,payment_method,payment_reference,payment_date,
    principal_portion,interest_portion,penalty_portion,registration_fee_portion,
    processing_fee_portion,loan_portion,credit_portion,mpesa_confirmed,collected_by,business_id,notes
  ) values (
    v_loan.id,'EXC-'||upper(substr(replace(gen_random_uuid()::text,'-',''),1,10)),v_apply,'client_excess',
    'EXCESS-'||substr(v_excess.id::text,1,8),now(),v_apply,0,0,0,0,v_apply,0,false,v_staff,v_loan.business_id,
    'Admin applied available client excess to a future loan. '||coalesce(p_note,'')
  ) returning id into v_repayment;

  v_remaining:=v_apply;
  for v_sched in select * from public.loan_schedules where loan_id=v_loan.id
    and status in ('pending','partial','overdue') order by due_date,installment_no for update
  loop
    exit when v_remaining<=0;
    v_sched_apply:=least(v_remaining,greatest(0,coalesce(v_sched.total_due,0)-coalesce(v_sched.total_paid,0)));
    if v_sched_apply>0 then
      v_total_paid:=round(coalesce(v_sched.total_paid,0)+v_sched_apply,2);
      update public.loan_schedules set total_paid=v_total_paid,
        status=case when v_total_paid>=coalesce(total_due,0)-0.01 then 'paid' else 'partial' end,
        paid_at=case when v_total_paid>=coalesce(total_due,0)-0.01 then now() else paid_at end
      where id=v_sched.id;
      v_remaining:=round(v_remaining-v_sched_apply,2);
    end if;
  end loop;

  v_balance:=round(greatest(0,v_balance-v_apply),2);
  update public.loans set total_paid=round(coalesce(total_paid,0)+v_apply,2),outstanding_balance=v_balance,
    status=case when v_balance<=0 then 'completed' else status end,
    arrears_amount=case when v_balance<=0 then 0 else least(coalesce(arrears_amount,0),v_balance) end,
    overdue_days=case when v_balance<=0 then 0 else overdue_days end where id=v_loan.id;

  v_left:=round(v_excess.amount_available-v_apply,2);
  update public.bripta_excess_ledger set amount_available=v_left,
    status=case when v_left<=0 then 'applied' else 'partially_used' end,
    resolved_at=case when v_left<=0 then now() else null end,
    resolved_by=case when v_left<=0 then v_staff else null end,
    resolution_note=coalesce(p_note,'Applied to future loan'),updated_at=now() where id=p_excess_id;
  insert into public.bripta_excess_allocations(business_id,excess_id,action,amount,note,created_by)
    values(v_excess.business_id,p_excess_id,'loan_payment',v_apply,p_note,v_staff);
  return jsonb_build_object('ok',true,'amount_applied',v_apply,'excess_remaining',v_left,'loan_balance',v_balance,'repayment_id',v_repayment);
end $$;

grant execute on function public.bripta_record_future_excess(text,uuid,uuid,uuid,numeric,text,text,uuid) to authenticated, service_role;
grant execute on function public.bripta_apply_excess_to_charge(uuid,uuid,numeric,text) to authenticated;
grant execute on function public.bripta_resolve_excess(uuid,text,numeric,text) to authenticated;
grant execute on function public.bripta_settle_charge(uuid,text,text) to authenticated;
grant execute on function public.bripta_apply_excess_to_loan(uuid,uuid,numeric,text) to authenticated;

commit;

select 'Bripta future-only Excess & Charges setup is ready' as result,
       (select count(*) from public.bripta_excess_ledger) as historical_excess_rows_created,
       (select count(*) from public.bripta_charges) as historical_charge_rows_created;
