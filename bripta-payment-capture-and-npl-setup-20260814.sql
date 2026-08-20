-- Bripta payment capture and NPL reporting setup - 14 Aug 2026
-- Run once in the Bripta Supabase SQL Editor before redeploying payment-callback.
-- This setup does not recalculate historical loan balances or auto-match old suspense rows.

begin;

-- Preserve an explicitly labelled charge payment. Ordinary repayments continue
-- to allocate to the loan first, with only the true overpayment becoming excess.
create or replace function public.bripta_prepare_future_repayment()
returns trigger language plpgsql set search_path = public
as $$
declare
  v_balance numeric;
  v_total numeric;
  v_interest numeric;
  v_ratio numeric;
  v_charge_portion numeric;
  v_available numeric;
  v_loan_portion numeric;
begin
  select greatest(0,coalesce(l.outstanding_balance,0)),
         coalesce(l.total_payable,0),
         coalesce(l.total_interest,0)
    into v_balance,v_total,v_interest
  from public.loans l
  where l.id=new.loan_id;

  if not found then return new; end if;

  if coalesce(new.notes,'') like '%[BRIPTA_CHARGE_PAYMENT]%' then
    v_charge_portion := greatest(0,
      coalesce(new.registration_fee_portion,0) +
      coalesce(new.processing_fee_portion,0) +
      coalesce(new.penalty_portion,0)
    );
    if v_charge_portion > coalesce(new.amount,0) + 0.01 then
      raise exception 'Charge allocation cannot exceed the payment amount';
    end if;
    v_available := greatest(0,coalesce(new.amount,0)-v_charge_portion);
    v_loan_portion := round(least(v_available,v_balance,greatest(0,coalesce(new.loan_portion,0))),2);
  else
    new.registration_fee_portion := 0;
    new.processing_fee_portion := 0;
    v_available := greatest(0,coalesce(new.amount,0)-coalesce(new.penalty_portion,0));
    v_loan_portion := round(least(v_available,v_balance),2);
  end if;

  new.loan_portion := v_loan_portion;
  new.credit_portion := round(greatest(0,v_available-v_loan_portion),2);
  v_ratio := case when v_total>0 and v_interest>0 then v_interest/v_total else 0 end;
  new.interest_portion := round(v_loan_portion*v_ratio,2);
  new.principal_portion := round(v_loan_portion-new.interest_portion,2);
  return new;
end $$;

drop trigger if exists bripta_prepare_future_repayment_trigger on public.loan_repayments;
create trigger bripta_prepare_future_repayment_trigger
before insert on public.loan_repayments
for each row execute function public.bripta_prepare_future_repayment();

-- Atomic callback operation for a processing fee received after the related
-- loan has cleared. It creates a visible payment row, settles only the charge,
-- leaves the cleared loan balance at zero and records any remainder as excess.
create or replace function public.bripta_record_callback_charge_payment(
  p_business_id text,
  p_client_id uuid,
  p_loan_id uuid,
  p_charge_id uuid,
  p_amount numeric,
  p_payment_reference text,
  p_payment_date timestamptz,
  p_payer_name text default null,
  p_payer_phone text default null
) returns jsonb
language plpgsql security definer set search_path = public
as $$
declare
  v_charge public.bripta_charges%rowtype;
  v_loan public.loans%rowtype;
  v_repayment_id uuid;
  v_fee_balance numeric;
  v_fee_paid numeric;
  v_excess numeric;
begin
  if coalesce(p_amount,0)<=0 then raise exception 'Payment amount must be greater than zero'; end if;
  if nullif(trim(coalesce(p_payment_reference,'')),'') is null then raise exception 'Payment reference is required'; end if;

  select id into v_repayment_id
  from public.loan_repayments
  where business_id=p_business_id
    and (payment_reference=p_payment_reference or receipt_no=p_payment_reference)
  limit 1;
  if v_repayment_id is not null then
    return jsonb_build_object('ok',true,'duplicate',true,'repayment_id',v_repayment_id);
  end if;

  select * into v_charge from public.bripta_charges where id=p_charge_id for update;
  select * into v_loan from public.loans where id=p_loan_id for update;
  if v_charge.id is null or v_loan.id is null then raise exception 'Charge or loan was not found'; end if;
  if v_charge.business_id<>p_business_id or v_charge.client_id<>p_client_id
     or v_loan.business_id<>p_business_id or v_loan.client_id<>p_client_id
     or v_charge.loan_id<>p_loan_id then
    raise exception 'Charge, loan and client do not belong to the same Bripta business';
  end if;
  if v_charge.charge_type<>'processing_fee' or v_charge.status not in ('pending','partially_paid') then
    raise exception 'The processing fee is no longer pending';
  end if;

  v_fee_balance:=round(greatest(0,v_charge.amount-v_charge.amount_paid),2);
  v_fee_paid:=round(least(p_amount,v_fee_balance),2);
  v_excess:=round(greatest(0,p_amount-v_fee_paid),2);
  if v_fee_paid<=0 then raise exception 'The processing fee is already fully paid'; end if;

  insert into public.loan_repayments(
    loan_id,receipt_no,amount,payment_method,payment_reference,payment_date,
    principal_portion,interest_portion,penalty_portion,registration_fee_portion,
    processing_fee_portion,loan_portion,credit_portion,mpesa_confirmed,business_id,
    sender_name,sender_phone,notes
  ) values (
    p_loan_id,p_payment_reference,round(p_amount,2),'M-Pesa',p_payment_reference,
    coalesce(p_payment_date,now()),0,0,0,0,v_fee_paid,0,v_excess,true,p_business_id,
    nullif(trim(coalesce(p_payer_name,'')),''),
    nullif(trim(coalesce(p_payer_phone,'')),''),
    '[BRIPTA_CHARGE_PAYMENT] Auto-confirmed processing-fee payment after loan clearance. Payer: '||coalesce(p_payer_name,'')
  ) returning id into v_repayment_id;

  update public.bripta_charges
  set amount_paid=round(amount_paid+v_fee_paid,2),
      status=case when amount_paid+v_fee_paid>=amount-0.01 then 'paid' else 'partially_paid' end,
      settled_at=case when amount_paid+v_fee_paid>=amount-0.01 then coalesce(p_payment_date,now()) else null end,
      settlement_note='Paid automatically through M-Pesa reference '||p_payment_reference,
      updated_at=now()
  where id=p_charge_id;

  update public.loans
  set processing_fee_paid=least(coalesce(processing_fee,0),coalesce(processing_fee_paid,0)+v_fee_paid)
  where id=p_loan_id;

  return jsonb_build_object(
    'ok',true,
    'repayment_id',v_repayment_id,
    'processing_fee_paid',v_fee_paid,
    'excess_recorded',v_excess,
    'loan_balance_unchanged',v_loan.outstanding_balance
  );
end $$;

grant execute on function public.bripta_record_callback_charge_payment(text,uuid,uuid,uuid,numeric,text,timestamptz,text,text)
to service_role;

-- Keep the existing admin action, but make a paid processing/registration
-- charge visible in Payment History instead of changing only the charge card.
create or replace function public.bripta_settle_charge(
  p_charge_id uuid, p_action text, p_note text default null
) returns jsonb
language plpgsql security definer set search_path = public
as $$
declare
  v_charge public.bripta_charges%rowtype;
  v_staff uuid;
  v_amount numeric;
  v_repayment uuid;
  v_reference text;
begin
  if p_action not in ('paid','waived') then raise exception 'Invalid charge action'; end if;
  select * into v_charge from public.bripta_charges where id=p_charge_id for update;
  if v_charge.id is null then raise exception 'Charge was not found'; end if;

  select s.id into v_staff
  from public.loan_staff s
  where s.auth_user_id=auth.uid() and s.business_id=v_charge.business_id and s.is_active=true
    and (s.role ilike '%admin%' or s.role ilike '%branch_manager%')
  limit 1;
  if v_staff is null then raise exception 'Only an administrator can settle charges'; end if;
  if v_charge.status in ('paid','waived') then
    return jsonb_build_object('ok',true,'status',v_charge.status,'already_settled',true);
  end if;

  v_amount:=round(greatest(0,v_charge.amount-v_charge.amount_paid),2);
  if p_action='paid' and v_amount>0 and v_charge.loan_id is not null
     and v_charge.charge_type in ('processing_fee','registration_fee','penalty') then
    v_reference:=coalesce(nullif(trim(p_note),''),'Manual charge payment '||to_char(clock_timestamp(),'YYYYMMDDHH24MISS'));
    insert into public.loan_repayments(
      loan_id,receipt_no,amount,payment_method,payment_reference,payment_date,
      principal_portion,interest_portion,penalty_portion,registration_fee_portion,
      processing_fee_portion,loan_portion,credit_portion,mpesa_confirmed,collected_by,business_id,notes
    ) values (
      v_charge.loan_id,'CHG-'||upper(substr(replace(gen_random_uuid()::text,'-',''),1,10)),v_amount,
      'manual_charge',v_reference,now(),0,0,
      case when v_charge.charge_type='penalty' then v_amount else 0 end,
      case when v_charge.charge_type='registration_fee' then v_amount else 0 end,
      case when v_charge.charge_type='processing_fee' then v_amount else 0 end,
      0,0,false,v_staff,v_charge.business_id,
      '[BRIPTA_CHARGE_PAYMENT] Charge confirmed manually. '||coalesce(p_note,'')
    ) returning id into v_repayment;

    if v_charge.charge_type='processing_fee' then
      update public.loans
      set processing_fee_paid=least(coalesce(processing_fee,0),coalesce(processing_fee_paid,0)+v_amount)
      where id=v_charge.loan_id;
    elsif v_charge.charge_type='registration_fee' then
      update public.loans
      set registration_fee_paid=least(coalesce(registration_fee_due,0),coalesce(registration_fee_paid,0)+v_amount)
      where id=v_charge.loan_id;
    end if;
  end if;

  update public.bripta_charges
  set amount_paid=case when p_action='paid' then amount else amount_paid end,
      status=p_action,settled_at=now(),settled_by=v_staff,
      settlement_note=p_note,updated_at=now()
  where id=p_charge_id;

  return jsonb_build_object('ok',true,'status',p_action,'amount',v_amount,'repayment_id',v_repayment);
end $$;

grant execute on function public.bripta_settle_charge(uuid,text,text) to authenticated;

create index if not exists loan_clients_business_phone_idx
  on public.loan_clients(business_id,phone);
create index if not exists bripta_charges_pending_client_idx
  on public.bripta_charges(business_id,client_id,charge_date)
  where status in ('pending','partially_paid');
create index if not exists loans_business_client_status_idx
  on public.loans(business_id,client_id,status,created_at desc);

commit;

-- Read-only deployment and historical review report. No historical money is changed.
with unresolved as (
  select u.id,u.business_id,u.mpesa_reference,u.amount,u.payer_phone,u.account_number,u.created_at,
         coalesce(nullif(regexp_replace(u.account_number,'\D','','g'),''),
                  nullif(regexp_replace(u.payer_phone,'\D','','g'),'')) as match_digits
  from public.unmatched_payments u
  where coalesce(u.resolved,false)=false
), probable as (
  select u.*,
         c.id as client_id,c.full_name,c.phone,
         (select count(*) from public.loans l where l.business_id=u.business_id and l.client_id=c.id and l.status='active' and l.outstanding_balance>0) as active_loans,
         (select count(*) from public.bripta_charges ch where ch.business_id=u.business_id and ch.client_id=c.id and ch.charge_type='processing_fee' and ch.status in ('pending','partially_paid')) as pending_processing_fees
  from unresolved u
  left join public.loan_clients c
    on c.business_id=u.business_id
   and right(regexp_replace(c.phone,'\D','','g'),9)=right(u.match_digits,9)
)
select 1 as section_order,'deployment_check' as section,
       jsonb_build_object(
         'callback_charge_function_ready',to_regprocedure('public.bripta_record_callback_charge_payment(text,uuid,uuid,uuid,numeric,text,timestamp with time zone,text)') is not null,
         'npl_threshold_days',180,
         'historical_balances_changed',false
       ) as result
union all
select 2,'unresolved_payment_summary',jsonb_build_object(
  'unresolved_rows',count(*),
  'probable_client_matches',count(*) filter(where client_id is not null),
  'probable_active_loan_matches',count(*) filter(where active_loans=1),
  'probable_post_clearance_fee_matches',count(*) filter(where active_loans=0 and pending_processing_fees>0)
) from probable
union all
select 3,'unresolved_payments_requiring_review',coalesce(jsonb_agg(jsonb_build_object(
  'reference',mpesa_reference,'amount',amount,'received_at',created_at,
  'payer_phone',payer_phone,'account_number',account_number,'client',full_name,
  'client_phone',phone,'active_loans',active_loans,'pending_processing_fees',pending_processing_fees
) order by created_at desc) filter(where client_id is not null),'[]'::jsonb)
from probable;
