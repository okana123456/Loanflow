-- Bripta / Loanflow
-- Repayment sender tracking support.
--
-- This does not change repayment amounts, balances, loan statuses, schedules,
-- processing fees, registration fees, or accounting values.

begin;

alter table public.loan_repayments
  add column if not exists sender_name text,
  add column if not exists sender_phone text;

alter table public.mpesa_callback_queue
  add column if not exists middle_name text,
  add column if not exists last_name text;

-- Keep the post-clearance processing-fee payment path visible in repayments
-- with sender details too. This only changes metadata captured on future rows.
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

create index if not exists loan_repayments_business_sender_phone_idx
  on public.loan_repayments (business_id, sender_phone);

create index if not exists loan_repayments_business_payment_ref_idx
  on public.loan_repayments (business_id, payment_reference);

-- Backfill existing rows where the repayment is linked to the callback queue.
update public.loan_repayments r
set
  sender_name = nullif(trim(concat_ws(' ', q.first_name, q.middle_name, q.last_name)), ''),
  sender_phone = nullif(q.msisdn, '')
from public.mpesa_callback_queue q
where q.repayment_id = r.id
  and (r.sender_name is null or r.sender_phone is null);

-- Backfill rows matched from unmatched payments where the reference is known.
update public.loan_repayments r
set
  sender_name = coalesce(nullif(r.sender_name, ''), nullif(u.payer_name, '')),
  sender_phone = coalesce(nullif(r.sender_phone, ''), nullif(u.payer_phone, ''))
from public.unmatched_payments u
where r.business_id = u.business_id
  and (
    r.payment_reference = u.mpesa_reference
    or r.receipt_no = u.mpesa_reference
  )
  and (r.sender_name is null or r.sender_phone is null);

commit;

select
  'Bripta repayment sender tracking is ready' as result,
  count(*) filter (where sender_name is not null or sender_phone is not null) as repayment_rows_with_sender_details,
  false as financial_values_changed,
  false as balances_changed
from public.loan_repayments;
