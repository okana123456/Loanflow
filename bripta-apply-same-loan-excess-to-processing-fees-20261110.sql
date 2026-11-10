-- Bripta only. Apply verified same-loan excess to an unpaid processing fee.
-- Keeps original repayment amounts, loan balances and fee charges unchanged.
-- Rerunnable: only positive available excess and unpaid fees are selected.
begin;

create or replace function public.bripta_apply_same_loan_processing_excess(p_excess_id uuid)
returns numeric language plpgsql security definer set search_path=public as $$
declare
  v_x public.bripta_excess_ledger%rowtype;
  v_l public.loans%rowtype;
  v_c public.bripta_charges%rowtype;
  v_r public.loan_repayments%rowtype;
  v_amount numeric(14,2);
  v_note text:='Same-loan client excess applied to outstanding processing fee; original repayment preserved';
begin
  select * into v_x from public.bripta_excess_ledger
    where id=p_excess_id and business_id='BIZ-B3F5E5D9' for update;
  if v_x.id is null or v_x.amount_available<=0 or v_x.loan_id is null
    or v_x.repayment_id is null then return 0; end if;
  select * into v_l from public.loans where id=v_x.loan_id
    and business_id=v_x.business_id and client_id=v_x.client_id
    and branch_id=v_x.branch_id for update;
  if v_l.id is null then return 0; end if;
  select * into v_r from public.loan_repayments where id=v_x.repayment_id
    and loan_id=v_l.id and business_id=v_l.business_id
    and branch_id=v_l.branch_id for update;
  if v_r.id is null or coalesce(v_r.processing_fee_portion,0)>0
    or coalesce(v_r.loan_portion,0)<=0
    or abs(coalesce(v_r.credit_portion,0)-v_x.amount_original)>0.01
    or v_r.payment_date::date<v_l.disbursement_date then return 0; end if;
  select * into v_c from public.bripta_charges
    where loan_id=v_l.id and client_id=v_l.client_id
      and business_id=v_l.business_id and branch_id=v_l.branch_id
      and charge_type='processing_fee' and source='system'
      and amount_paid<amount
    order by charge_date,id limit 1 for update;
  if v_c.id is null or abs(v_c.amount-v_l.processing_fee)>0.01
    or abs(v_c.amount_paid-coalesce(v_l.processing_fee_paid,0))>0.01
  then return 0; end if;
  v_amount:=least(v_x.amount_available,v_c.amount-v_c.amount_paid);
  if v_amount<=0 then return 0; end if;

  update public.bripta_excess_ledger set amount_available=amount_available-v_amount,
    status=case when amount_available-v_amount<=0 then 'applied' else 'partially_used' end,
    resolved_at=case when amount_available-v_amount<=0 then now() else null end,
    resolution_note=v_note,updated_at=now()
    where id=v_x.id;
  update public.bripta_charges set amount_paid=amount_paid+v_amount,
    status=case when amount_paid+v_amount>=amount then 'paid' else 'partially_paid' end,
    settled_at=case when amount_paid+v_amount>=amount then now() else null end,
    settlement_note=v_note,updated_at=now() where id=v_c.id;
  update public.loans set processing_fee_paid=coalesce(processing_fee_paid,0)+v_amount,
    updated_at=now() where id=v_l.id;
  insert into public.bripta_excess_allocations
    (business_id,branch_id,excess_id,charge_id,action,amount,note,created_by)
  values(v_x.business_id,v_x.branch_id,v_x.id,v_c.id,'charge_payment',v_amount,
    v_note,v_x.created_by);

  -- The cash was already received. Move only the liability into fee receivable;
  -- do not recognize the fee a second time as income.
  perform public.bripta_post_entry(v_x.business_id,v_x.branch_id,v_r.payment_date::date,
    '2100','Client Credit','liability',v_amount,0,v_note,
    'bripta_excess_allocations',v_x.id::text||':'||v_c.id::text,'processing_client_credit',
    v_l.id,null,null,v_x.created_by);
  perform public.bripta_post_entry(v_x.business_id,v_x.branch_id,v_r.payment_date::date,
    '1150','Client Charges Receivable','asset',0,v_amount,v_note,
    'bripta_excess_allocations',v_x.id::text||':'||v_c.id::text,'processing_charge_settled',
    v_l.id,null,null,v_x.created_by);
  return v_amount;
end $$;

revoke all on function public.bripta_apply_same_loan_processing_excess(uuid)
  from public,anon,authenticated;

-- Keep the loan's fee-paid field aligned when an administrator allocates
-- excess through the existing Charges & Excess screen as well.
create or replace function public.bripta_sync_processing_fee_paid_from_allocation()
returns trigger language plpgsql security definer set search_path=public as $$
declare v_c public.bripta_charges%rowtype;
begin
  if new.action<>'charge_payment' or new.charge_id is null then return new; end if;
  select * into v_c from public.bripta_charges where id=new.charge_id;
  if v_c.charge_type='processing_fee' and v_c.loan_id is not null
    and v_c.business_id=new.business_id and v_c.branch_id=new.branch_id then
    update public.loans set processing_fee_paid=least(processing_fee,
      greatest(coalesce(processing_fee_paid,0),v_c.amount_paid)),updated_at=now()
    where id=v_c.loan_id and business_id=v_c.business_id
      and branch_id=v_c.branch_id and client_id=v_c.client_id;
  end if;
  return new;
end $$;
drop trigger if exists bripta_sync_processing_fee_paid_from_allocation
  on public.bripta_excess_allocations;
create trigger bripta_sync_processing_fee_paid_from_allocation
after insert on public.bripta_excess_allocations for each row
execute function public.bripta_sync_processing_fee_paid_from_allocation();

-- New verified excess is allocated automatically once its ledger row exists.
create or replace function public.bripta_auto_apply_same_loan_processing_excess()
returns trigger language plpgsql security definer set search_path=public as $$
begin
  if new.business_id='BIZ-B3F5E5D9' then
    perform public.bripta_apply_same_loan_processing_excess(new.id);
  end if;
  return new;
end $$;
drop trigger if exists bripta_auto_apply_same_loan_processing_excess
  on public.bripta_excess_ledger;
create trigger bripta_auto_apply_same_loan_processing_excess
after insert on public.bripta_excess_ledger for each row
execute function public.bripta_auto_apply_same_loan_processing_excess();

-- Existing rows: review each under row locks. Ambiguous or already settled rows
-- return zero and remain untouched. All work is one transaction.
do $$
declare v_id uuid;
begin
  for v_id in
    select x.id from public.bripta_excess_ledger x
    where x.business_id='BIZ-B3F5E5D9' and x.amount_available>0
      and x.loan_id is not null and x.repayment_id is not null
    order by x.created_at,x.id
  loop
    perform public.bripta_apply_same_loan_processing_excess(v_id);
  end loop;
end $$;

commit;

-- Result: applied amount, remaining excess and branch boundaries.
select b.name as branch,count(*) as allocations,
  coalesce(sum(a.amount),0) as processing_fee_settled_from_excess,
  coalesce(sum(x.amount_available),0) as remaining_on_these_excess_rows,
  count(*) filter(where c.amount_paid>c.amount or l.processing_fee_paid>l.processing_fee)
    as overpaid_fee_errors
from public.bripta_excess_allocations a
join public.bripta_excess_ledger x on x.id=a.excess_id
join public.bripta_charges c on c.id=a.charge_id
join public.loans l on l.id=c.loan_id
join public.bripta_branches b on b.id=x.branch_id
where a.business_id='BIZ-B3F5E5D9' and a.action='charge_payment'
  and c.charge_type='processing_fee'
  and a.note like 'Same-loan client excess applied to outstanding processing fee%'
group by b.name order by b.name;
