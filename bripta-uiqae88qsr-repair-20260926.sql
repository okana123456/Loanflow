-- Bripta: remove UIQAE88QSR from Anjeline and attach it to Miriam's existing
-- KES 50 correction entry. Run the whole file once in Supabase SQL Editor.

begin;

do $$
declare
  v_wrong_repayment constant uuid := '18f95d6b-e7a6-42a5-a486-89861d9b3d2b';
  v_correct_repayment constant uuid := '394f8b4b-60f6-404f-ba25-92e4748d9113';
  v_anjeline_loan constant uuid := '1f9e02f5-6a6e-45df-9937-3abe89141a88';
  v_miriam_loan constant uuid := '4a4070c5-66bb-4e32-ae6b-b1850d216576';
  v_payment record;
  v_schedule record;
  v_remaining numeric;
  v_apply numeric;
  v_total_paid numeric;
  v_balance numeric;
  v_arrears numeric;
  v_oldest_due date;
begin
  select * into v_payment
  from public.loan_repayments
  where id = v_wrong_repayment
  for update;

  if not found
     or v_payment.loan_id <> v_anjeline_loan
     or v_payment.amount <> 50
     or coalesce(v_payment.payment_reference, '') <> 'UIQAE88QSR' then
    raise exception 'Safety check failed: the Anjeline UIQAE88QSR repayment is not exactly as audited';
  end if;

  if not exists (
    select 1 from public.loan_repayments
    where id = v_correct_repayment
      and loan_id = v_miriam_loan
      and amount = 50
      and coalesce(payment_reference, '') = ''
  ) then
    raise exception 'Safety check failed: Miriam existing KES 50 correction entry was not found';
  end if;

  -- Unlink the callback before removing the incorrect repayment.
  update public.mpesa_callback_queue
  set confirmed = false, loan_id = null, repayment_id = null
  where trans_id = 'UIQAE88QSR'
    and repayment_id = v_wrong_repayment;

  delete from public.loan_repayments
  where id = v_wrong_repayment;

  -- Rebuild Anjeline's schedule allocation from all remaining repayments.
  update public.loan_schedules
  set total_paid = 0,
      status = case when due_date < current_date then 'overdue' else 'pending' end,
      paid_at = null
  where loan_id = v_anjeline_loan;

  for v_payment in
    select coalesce(loan_portion, amount, 0)::numeric as amount,
           payment_date
    from public.loan_repayments
    where loan_id = v_anjeline_loan
    order by payment_date asc, created_at asc, id asc
  loop
    v_remaining := v_payment.amount;
    for v_schedule in
      select id, due_date, total_due, total_paid
      from public.loan_schedules
      where loan_id = v_anjeline_loan
        and coalesce(total_paid, 0) < coalesce(total_due, 0)
      order by due_date asc, installment_no asc, id asc
      for update
    loop
      exit when v_remaining <= 0;
      v_apply := least(
        v_remaining,
        greatest(0, coalesce(v_schedule.total_due, 0) - coalesce(v_schedule.total_paid, 0))
      );
      update public.loan_schedules
      set total_paid = round(coalesce(total_paid, 0) + v_apply, 2),
          status = case
            when coalesce(total_paid, 0) + v_apply >= coalesce(total_due, 0) - 0.01 then 'paid'
            when due_date < current_date then 'overdue'
            else 'partial'
          end,
          paid_at = case
            when coalesce(total_paid, 0) + v_apply >= coalesce(total_due, 0) - 0.01
              then v_payment.payment_date
            else null
          end
      where id = v_schedule.id;
      v_remaining := round(v_remaining - v_apply, 2);
    end loop;
  end loop;

  select coalesce(sum(coalesce(loan_portion, amount, 0)), 0)
  into v_total_paid
  from public.loan_repayments
  where loan_id = v_anjeline_loan;

  select greatest(0, coalesce(total_payable, 0) - v_total_paid)
  into v_balance
  from public.loans
  where id = v_anjeline_loan
  for update;

  select
    coalesce(sum(greatest(0, coalesce(total_due, 0) - coalesce(total_paid, 0))), 0),
    min(due_date) filter (
      where due_date < current_date
        and coalesce(total_due, 0) - coalesce(total_paid, 0) > 0.01
    )
  into v_arrears, v_oldest_due
  from public.loan_schedules
  where loan_id = v_anjeline_loan
    and due_date < current_date;

  update public.loans
  set total_paid = round(v_total_paid, 2),
      outstanding_balance = round(v_balance, 2),
      arrears_amount = round(least(v_balance, greatest(0, v_arrears)), 2),
      overdue_days = case
        when v_oldest_due is null then 0
        else greatest(0, current_date - v_oldest_due)
      end,
      status = case when v_balance <= 0.01 then 'completed' else 'active' end
  where id = v_anjeline_loan;

  -- Miriam was already credited KES 50 manually eleven minutes later. Attach
  -- the real M-Pesa reference to that entry instead of crediting her twice.
  update public.loan_repayments
  set payment_reference = 'UIQAE88QSR',
      mpesa_confirmed = true,
      sender_name = 'MIRIAM ATIENO ODHIAMBO',
      sender_phone = '254708190258',
      notes = 'Corrected allocation: UIQAE88QSR moved from Anjeline to Miriam after wrong Paybill account entry 50'
  where id = v_correct_repayment;

  update public.mpesa_callback_queue
  set confirmed = true,
      loan_id = v_miriam_loan,
      repayment_id = v_correct_repayment,
      business_short_code = 'BIZ-B3F5E5D9'
  where trans_id = 'UIQAE88QSR';

  update public.unmatched_payments
  set resolved = true,
      resolved_at = now()
  where business_id = 'BIZ-B3F5E5D9'
    and mpesa_reference = 'UIQAE88QSR';
end;
$$;

commit;

select
  'transaction_repayments' as check_name,
  count(*)::numeric as result
from public.loan_repayments
where payment_reference = 'UIQAE88QSR'
union all
select
  'transaction_now_on_miriam_loan',
  count(*)::numeric
from public.loan_repayments
where payment_reference = 'UIQAE88QSR'
  and loan_id = '4a4070c5-66bb-4e32-ae6b-b1850d216576'
union all
select
  'anjeline_outstanding_after_reversal',
  outstanding_balance::numeric
from public.loans
where id = '1f9e02f5-6a6e-45df-9937-3abe89141a88'
union all
select
  'miriam_outstanding_after_existing_credit',
  outstanding_balance::numeric
from public.loans
where id = '4a4070c5-66bb-4e32-ae6b-b1850d216576';
