-- One-off correction for Bripta September 2026 subscription.
-- Reason: payment was made directly with account BRIPTAF5E5D9, not through
-- the system STK prompt, so the STK callback had no checkout request to close.
-- This does not change loans, repayments, loan balances or client records.

begin;

insert into public.loan_billing_cycles (
  business_id,
  billing_month,
  amount,
  status,
  paid_at,
  paid_until,
  receipt_number,
  phone
)
values (
  'BIZ-B3F5E5D9',
  '2026-09-01',
  3000,
  'paid',
  '2026-09-02 04:37:00+00',
  '2026-10-04',
  'UI20L4RBUC',
  null
)
on conflict (business_id,billing_month) do update
set amount=excluded.amount,
    status='paid',
    paid_at=excluded.paid_at,
    paid_until=excluded.paid_until,
    receipt_number=excluded.receipt_number,
    phone=coalesce(public.loan_billing_cycles.phone, excluded.phone),
    updated_at=now();

commit;

select
  'Bripta September 2026 subscription payment recorded' as result,
  'BIZ-B3F5E5D9' as business_id,
  'UI20L4RBUC' as receipt_number,
  '2026-10-04' as paid_until,
  false as loan_balances_changed,
  false as repayments_changed;
