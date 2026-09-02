-- Bripta SMS bundle M-Pesa payment support.
-- This adds payment tracking fields to SMS bundle requests only. It does not
-- change loans, repayments, balances, clients or accounting figures.

begin;

alter table public.bripta_sms_bundle_requests
  add column if not exists amount numeric(14,2),
  add column if not exists merchant_request_id text,
  add column if not exists checkout_request_id text,
  add column if not exists result_code text,
  add column if not exists result_description text,
  add column if not exists provider_response jsonb,
  add column if not exists paid_at timestamptz;

create unique index if not exists bripta_sms_bundle_requests_checkout_uidx
  on public.bripta_sms_bundle_requests(checkout_request_id)
  where checkout_request_id is not null;

create index if not exists bripta_sms_bundle_requests_checkout_idx
  on public.bripta_sms_bundle_requests(checkout_request_id);

commit;

select
  'Bripta SMS bundle M-Pesa tracking is ready' as result,
  true as sms_bundle_payment_ready,
  false as loan_balances_changed,
  false as repayments_changed,
  false as client_data_changed;
