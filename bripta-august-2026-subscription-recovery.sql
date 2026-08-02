-- Bripta: recover Brian Obanda Ochieng's verified August 2026 subscription.
-- Evidence supplied: KES 3,000 received on 02 Aug 2026 at 08:07 EAT,
-- M-Pesa receipt UH20L17F3A, business BIZ-B3F5E5D9.

begin;

do $$
begin
  if exists (
    select 1
    from public.loan_billing_cycles
    where receipt_number = 'UH20L17F3A'
      and business_id <> 'BIZ-B3F5E5D9'
  ) then
    raise exception 'Receipt UH20L17F3A is already assigned to a different business.';
  end if;
end;
$$;

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
  '2026-08-01',
  3000,
  'paid',
  '2026-08-02 05:07:00+00',
  '2026-09-04',
  'UH20L17F3A',
  null
)
on conflict (business_id, billing_month)
do update set
  amount = excluded.amount,
  status = excluded.status,
  paid_at = excluded.paid_at,
  paid_until = excluded.paid_until,
  receipt_number = excluded.receipt_number,
  phone = coalesce(public.loan_billing_cycles.phone, excluded.phone),
  updated_at = now();

commit;

select
  business_id,
  billing_month,
  amount,
  status,
  paid_at,
  paid_until,
  receipt_number
from public.loan_billing_cycles
where business_id = 'BIZ-B3F5E5D9'
  and billing_month::text like '2026-08-01%';
