-- Bripta / Loanflow: phone-payment fee allocation setup
-- Run once in the Bripta Supabase SQL Editor before deploying the updated files.
-- Existing loans keep fee dues at zero, so historical clients are not charged again.

begin;

alter table public.loans
  add column if not exists registration_fee_due numeric(14,2) not null default 0,
  add column if not exists registration_fee_paid numeric(14,2) not null default 0,
  add column if not exists processing_fee_paid numeric(14,2) not null default 0;

alter table public.loan_clients
  add column if not exists account_credit numeric(14,2) not null default 0;

alter table public.loan_repayments
  add column if not exists registration_fee_portion numeric(14,2) not null default 0,
  add column if not exists processing_fee_portion numeric(14,2) not null default 0,
  add column if not exists loan_portion numeric(14,2),
  add column if not exists credit_portion numeric(14,2) not null default 0;

create index if not exists loan_clients_business_phone_idx
  on public.loan_clients (business_id, phone);

create index if not exists loan_repayments_loan_payment_date_idx
  on public.loan_repayments (loan_id, payment_date, created_at);

-- Existing repayment rows pre-date allocation columns; their entire amount was
-- a loan repayment. New rows explicitly store loan_portion, including zero.
update public.loan_repayments
set loan_portion = amount
where loan_portion is null;

-- Replace the earlier balance trigger so fees and client credit never reduce
-- the loan. Only loan_portion is included in total_paid/outstanding balance.
create or replace function public.bripta_sync_loan_balance_from_repayments()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  new_loan_id public.loans.id%type;
  old_loan_id public.loans.id%type;
begin
  if tg_op <> 'DELETE' then new_loan_id := new.loan_id; end if;
  if tg_op <> 'INSERT' then old_loan_id := old.loan_id; end if;

  if new_loan_id is not null then
    update public.loans l
    set
      total_paid = x.repayment_total,
      outstanding_balance = x.expected_balance,
      status = case
        when x.expected_balance <= 0.01 then 'completed'
        when l.status = 'completed' then 'active'
        else l.status
      end,
      arrears_amount = case
        when x.expected_balance <= 0.01 then 0
        else least(greatest(0, coalesce(l.arrears_amount, 0)::numeric), x.expected_balance)
      end,
      overdue_days = case when x.expected_balance <= 0.01 then 0 else coalesce(l.overdue_days, 0) end
    from (
      select
        round(coalesce(sum(coalesce(r.loan_portion, r.amount)), 0)::numeric, 2) as repayment_total,
        round(greatest(
          0,
          coalesce((select total_payable from public.loans where id = new_loan_id), 0)::numeric
          - coalesce(sum(coalesce(r.loan_portion, r.amount)), 0)::numeric
        ), 2) as expected_balance
      from public.loan_repayments r
      where r.loan_id = new_loan_id
    ) x
    where l.id = new_loan_id;
  end if;

  if old_loan_id is not null and old_loan_id is distinct from new_loan_id then
    update public.loans l
    set
      total_paid = x.repayment_total,
      outstanding_balance = x.expected_balance,
      status = case
        when x.expected_balance <= 0.01 then 'completed'
        when l.status = 'completed' then 'active'
        else l.status
      end,
      arrears_amount = case
        when x.expected_balance <= 0.01 then 0
        else least(greatest(0, coalesce(l.arrears_amount, 0)::numeric), x.expected_balance)
      end,
      overdue_days = case when x.expected_balance <= 0.01 then 0 else coalesce(l.overdue_days, 0) end
    from (
      select
        round(coalesce(sum(coalesce(r.loan_portion, r.amount)), 0)::numeric, 2) as repayment_total,
        round(greatest(
          0,
          coalesce((select total_payable from public.loans where id = old_loan_id), 0)::numeric
          - coalesce(sum(coalesce(r.loan_portion, r.amount)), 0)::numeric
        ), 2) as expected_balance
      from public.loan_repayments r
      where r.loan_id = old_loan_id
    ) x
    where l.id = old_loan_id;
  end if;

  if tg_op = 'DELETE' then return old; end if;
  return new;
end;
$$;

drop trigger if exists bripta_sync_loan_balance_trigger on public.loan_repayments;
create trigger bripta_sync_loan_balance_trigger
after insert or update or delete on public.loan_repayments
for each row execute function public.bripta_sync_loan_balance_from_repayments();

commit;

select
  'Bripta phone payment allocation is ready' as result,
  count(*) filter (where registration_fee_due > 0 or processing_fee_paid > 0) as allocation_enabled_loans
from public.loans;
