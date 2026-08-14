-- Bripta historical callback cleanup - SAFE, BACKED UP, NO BALANCE CHANGES
--
-- What this does:
-- 1. Repeats the strict one-callback-to-one-ledger-payment comparison.
-- 2. Backs up every affected unmatched and callback-queue row.
-- 3. Marks only confirmed one-to-one manual duplicates as resolved/dismissed.
-- 4. Moves the remaining identified Bripta-client rows into Bripta suspense.
-- 5. Dismisses their duplicate queue copies so each payment appears once.
--
-- It does not insert repayments or update loans, schedules, charges or balances.

begin;

alter table if exists public.unmatched_payments
  add column if not exists dismissed boolean not null default false,
  add column if not exists dismissed_at timestamptz,
  add column if not exists dismissed_by text;

alter table if exists public.mpesa_callback_queue
  add column if not exists dismissed boolean not null default false,
  add column if not exists dismissed_at timestamptz,
  add column if not exists dismissed_by text;

drop table if exists pg_temp.bripta_historical_callback_classification;

create temporary table bripta_historical_callback_classification as
with params as (
  select 'BIZ-B3F5E5D9'::text as business_id
), unresolved as (
  select
    u.id as payment_id,
    u.business_id as stored_business_id,
    u.mpesa_reference,
    u.amount::numeric as amount,
    u.account_number,
    u.created_at,
    (u.created_at at time zone 'Africa/Nairobi')::date as payment_day,
    right(regexp_replace(coalesce(u.account_number, ''), '\D', '', 'g'), 9) as account_tail
  from public.unmatched_payments u
  where coalesce(u.resolved, false) = false
), client_candidates as (
  select
    u.*,
    c.id as client_id,
    c.full_name,
    c.phone,
    count(*) over (partition by u.payment_id) as client_matches
  from unresolved u
  cross join params p
  join public.loan_clients c
    on c.business_id = p.business_id
   and length(u.account_tail) = 9
   and right(regexp_replace(coalesce(c.phone, ''), '\D', '', 'g'), 9) = u.account_tail
), unique_clients as (
  select * from client_candidates where client_matches = 1
), likely_loans as (
  select
    u.*,
    l.id as loan_id,
    l.loan_no
  from unique_clients u
  left join lateral (
    select l.*
    from public.loans l
    cross join params p
    where l.business_id = p.business_id
      and l.client_id = u.client_id
      and coalesce(l.disbursement_date, l.created_at::date) <= u.payment_day
    order by coalesce(l.disbursement_date, l.created_at::date) desc, l.created_at desc
    limit 1
  ) l on true
), numbered_callbacks as (
  select
    l.*,
    row_number() over (
      partition by l.loan_id, l.payment_day, round(l.amount, 2)
      order by l.created_at, l.payment_id
    ) as amount_day_sequence
  from likely_loans l
), numbered_ledger as (
  select
    r.id as repayment_id,
    r.loan_id,
    r.payment_date::date as payment_day,
    round(coalesce(r.amount, 0)::numeric, 2) as amount,
    row_number() over (
      partition by r.loan_id, r.payment_date::date, round(coalesce(r.amount, 0)::numeric, 2)
      order by r.created_at, r.id
    ) as amount_day_sequence
  from public.loan_repayments r
  cross join params p
  where r.business_id = p.business_id
)
select
  c.*,
  r.repayment_id as matched_repayment_id,
  case
    when c.loan_id is null then 'REVIEW_NO_EARLIER_LOAN'
    when r.repayment_id is not null then 'SAFE_MANUAL_DUPLICATE'
    else 'REVIEW_NO_LEDGER_MATCH'
  end as classification
from numbered_callbacks c
left join numbered_ledger r
  on r.loan_id = c.loan_id
 and r.payment_day = c.payment_day
 and abs(r.amount - round(c.amount, 2)) <= 0.01
 and r.amount_day_sequence = c.amount_day_sequence;

-- Permanent rollback copies. Re-running this script does not duplicate backups.
create table if not exists public.bripta_unmatched_cleanup_backup_20260814 as
select u.*, now() as backed_up_at
from public.unmatched_payments u
where false;

insert into public.bripta_unmatched_cleanup_backup_20260814
select u.*, now()
from public.unmatched_payments u
join bripta_historical_callback_classification c on c.payment_id = u.id
where not exists (
  select 1
  from public.bripta_unmatched_cleanup_backup_20260814 b
  where b.id = u.id
);

create table if not exists public.bripta_callback_queue_cleanup_backup_20260814 as
select q.*, now() as backed_up_at
from public.mpesa_callback_queue q
where false;

insert into public.bripta_callback_queue_cleanup_backup_20260814
select q.*, now()
from public.mpesa_callback_queue q
join bripta_historical_callback_classification c
  on upper(trim(q.trans_id)) = upper(trim(c.mpesa_reference))
where not exists (
  select 1
  from public.bripta_callback_queue_cleanup_backup_20260814 b
  where b.id = q.id
);

-- Confirmed duplicates already exist in repayment history. Hide only their
-- suspense copies; the recorded repayment remains untouched.
update public.unmatched_payments u
set
  business_id = 'BIZ-B3F5E5D9',
  resolved = true,
  resolved_at = coalesce(u.resolved_at, now()),
  dismissed = true,
  dismissed_at = coalesce(u.dismissed_at, now()),
  dismissed_by = coalesce(nullif(u.dismissed_by, ''), 'strict one-to-one historical cleanup')
from bripta_historical_callback_classification c
where u.id = c.payment_id
  and c.classification = 'SAFE_MANUAL_DUPLICATE';

-- Genuine review rows become visible in Bripta's suspense account. No money is
-- posted until an authorized user deliberately matches each row.
update public.unmatched_payments u
set
  business_id = 'BIZ-B3F5E5D9',
  dismissed = false,
  dismissed_at = null,
  dismissed_by = null
from bripta_historical_callback_classification c
where u.id = c.payment_id
  and c.classification <> 'SAFE_MANUAL_DUPLICATE';

-- Each payment exists in both the raw callback queue and unmatched table.
-- Keep the unmatched row as the single actionable copy and hide the queue copy.
update public.mpesa_callback_queue q
set
  dismissed = true,
  dismissed_at = coalesce(q.dismissed_at, now()),
  dismissed_by = coalesce(nullif(q.dismissed_by, ''), 'represented once in Bripta suspense')
from bripta_historical_callback_classification c
where upper(trim(q.trans_id)) = upper(trim(c.mpesa_reference));

commit;

-- Verification only.
select
  'Bripta historical suspense cleanup completed' as result,
  count(*) filter (where classification = 'SAFE_MANUAL_DUPLICATE') as duplicate_rows_safely_closed,
  round(coalesce(sum(amount) filter (where classification = 'SAFE_MANUAL_DUPLICATE'), 0), 2) as duplicate_amount_already_in_ledger,
  count(*) filter (where classification <> 'SAFE_MANUAL_DUPLICATE') as rows_moved_to_bripta_suspense,
  round(coalesce(sum(amount) filter (where classification <> 'SAFE_MANUAL_DUPLICATE'), 0), 2) as amount_waiting_for_review,
  false as repayments_inserted,
  false as loan_balances_changed,
  (select count(*) from public.bripta_unmatched_cleanup_backup_20260814) as unmatched_backup_rows,
  (select count(*) from public.bripta_callback_queue_cleanup_backup_20260814) as queue_backup_rows
from bripta_historical_callback_classification;
