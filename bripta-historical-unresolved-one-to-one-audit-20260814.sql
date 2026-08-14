-- Bripta historical unresolved-payment audit - strict one-to-one comparison
-- READ ONLY: this file changes no financial or suspense data.

drop table if exists pg_temp.bripta_one_to_one_audit;

create temporary table bripta_one_to_one_audit as
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
    l.loan_no,
    l.status as current_loan_status,
    l.disbursement_date,
    l.maturity_date,
    l.total_payable,
    l.outstanding_balance
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
    r.payment_date,
    r.payment_date::date as payment_day,
    round(coalesce(r.amount, 0)::numeric, 2) as amount,
    r.payment_method,
    r.payment_reference,
    r.receipt_no,
    row_number() over (
      partition by r.loan_id, r.payment_date::date, round(coalesce(r.amount, 0)::numeric, 2)
      order by r.created_at, r.id
    ) as amount_day_sequence
  from public.loan_repayments r
  cross join params p
  where r.business_id = p.business_id
), compared as (
  select
    c.*,
    r.repayment_id as matched_repayment_id,
    r.payment_date as matched_payment_date,
    r.payment_method as matched_payment_method,
    r.payment_reference as matched_payment_reference,
    r.receipt_no as matched_receipt_no,
    case
      when c.loan_id is null then 'NO_EARLIER_LOAN'
      when r.repayment_id is not null then 'ONE_TO_ONE_MANUAL_DUPLICATE'
      else 'NO_ONE_TO_ONE_LEDGER_MATCH'
    end as classification
  from numbered_callbacks c
  left join numbered_ledger r
    on r.loan_id = c.loan_id
   and r.payment_day = c.payment_day
   and abs(r.amount - round(c.amount, 2)) <= 0.01
   and r.amount_day_sequence = c.amount_day_sequence
)
select * from compared;

with sections as (
  select 1 as section_order, 'strict_one_to_one_summary'::text as section,
    jsonb_build_object(
      'rows_compared', count(*),
      'one_to_one_manual_duplicates', count(*) filter (where classification = 'ONE_TO_ONE_MANUAL_DUPLICATE'),
      'no_one_to_one_ledger_match', count(*) filter (where classification = 'NO_ONE_TO_ONE_LEDGER_MATCH'),
      'no_earlier_loan', count(*) filter (where classification = 'NO_EARLIER_LOAN'),
      'duplicate_amount', round(coalesce(sum(amount) filter (where classification = 'ONE_TO_ONE_MANUAL_DUPLICATE'), 0), 2),
      'unmatched_amount', round(coalesce(sum(amount) filter (where classification = 'NO_ONE_TO_ONE_LEDGER_MATCH'), 0), 2),
      'financial_data_changed', false
    ) as result
  from bripta_one_to_one_audit

  union all

  select 2, 'safe_duplicate_rows', coalesce(jsonb_agg(jsonb_build_object(
    'payment_id', payment_id,
    'reference', mpesa_reference,
    'amount', amount,
    'received_at', created_at,
    'client', full_name,
    'phone', phone,
    'loan_no', loan_no,
    'matching_repayment_id', matched_repayment_id,
    'matching_payment_date', matched_payment_date,
    'matching_method', matched_payment_method,
    'matching_reference', matched_payment_reference,
    'matching_receipt', matched_receipt_no
  ) order by created_at desc), '[]'::jsonb)
  from bripta_one_to_one_audit
  where classification = 'ONE_TO_ONE_MANUAL_DUPLICATE'

  union all

  select 3, 'still_requires_suspense_review', coalesce(jsonb_agg(jsonb_build_object(
    'payment_id', payment_id,
    'reference', mpesa_reference,
    'amount', amount,
    'received_at', created_at,
    'client', full_name,
    'phone', phone,
    'loan_id', loan_id,
    'loan_no', loan_no,
    'loan_status', current_loan_status,
    'disbursement_date', disbursement_date,
    'maturity_date', maturity_date,
    'classification', classification
  ) order by created_at desc), '[]'::jsonb)
  from bripta_one_to_one_audit
  where classification <> 'ONE_TO_ONE_MANUAL_DUPLICATE'
)
select * from sections order by section_order;
