-- Bripta historical unresolved-payment duplicate audit - READ ONLY
-- Run after bripta-live-unresolved-cross-workspace-audit-20260814.sql.
-- It compares old callback rows with the live Bripta repayment ledger so that
-- manually recorded payments are not posted for a second time.

drop table if exists pg_temp.bripta_unresolved_duplicate_audit;

create temporary table bripta_unresolved_duplicate_audit as
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
  select *
  from client_candidates
  where client_matches = 1
), likely_loans as (
  select
    u.*,
    l.id as loan_id,
    l.loan_no,
    l.disbursement_date,
    l.maturity_date,
    l.total_payable,
    l.status as current_loan_status
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
), compared as (
  select
    u.*,
    exact_match.repayment_id as exact_same_day_amount_repayment_id,
    exact_match.payment_date as exact_match_payment_date,
    exact_match.payment_method as exact_match_method,
    exact_match.payment_reference as exact_match_reference,
    exact_match.receipt_no as exact_match_receipt,
    near_match.repayment_id as nearby_amount_repayment_id,
    near_match.payment_date as nearby_match_payment_date,
    near_match.payment_method as nearby_match_method,
    near_match.payment_reference as nearby_match_reference,
    near_match.receipt_no as nearby_match_receipt,
    coalesce(exact_match.same_day_candidates, 0) as same_day_candidates,
    coalesce(near_match.nearby_candidates, 0) as nearby_candidates
  from likely_loans u
  left join lateral (
    select
      min(r.id::text)::uuid as repayment_id,
      min(r.payment_date) as payment_date,
      min(coalesce(r.payment_method, '')) as payment_method,
      min(coalesce(r.payment_reference, '')) as payment_reference,
      min(coalesce(r.receipt_no, '')) as receipt_no,
      count(*) as same_day_candidates
    from public.loan_repayments r
    cross join params p
    where r.business_id = p.business_id
      and r.loan_id = u.loan_id
      and abs(coalesce(r.amount, 0)::numeric - u.amount) <= 0.01
      and r.payment_date::date = u.payment_day
  ) exact_match on true
  left join lateral (
    select
      min(r.id::text)::uuid as repayment_id,
      min(r.payment_date) as payment_date,
      min(coalesce(r.payment_method, '')) as payment_method,
      min(coalesce(r.payment_reference, '')) as payment_reference,
      min(coalesce(r.receipt_no, '')) as receipt_no,
      count(*) as nearby_candidates
    from public.loan_repayments r
    cross join params p
    where r.business_id = p.business_id
      and r.loan_id = u.loan_id
      and abs(coalesce(r.amount, 0)::numeric - u.amount) <= 0.01
      and r.payment_date::date between u.payment_day - 1 and u.payment_day + 1
  ) near_match on true
)
select
  *,
  case
    when loan_id is null then 'NO_EARLIER_LOAN'
    when same_day_candidates = 1 then 'PROBABLE_MANUAL_DUPLICATE_EXACT_DAY'
    when same_day_candidates > 1 then 'MULTIPLE_SAME_DAY_CANDIDATES_REVIEW'
    when nearby_candidates = 1 then 'PROBABLE_MANUAL_DUPLICATE_WITHIN_ONE_DAY'
    when nearby_candidates > 1 then 'MULTIPLE_NEARBY_CANDIDATES_REVIEW'
    else 'NO_LEDGER_MATCH_REQUIRES_REVIEW'
  end as classification
from compared;

with sections as (
  select
    1 as section_order,
    'comparison_summary'::text as section,
    jsonb_build_object(
      'rows_compared', count(*),
      'total_amount_compared', round(coalesce(sum(amount), 0), 2),
      'probable_manual_duplicates_exact_day', count(*) filter (where classification = 'PROBABLE_MANUAL_DUPLICATE_EXACT_DAY'),
      'probable_manual_duplicates_within_one_day', count(*) filter (where classification = 'PROBABLE_MANUAL_DUPLICATE_WITHIN_ONE_DAY'),
      'multiple_candidates_requiring_review', count(*) filter (where classification like 'MULTIPLE_%'),
      'no_ledger_match_requiring_review', count(*) filter (where classification = 'NO_LEDGER_MATCH_REQUIRES_REVIEW'),
      'no_earlier_loan', count(*) filter (where classification = 'NO_EARLIER_LOAN'),
      'financial_data_changed', false
    ) as result
  from bripta_unresolved_duplicate_audit

  union all

  select 2, 'probable_manual_duplicates', coalesce(jsonb_agg(jsonb_build_object(
    'reference', mpesa_reference,
    'amount', amount,
    'received_at', created_at,
    'client', full_name,
    'phone', phone,
    'loan_no', loan_no,
    'classification', classification,
    'matching_repayment_id', coalesce(exact_same_day_amount_repayment_id, nearby_amount_repayment_id),
    'matching_payment_date', coalesce(exact_match_payment_date, nearby_match_payment_date),
    'matching_method', coalesce(exact_match_method, nearby_match_method),
    'matching_reference', coalesce(nullif(exact_match_reference, ''), nullif(nearby_match_reference, '')),
    'matching_receipt', coalesce(nullif(exact_match_receipt, ''), nullif(nearby_match_receipt, ''))
  ) order by created_at desc), '[]'::jsonb)
  from bripta_unresolved_duplicate_audit
  where classification in (
    'PROBABLE_MANUAL_DUPLICATE_EXACT_DAY',
    'PROBABLE_MANUAL_DUPLICATE_WITHIN_ONE_DAY'
  )

  union all

  select 3, 'payments_with_no_ledger_match', coalesce(jsonb_agg(jsonb_build_object(
    'reference', mpesa_reference,
    'amount', amount,
    'received_at', created_at,
    'client', full_name,
    'phone', phone,
    'loan_id', loan_id,
    'loan_no', loan_no,
    'disbursement_date', disbursement_date,
    'maturity_date', maturity_date,
    'current_loan_status', current_loan_status,
    'classification', classification
  ) order by created_at desc), '[]'::jsonb)
  from bripta_unresolved_duplicate_audit
  where classification = 'NO_LEDGER_MATCH_REQUIRES_REVIEW'

  union all

  select 4, 'ambiguous_or_no_loan_do_not_post', coalesce(jsonb_agg(jsonb_build_object(
    'reference', mpesa_reference,
    'amount', amount,
    'received_at', created_at,
    'client', full_name,
    'phone', phone,
    'loan_no', loan_no,
    'classification', classification,
    'same_day_candidates', same_day_candidates,
    'nearby_candidates', nearby_candidates
  ) order by created_at desc), '[]'::jsonb)
  from bripta_unresolved_duplicate_audit
  where classification like 'MULTIPLE_%' or classification = 'NO_EARLIER_LOAN'
)
select * from sections order by section_order;
