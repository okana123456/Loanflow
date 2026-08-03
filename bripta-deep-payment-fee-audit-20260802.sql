-- Bripta / Loanflow deep payment, balance and fee audit
-- READ ONLY: this does not update, delete, match or recalculate business records.
-- Scope: Brian Obanda Ochieng's business, BIZ-B3F5E5D9.
-- Paste the entire file into the Bripta Supabase SQL Editor and run it once.

drop table if exists pg_temp.bripta_audit_results;
create temporary table bripta_audit_results (
  section_order integer,
  section text,
  result jsonb
) on commit preserve rows;

-- 1. Confirm the audited business and its current record volumes.
insert into bripta_audit_results
select 1, '01_business_scope', jsonb_build_object(
  'business_id', 'BIZ-B3F5E5D9',
  'staff', (select count(*) from public.loan_staff where business_id = 'BIZ-B3F5E5D9'),
  'clients', (select count(*) from public.loan_clients where business_id = 'BIZ-B3F5E5D9'),
  'loans', (select count(*) from public.loans where business_id = 'BIZ-B3F5E5D9'),
  'repayments', (select count(*) from public.loan_repayments where business_id = 'BIZ-B3F5E5D9'),
  'schedules', (select count(*) from public.loan_schedules where business_id = 'BIZ-B3F5E5D9')
);

-- 2. Nancy's loan as currently stored.
insert into bripta_audit_results
select 2, '02_nancy_loan_385693_current_record',
  coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
from (
  select
    l.id as loan_id,
    c.full_name,
    c.phone,
    l.loan_no,
    l.status,
    l.principal_amount,
    l.total_interest,
    l.total_payable,
    l.total_paid as stored_total_paid,
    l.outstanding_balance as stored_balance,
    l.processing_fee,
    l.processing_fee_paid,
    l.registration_fee_due,
    l.registration_fee_paid,
    l.disbursement_date,
    l.created_at
  from public.loans l
  join public.loan_clients c on c.id::text = l.client_id::text
  where l.business_id = 'BIZ-B3F5E5D9'
    and (l.loan_no = '385693' or regexp_replace(coalesce(c.phone,''), '\D', '', 'g') like '%707707311')
  order by l.created_at
) x;

-- 3. Every payment recorded against Nancy's loan, regardless of source.
insert into bripta_audit_results
select 3, '03_nancy_loan_385693_complete_payment_ledger',
  coalesce(jsonb_agg(to_jsonb(x) order by x.payment_date, x.created_at, x.repayment_id), '[]'::jsonb)
from (
  select
    r.id as repayment_id,
    r.payment_date,
    r.created_at,
    r.receipt_no,
    r.payment_reference,
    r.payment_method,
    r.amount as gross_amount,
    coalesce(r.loan_portion, r.amount) as loan_portion,
    coalesce(r.principal_portion, 0) as principal_portion,
    coalesce(r.interest_portion, 0) as interest_portion,
    coalesce(r.penalty_portion, 0) as penalty_portion,
    coalesce(r.registration_fee_portion, 0) as registration_fee_portion,
    coalesce(r.processing_fee_portion, 0) as processing_fee_portion,
    coalesce(r.credit_portion, 0) as credit_portion,
    r.mpesa_confirmed,
    r.notes
  from public.loan_repayments r
  join public.loans l on l.id::text = r.loan_id::text
  where l.business_id = 'BIZ-B3F5E5D9'
    and l.loan_no = '385693'
) x;

-- 4. Nancy reconciliation: stored loan figures versus the complete ledger and schedule.
insert into bripta_audit_results
select 4, '04_nancy_loan_385693_reconciliation', to_jsonb(x)
from (
  select
    l.loan_no,
    l.total_payable,
    l.total_paid as stored_total_paid,
    coalesce(r.gross_repayment_total, 0) as gross_repayment_total,
    coalesce(r.loan_portion_total, 0) as loan_portion_total,
    l.outstanding_balance as stored_balance,
    greatest(0, coalesce(l.total_payable,0) - coalesce(r.gross_repayment_total,0)) as expected_balance_from_gross_ledger,
    coalesce(s.schedule_due_total, 0) as schedule_due_total,
    coalesce(s.schedule_paid_total, 0) as schedule_paid_total,
    round(coalesce(l.total_paid,0) - coalesce(r.gross_repayment_total,0), 2) as stored_paid_minus_ledger,
    round(coalesce(l.outstanding_balance,0) - greatest(0,coalesce(l.total_payable,0)-coalesce(r.gross_repayment_total,0)), 2) as stored_balance_minus_expected
  from public.loans l
  left join (
    select loan_id,
      round(sum(coalesce(amount,0))::numeric,2) as gross_repayment_total,
      round(sum(coalesce(loan_portion,amount,0))::numeric,2) as loan_portion_total
    from public.loan_repayments group by loan_id
  ) r on r.loan_id::text = l.id::text
  left join (
    select loan_id,
      round(sum(coalesce(total_due,0))::numeric,2) as schedule_due_total,
      round(sum(coalesce(total_paid,0))::numeric,2) as schedule_paid_total
    from public.loan_schedules group by loan_id
  ) s on s.loan_id::text = l.id::text
  where l.business_id = 'BIZ-B3F5E5D9' and l.loan_no = '385693'
) x;

-- 5. Nancy's complete installment schedule.
insert into bripta_audit_results
select 5, '05_nancy_loan_385693_schedule',
  coalesce(jsonb_agg(to_jsonb(x) order by x.installment_no), '[]'::jsonb)
from (
  select s.id, s.installment_no, s.due_date, s.total_due, s.total_paid,
         greatest(0,coalesce(s.total_due,0)-coalesce(s.total_paid,0)) as remaining,
         s.status, s.paid_at
  from public.loan_schedules s
  join public.loans l on l.id::text = s.loan_id::text
  where l.business_id = 'BIZ-B3F5E5D9' and l.loan_no = '385693'
) x;

-- 6. Show the before/after backup history for Nancy, which can explain a rollback drift.
insert into bripta_audit_results
select 6, '06_nancy_balance_backup_history',
  coalesce(jsonb_agg(to_jsonb(x) order by x.snapshot), '[]'::jsonb)
from (
  select '31 July balance-fix backup'::text as snapshot, b.total_paid, b.outstanding_balance,
         b.total_payable, b.status, b.ledger_repayment_total, b.ledger_expected_balance,
         b.backed_up_at as snapshot_at
  from public.bripta_balance_fix_backup_20260731 b
  where b.business_id = 'BIZ-B3F5E5D9' and b.loan_no = '385693'
  union all
  select '02 August safe-rollback backup', b.total_paid, b.outstanding_balance,
         b.total_payable, b.status, null::numeric, null::numeric,
         b.rollback_backed_up_at
  from public.bripta_safe_rollback_loans_20260802 b
  where b.business_id = 'BIZ-B3F5E5D9' and b.loan_no = '385693'
) x;

-- 7. All business loans whose stored paid/balance differs from their repayment ledger.
insert into bripta_audit_results
select 7, '07_all_loan_balance_mismatches',
  coalesce(jsonb_agg(to_jsonb(x) order by abs(x.stored_paid_minus_ledger) desc, x.full_name), '[]'::jsonb)
from (
  select
    l.id as loan_id, c.full_name, c.phone, l.loan_no, l.status,
    l.total_payable, l.total_paid as stored_total_paid,
    coalesce(r.repayment_total,0) as repayment_total,
    l.outstanding_balance as stored_balance,
    greatest(0,coalesce(l.total_payable,0)-coalesce(r.repayment_total,0)) as ledger_expected_balance,
    round(coalesce(l.total_paid,0)-coalesce(r.repayment_total,0),2) as stored_paid_minus_ledger,
    round(coalesce(l.outstanding_balance,0)-greatest(0,coalesce(l.total_payable,0)-coalesce(r.repayment_total,0)),2) as stored_balance_minus_expected
  from public.loans l
  join public.loan_clients c on c.id::text = l.client_id::text
  left join (
    select loan_id, round(sum(coalesce(amount,0))::numeric,2) repayment_total
    from public.loan_repayments group by loan_id
  ) r on r.loan_id::text = l.id::text
  where l.business_id = 'BIZ-B3F5E5D9'
    and (
      abs(coalesce(l.total_paid,0)-coalesce(r.repayment_total,0)) > 0.01
      or abs(coalesce(l.outstanding_balance,0)-greatest(0,coalesce(l.total_payable,0)-coalesce(r.repayment_total,0))) > 0.01
    )
  limit 500
) x;

-- 8. Payment totals by method, preserving manual, matched, imported and Daraja sources.
insert into bripta_audit_results
select 8, '08_payment_sources_all_time',
  coalesce(jsonb_agg(to_jsonb(x) order by x.total_amount desc), '[]'::jsonb)
from (
  select coalesce(nullif(trim(payment_method),''),'(blank)') as payment_method,
         count(*) as payment_rows,
         round(sum(coalesce(amount,0))::numeric,2) as total_amount,
         min(payment_date) as first_payment,
         max(payment_date) as latest_payment
  from public.loan_repayments
  where business_id = 'BIZ-B3F5E5D9'
  group by coalesce(nullif(trim(payment_method),''),'(blank)')
) x;

-- 9. Duplicate receipts/references that could overstate total paid.
insert into bripta_audit_results
select 9, '09_duplicate_repayment_references',
  coalesce(jsonb_agg(to_jsonb(x) order by x.occurrences desc, x.reference), '[]'::jsonb)
from (
  select upper(trim(reference)) as reference,
         count(*) as occurrences,
         round(sum(amount)::numeric,2) as combined_amount,
         string_agg(distinct loan_no, ', ') as loan_numbers,
         string_agg(distinct payment_method, ', ') as methods
  from (
    select coalesce(nullif(r.payment_reference,''),nullif(r.receipt_no,'')) as reference,
           r.amount, l.loan_no, coalesce(r.payment_method,'(blank)') payment_method
    from public.loan_repayments r
    join public.loans l on l.id::text = r.loan_id::text
    where r.business_id = 'BIZ-B3F5E5D9'
  ) p
  where nullif(trim(reference),'') is not null and upper(trim(reference)) <> 'IMPORT'
  group by upper(trim(reference))
  having count(*) > 1
) x;

-- 10. Loans disbursed from 1 August onward and the fee fields accounting reads.
insert into bripta_audit_results
select 10, '10_loans_disbursed_from_01_august_2026',
  coalesce(jsonb_agg(to_jsonb(x) order by x.disbursement_date, x.created_at), '[]'::jsonb)
from (
  select l.id as loan_id, c.full_name, c.phone, l.loan_no, l.loan_type,
         l.status, l.principal_amount, l.disbursed_amount, l.total_payable,
         l.processing_fee, l.processing_fee_paid,
         l.registration_fee_due, l.registration_fee_paid,
         l.total_paid, l.outstanding_balance, l.disbursement_date, l.created_at
  from public.loans l
  join public.loan_clients c on c.id::text = l.client_id::text
  where l.business_id = 'BIZ-B3F5E5D9'
    and l.disbursement_date >= '2026-08-01'::date
) x;

-- 11. Payments from 1 August onward, including all stored fee portions.
insert into bripta_audit_results
select 11, '11_payments_from_01_august_2026',
  coalesce(jsonb_agg(to_jsonb(x) order by x.payment_date, x.created_at), '[]'::jsonb)
from (
  select c.full_name, c.phone, l.loan_no, r.id as repayment_id,
         r.receipt_no, r.payment_reference, r.payment_method,
         r.amount, coalesce(r.loan_portion,r.amount) loan_portion,
         coalesce(r.registration_fee_portion,0) registration_fee_portion,
         coalesce(r.processing_fee_portion,0) processing_fee_portion,
         coalesce(r.penalty_portion,0) penalty_portion,
         r.payment_date, r.created_at, r.notes
  from public.loan_repayments r
  join public.loans l on l.id::text = r.loan_id::text
  join public.loan_clients c on c.id::text = l.client_id::text
  where r.business_id = 'BIZ-B3F5E5D9'
    and r.payment_date >= '2026-08-01'::date
) x;

-- 12. Why Accounting currently shows zero or non-zero fees.
insert into bripta_audit_results
select 12, '12_fee_accounting_totals', jsonb_build_object(
  'loan_processing_fee_configured_all_time', (select round(coalesce(sum(processing_fee),0)::numeric,2) from public.loans where business_id='BIZ-B3F5E5D9'),
  'loan_processing_fee_marked_paid_all_time', (select round(coalesce(sum(processing_fee_paid),0)::numeric,2) from public.loans where business_id='BIZ-B3F5E5D9'),
  'repayment_processing_fee_portions_all_time', (select round(coalesce(sum(processing_fee_portion),0)::numeric,2) from public.loan_repayments where business_id='BIZ-B3F5E5D9'),
  'loan_registration_fee_due_all_time', (select round(coalesce(sum(registration_fee_due),0)::numeric,2) from public.loans where business_id='BIZ-B3F5E5D9'),
  'loan_registration_fee_marked_paid_all_time', (select round(coalesce(sum(registration_fee_paid),0)::numeric,2) from public.loans where business_id='BIZ-B3F5E5D9'),
  'repayment_registration_fee_portions_all_time', (select round(coalesce(sum(registration_fee_portion),0)::numeric,2) from public.loan_repayments where business_id='BIZ-B3F5E5D9'),
  'clients_registered_all_time', (select count(*) from public.loan_clients where business_id='BIZ-B3F5E5D9'),
  'nominal_registration_value_at_200_each', (select count(*) * 200 from public.loan_clients where business_id='BIZ-B3F5E5D9'),
  'loans_from_august', (select count(*) from public.loans where business_id='BIZ-B3F5E5D9' and disbursement_date >= '2026-08-01'::date),
  'processing_fee_configured_from_august', (select round(coalesce(sum(processing_fee),0)::numeric,2) from public.loans where business_id='BIZ-B3F5E5D9' and disbursement_date >= '2026-08-01'::date),
  'processing_fee_received_from_august', (select round(coalesce(sum(processing_fee_portion),0)::numeric,2) from public.loan_repayments where business_id='BIZ-B3F5E5D9' and payment_date >= '2026-08-01'::date)
);

-- 13. Callback payments for this business not represented by a repayment row.
insert into bripta_audit_results
select 13, '13_callback_payments_without_repayment',
  coalesce(jsonb_agg(to_jsonb(x) order by x.created_at desc), '[]'::jsonb)
from (
  select q.id, q.trans_id, q.trans_amount, q.msisdn, q.bill_ref_number,
         q.confirmed, q.dismissed, q.loan_id, q.repayment_id, q.created_at
  from public.mpesa_callback_queue q
  where q.business_short_code = 'BIZ-B3F5E5D9'
    and coalesce(q.dismissed,false)=false
    and not exists (
      select 1 from public.loan_repayments r
      where r.business_id='BIZ-B3F5E5D9'
        and (upper(trim(coalesce(r.payment_reference,'')))=upper(trim(coalesce(q.trans_id,'')))
          or upper(trim(coalesce(r.receipt_no,'')))=upper(trim(coalesce(q.trans_id,''))))
    )
) x;

-- 14. Unresolved manual/Daraja suspense entries.
insert into bripta_audit_results
select 14, '14_unresolved_suspense',
  coalesce(jsonb_agg(to_jsonb(x) order by x.created_at desc), '[]'::jsonb)
from (
  select id, mpesa_reference, amount, payer_phone, account_number, payer_name,
         resolved, dismissed, created_at
  from public.unmatched_payments
  where business_id='BIZ-B3F5E5D9'
    and coalesce(resolved,false)=false
    and coalesce(dismissed,false)=false
) x;

-- 15. Confirm whether any automatic repayment trigger is still rewriting balances.
insert into bripta_audit_results
select 15, '15_repayment_table_triggers',
  coalesce(jsonb_agg(jsonb_build_object(
    'trigger_name', t.tgname,
    'enabled', t.tgenabled,
    'definition', pg_get_triggerdef(t.oid)
  )), '[]'::jsonb)
from pg_trigger t
join pg_class c on c.oid=t.tgrelid
join pg_namespace n on n.oid=c.relnamespace
where n.nspname='public' and c.relname='loan_repayments' and not t.tgisinternal;

select section_order, section, result
from bripta_audit_results
order by section_order;
