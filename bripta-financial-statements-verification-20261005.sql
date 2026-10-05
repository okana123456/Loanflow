-- Bripta only, read-only. No accounting postings or source amount changes.
select 'financial_statements_installed' as check_name,
  (to_regprocedure('public.bripta_financial_statements(date,date,uuid)') is not null)::text as result
union all
select 'approved_expenses_this_month',coalesce(sum(amount),0)::text
from public.bripta_expenses
where business_id='BIZ-B3F5E5D9' and status in ('approved','paid')
  and expense_date>=date_trunc('month',now() at time zone 'Africa/Nairobi')::date
  and expense_date<(date_trunc('month',now() at time zone 'Africa/Nairobi')+interval '1 month')::date
union all
select 'recorded_journal_entries',count(*)::text from public.bripta_accounting_entries where business_id='BIZ-B3F5E5D9'
union all
select 'recorded_journal_debits_less_credits',coalesce(sum(debit-credit),0)::text
from public.bripta_accounting_entries where business_id='BIZ-B3F5E5D9';
-- installed should be true. Expense totals reflect existing approved records.
-- Journal difference should be zero for a balanced ledger; this does not prove
-- historical coverage or opening balances are complete. The screen flags gaps.
