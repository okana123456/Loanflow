-- Bripta only: help officer repayment reads find the assigned loan rows
-- before the database checks branch and officer access on each result.
-- Adds a partial index; does not change any payment or accounting record.
-- Safe to run again.
create index if not exists bripta_repayments_branch_loan_read_idx
on public.loan_repayments (branch_id, loan_id, created_at)
where business_id = 'BIZ-B3F5E5D9';

-- Expected: one row with index_present = true.
select exists (
  select 1 from pg_indexes
  where schemaname = 'public' and tablename = 'loan_repayments'
    and indexname = 'bripta_repayments_branch_loan_read_idx'
) as index_present;
