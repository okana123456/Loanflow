# Bripta multi-branch finance upgrade deployment

This release is scoped to Supabase project `nngscmpsxtqqjzcnsrbi`. It discovers all populated business scopes in this project; legacy rows with no business identifier use the neutral `SYSTEM` scope.

## Deployment order

1. In the Bripta Supabase SQL Editor, run `bripta-multibranch-finance-upgrade-20261001.sql` once. It is idempotent and may be rerun safely. It backfills all current records to **Head Office** without changing financial amounts, statuses, schedules, repayments, or M-Pesa history.
2. Confirm every row in the verification result set. Missing-branch and unbalanced-source checks must return `0`; October pricing must return `3000`, and November pricing must return `7500`.
3. Redeploy these Edge Functions to the same Supabase project:
   - `start-service-payment`
   - `service-payment-callback`
   - `payment-callback`
4. Deploy the updated `index.html` through the existing Bripta hosting workflow.
5. Sign in as an administrator, create a test branch, assign a test staff member, and verify the branch selector. Then test a manager, loan officer, and cashier account before using the new branch in production.

## Secrets and settings

No new secret is required. Preserve the current Supabase URL/service-role secrets and all existing `SERVICE_*` Daraja/M-Pesa secrets. Do not set a client-side subscription amount: the Edge Functions and database derive KES 3,000 through 31 October 2026 and KES 7,500 from 1 November 2026.

## Operational checks

- Administrators can select All Branches; other users are locked to their assigned branch.
- Loan officers see only their assigned clients and portfolio.
- Expense approval creates balanced accounting entries once; rejection removes any related expense posting.
- Asset changes and transfers appear in the domain audit log.
- Portfolio transfers update clients, applications, and loans together.
- A billing request dated 31 October 2026 is KES 3,000; one dated 1 November 2026 is KES 7,500.
- Existing paid billing cycles remain unchanged.

If any verification check is unexpected, do not deploy the frontend or Edge Functions; retain the SQL output and investigate in the Bripta project only.
