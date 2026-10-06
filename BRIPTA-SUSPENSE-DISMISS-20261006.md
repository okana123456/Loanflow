# Restore Suspense dismiss actions

Apply only in Bripta's Supabase project `nngscmpsxtqqjzcnsrbi` and Loanflow repository.

1. Run the complete `bripta-suspense-dismiss-20261006.sql` file. Expected final output: `suspense_dismiss_installed = true`. The SQL can be rerun safely and needs the previously installed Suspense reader setup.
2. Commit and push the changed `index.html` and new SQL/test/documentation files through GitHub Desktop. Wait for the website deployment and reload Bripta.
3. Try Dismiss on a pending queue payment, a manual entry and a bulk selection. Each successful action removes the item from the pending list. Officers retain view-only access. Managers and cashiers can dismiss within their branch; admins can dismiss across Bripta branches.

No Edge Functions or secrets need changing. Do not rerun the older balance-repair SQL to enable dismissal.

The buttons now use `bripta_dismiss_suspense`, a checked database action. It authenticates the user, checks business/branch access, locks the selected rows, records dismissal and writes the audit event in one transaction. It checks the returned result before showing success. Matching fallback copies of a receipt are dismissed together. An already matched, confirmed or repaid receipt is rejected; a repeated dismissal is harmless. A bulk selection containing an invalid record rolls back entirely.

Dismissed queue receipts retain the existing terminal `confirmed=true` flag to stop callback retries from recording them as repayments, with `dismissed=true` distinguishing dismissal from payment confirmation. Manual entries receive a dismissal flag without being falsely marked as matched. Original amounts and records are retained; no repayments, SMS or journal postings are created.

Local PostgreSQL tests exercised branch/business restrictions, admin/manager/cashier permissions, officer/anonymous/disabled-staff denial, single/manual/bulk actions, direct-table RLS denial, matching fallback copies, repeated actions, confirmed-payment protection, audit events and rollback. Actual frontend action tests checked refresh after success and accurate error handling. Receipt amounts, loan balances and repayment totals were unchanged in the test fixture. Production behavior needs confirmation after deployment.
