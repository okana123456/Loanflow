# Bripta reporting update — 2 October 2026

Scope: Loanflow repository, Supabase project nngscmpsxtqqjzcnsrbi, business BIZ-B3F5E5D9 only.

## Deploy

1. Commit and push `index.html` and the new `bripta-reporting.js` together using GitHub Desktop. Both files must reach the website deployment.
2. Run `bripta-rename-head-office-migori-20261002.sql` in this project's SQL Editor. It renames the existing Head Office without creating a branch or moving records. It refuses an unexpected branch/business and can be rerun safely.
3. Reload the website, then use Refresh on Dashboard, Repayments and Officer Performance. Compare Juma Gad in both officer views using the same branch and latest data.
4. Run `bripta-verify-phone-payment-recovery-20261002.sql` and share the output. This is read-only and checks recovery records, payment links, remaining callbacks and SMS states. Do not rerun the recovery script merely to refresh the website.

This reporting update requires no Edge Function redeployment or new secrets. The previously deployed payment-callback change and previously supplied recovery SQL are separate from this reporting update.

## Changes

- Period unpaid uses oldest-due-first loan allocations, including late payments received by the period cutoff; payments after the cutoff are excluded. Completed loans remain in period dues. Fees and excess do not pay loan installments.
- Intact schedules are reconciled for display only. Malformed or restructured schedules retain their recorded allocations and show a warning. Historical imported data with no dated payment trail cannot provide a fully reconstructed past-period snapshot.
- Both officer views use canonical officer assignments and the same distinct-client arrears calculation. BQ is clean active clients divided by active clients. PAR retains the existing system definition: arrears amount divided by outstanding balance. Totals use combined numerators and denominators, not averages of percentages. Excel exports include them too.
- Due-today rows group by client ID, combine installments and loans, count balances once per loan, and keep a separate payment action for each loan.
- Audit users resolve to staff names through staff IDs or authentication IDs; missing historical users are labelled unknown and automated actions System.
- Restored payments use their original transaction date; Refresh requests a fresh incremental sync and reports sync failures instead of silently presenting stale data.

No loan balances, repayment amounts, schedule records, SMS balances or accounting entries are changed by the reporting code or branch rename.

## Payment recovery verification

Expected for the reviewed recovery batch: 20 repayments / KES 7,685, zero integrity errors and zero missing SMS outbox records. UJ2558DDTO / KES 300 remains held in suspense, as instructed. Inspect the returned SMS statuses; queued is not proof of sent. `other_pending_callbacks` should be zero or investigated individually. No live recovery or SMS delivery is asserted by local tests.

Emily's historical schedule discrepancy is separately flagged by the recovery verification; this update does not rewrite that history.

## Local checks

Run `node tests/bripta-reporting.test.cjs`, `node tests/bripta-payment-callback.test.mjs`, `node tests/bripta-migori-rename.test.mjs`, and `node tests/bripta-payment-recovery.test.mjs`.
The database tests use the local disposable PGlite runtime, not Supabase. Live officer figures and latest payment/SMS delivery still require the verification output and deployed UI check.
