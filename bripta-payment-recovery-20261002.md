# Bripta payment recovery — 2 October 2026

Scope: Supabase `nngscmpsxtqqjzcnsrbi`, business `BIZ-B3F5E5D9` only.

The owner confirmed that the Paybill account is the borrower's registered phone number. The callback had instead matched National IDs. The reviewed batch contains 21 payments for KES 7,985. The owner explicitly reserved UJ2558DDTO (KES 300, payer George/account candidate Joseph Bhoke Colleta) for Suspense. The recovery manifest contains the other 20 payments, totalling KES 7,685.

## Deployment order

1. In this Supabase project's SQL Editor, run the complete `bripta-restore-phone-account-payments-20261002.sql` file. Continue only if it succeeds. It creates the restricted phone lookup, persists the review hold, restores the listed payments and queues their missing SMS.
2. Copy the latest contents of `supabase/functions/payment-callback/index.ts` into the existing `payment-callback` Edge Function and deploy again. The previously deployed version does not contain this phone-account correction. Keep its existing settings/secrets; no new secrets are required.
3. Run `bripta-verify-phone-payment-recovery-20261002.sql`. Expected: 20 recovered payments, KES 7,685, zero integrity errors, zero missing SMS records, zero repayments for the held transaction and KES 300 held in Suspense. SMS statuses may initially be `queued`; verify `sent` after the existing SMS sender processes them. If needed, use the SMS Centre's existing process-queued action. Do not reset sent/unknown-delivery records or wallet credits.
4. Inspect `other_pending_callbacks`. It counts callbacks received since the incident that are outside this reviewed batch. A nonzero result requires reconciling those records too; the migration does not guess their allocation.

The SQL is one transaction and can be rerun. Changed client/loan matches, unexplained later repayments, unexpected trigger changes, or insufficient schedule amounts stop the recovery and roll it back. Preserve the error and inspect the affected payment rather than disabling these checks.

The reviewed Millicent KES 270 payment `UJ1DD89K9I` was actually received at 18:05 UTC on 1 October, before the missing KES 50 `UJ1DD89X72` at 20:40 UTC. Its stored payment date incorrectly used 21:05 UTC for the Kenya local time. The recovery recognises this earlier payment only when a separate confirmed callback verifies its reference, amount and receipt time within five minutes of the corrected timestamp. It preserves that historical repayment and its sent SMS. Unexplained later or newly backdated records still block recovery. Earlier payments already restored by this audited batch are excluded from that newly-created-record check.

### Reviewed schedule discrepancy

Emily's loan 222148 has balance KES 719.50 and recorded payments KES 6,995, but its three schedules total KES 4,265 and are all paid. One has date `36238-08-03` and instalment number `1785103`. The stored total payable also differs from paid plus balance. These figures cannot establish a trustworthy replacement schedule.

The recovery therefore has a narrowly guarded exception for `UJ20L8DGO8` only. It verifies the exact reported loan, three schedule IDs/totals, malformed row and 19 historical repayments before crediting KES 520 against the accepted balance. Result: balance and arrears KES 199.50; total paid KES 7,515. Existing loan terms, schedule rows and overdue days are preserved. The audit and repayment note record KES 520 of schedule allocation pending review. This is already a loan repayment, not an additional charge or a second amount in Suspense. `schedule_allocation_pending_review=520` is expected for this batch and `emily_balance_after_recovery=199.50` confirms its result. Unknown schedule shortfalls still abort the transaction. Historical schedule reconstruction remains outstanding.

Risper's schedules also differ from its accepted loan balance, but have enough unpaid capacity for the KES 300 payment. The recovery increments its existing schedule payments by KES 300 and reduces its existing loan balance by KES 300 to KES 7,147.50; it does not replace the baseline with schedule-derived totals.

Original M-Pesa times are interpreted in Africa/Nairobi. Callback arrival time is used only when a transaction timestamp is absent. Before/after snapshots are retained in `bripta_payment_recovery_20261002`, accessible through privileged database access. Repayments and schedule payments increase by only the recovered amounts; original loan principal, total payable, interest and fees are preserved. Existing SMS records are not resent by the recovery.

## Local verification

The tests use disposable synthetic PostgreSQL databases through PGlite and a simulated Supabase client for the Edge Function. They do not access production.

```powershell
npm.cmd install --prefix .recovery-test-runtime --ignore-scripts --package-lock=false @electric-sql/pglite@0.3.14
node tests/bripta-payment-recovery.test.mjs
node tests/bripta-payment-callback.test.mjs
node --experimental-strip-types --check supabase/functions/payment-callback/index.ts
git diff --check
```

Covered: exact repayment totals, original dates, schedules, existing balance triggers, safe repeat runs, queued versus previously sent SMS, review holds, shared/invalid phone references, isolation from other businesses, and rollback on changed amounts, missing callbacks, insufficient schedules or unexpected balance changes. The live database must still pass the migration's guards and verification queries.
