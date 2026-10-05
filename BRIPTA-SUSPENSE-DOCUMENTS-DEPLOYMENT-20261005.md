# Bripta Suspense, Accounting filters and onboarding documents

Apply only to Supabase project `nngscmpsxtqqjzcnsrbi` and the Loanflow repository. Business: `BIZ-B3F5E5D9`.

## Deploy in this order

1. Run the complete contents of `bripta-suspense-reader-20261005.sql` in the Supabase SQL Editor. It adds a protected, paginated Suspense reader and corrects missing/wrong branch metadata on pending Bripta payments. It also assigns future unmatched callbacks to the existing Head Office branch (currently Migori). It does not create repayments, resend SMS, or change amounts or balances.
2. Run `bripta-client-documents-20261005.sql`. It creates the private attachment bucket, document register, access policies and audit triggers. Both SQL files can be rerun safely. Do not rerun the original multibranch migration or recovery scripts.
3. Redeploy the existing **payment-callback** Edge Function using the full contents of `supabase/functions/payment-callback/index.ts`. Keep its existing secrets and endpoint/authentication configuration. This update explicitly marks new Bripta callbacks as not dismissed. No new secrets or Edge Functions are required.
4. In GitHub Desktop, commit and push the changes in this repository, including the new `bripta-client-documents.js` file and `index.html`. Wait for the website deployment, then reload the page.

## What changed

- Suspense now includes pending callbacks whose confirmation/dismissal flags are blank. Its reader supports the Bripta business code and its uniquely configured Paybill, enforces branch access, excludes already settled receipts and avoids double-counting queue/manual copies. Unknown or ambiguous accounts remain available for authorized review; they are not guessed or automatically applied to a loan. Officers retain view-only controls.
- Accounting's Yesterday, This Week and Custom controls now call the same original-record report used when opening Accounting. Previously they called the separate journal report, causing zero results. Financial calculations and source records are unchanged.
- Onboarding has six optional attachment slots: client passport, front/back ID, guarantor passport, front/back ID. Existing clients have a **Client & Guarantor Documents** button in their profile for viewing or replacing files. JPG, PNG and WEBP are supported; ID copies can also be PDF. Maximum 8 MB per file. New documents use private storage and short-lived viewing links. Existing client photos remain available.
- Admins can access Bripta documents; branch managers are limited to their branch; officers to their assigned clients/portfolio in their branch. Document uploads/replacements are audited. A failed upload can be retried without recreating the client.

## Verify after deployment

Run `bripta-suspense-documents-verification-20261005.sql` for installation and branch checks. Then:

1. As an admin, open Suspense with All Branches and Migori. Compare pending references against the callback register. A receipt already repaid should not appear as pending. Do not re-enter or replay old payments just to make them visible.
2. Confirm the next genuine unknown-account Paybill callback appears in Suspense without a loan repayment. An officer should see it as view-only in their branch.
3. In Accounting, select Yesterday, This Week and a Custom range known to contain repayments. Compare figures against the repayment register for the same Kenya dates and branch.
4. Attach all six documents to an authorized client. Open one, replace one and confirm another officer without that client's portfolio cannot access it. Test onboarding on phone and desktop. If an upload fails, use **Retry pending uploads** while the page remains open, or attach the file later from the saved client profile.

## Local validation completed

Synthetic PostgreSQL tests exercised both migrations twice, pending callback visibility, future branch assignment, settled exclusions, role/business isolation, private storage despite pre-existing broad storage policies, all six document types, audit events and unchanged financial totals. Frontend tests exercised period routing and Kenya date boundaries, paginated Suspense rendering, duplicate suppression, view-only controls, attachment validation and failed-upload retry. Existing officer dashboard/repayment, reporting, manual repayment and callback tests passed. No live payments, SMS or uploads were created during these tests. Production verification remains necessary after deployment.
