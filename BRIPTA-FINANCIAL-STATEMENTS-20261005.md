# Bripta Accounting statements

Deploy only to Loanflow and Supabase project `nngscmpsxtqqjzcnsrbi`, business `BIZ-B3F5E5D9`.

1. Run the complete contents of `bripta-financial-statements-20261005.sql`. It installs a read-only report function with authenticated accounting access and branch boundaries. It is safe to rerun. It does not post, delete or recalculate any historical financial record.
2. Commit and push the website changes through GitHub Desktop, including `index.html` and `bripta-financial-statements.js`. After deployment, refresh Accounting.
3. Run `bripta-financial-statements-verification-20261005.sql` and check Accounting for All Branches, Migori, Yesterday, This Week and a known Custom range.

No Edge Functions need redeploying and no new secrets are required.

Profit & Loss keeps the existing registration, processing, collected interest and collected penalty figures. Net profit/loss now subtracts approved or paid expense records once, using expense date and the selected branch/date range. Journal copies of these expenses are not subtracted again. Pending, draft, rejected and cancelled expenses are excluded. Principal disbursements, principal recoveries, suspense receipts and owner capital are not added as profit.

The balance sheet uses cumulative recorded journal balances through the selected end date. It shows assets, liabilities, owner equity and retained profit/loss from those postings. Historical income in the original source-based P&L may differ from journal retained profit if historical postings are missing. The screen marks missing opening balances, source records without journal postings, unbalanced transactions and any balance-sheet difference. As requested, missing balances are shown for review rather than estimated or automatically backfilled. Opening bank/cash balances and incomplete historical journals still need verified records before the balance sheet can represent the complete business position. This release does not infer depreciation or revalue assets from the editable asset register.

Local tests ran both SQL installations twice against synthetic PostgreSQL data. They exercised expenses, cumulative balances, date cutoffs, all-branch totals, manager branch restrictions, explicit accounting permissions, other-business/anonymous denial, preservation of source amounts, the actual Accounting renderer, profit/loss and missing-data states. Desktop and phone preview checks used sample figures; live results must be checked after deployment.
