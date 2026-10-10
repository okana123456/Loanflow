# Charged penalty records in Accounting

1. Run `bripta-accounting-penalties-20261010.sql` in Bripta Supabase project `nngscmpsxtqqjzcnsrbi`. Expected result: `accounting_penalties_installed = true`. Safe to rerun.
2. Commit and push the changes, including `index.html` and `bripta-financial-statements.js`, through GitHub Desktop. Reload Accounting after deployment.
3. Choose This Month (October 2026) and Migori or All Branches. Find **Rollover Penalties Charged** below the financial statements. Hellen Akinyi Opiyo, loan 251959, should show KES 525 dated 10 October 2026 if that record remains in the database. A Custom range covering 10 October must also include it; dates/branches outside its scope must exclude it.

The section reads existing penalty records through their parent loans, with authenticated accounting permissions and branch restrictions. It shows the loan, client, amount charged, charge date, reason and applied/waived status. Applied and waived totals are separate. Missing penalty branch metadata does not hide an authorized parent loan's record. Read errors are explicit, and paging avoids silently incomplete totals.

Accounting's existing Penalties Collected amount still comes from repayments. Charged penalties are shown separately and are not added a second time to revenue or net profit. This release is read-only: it does not create journal postings, apply penalties, change balances, or alter the one-time rollover rule. No Edge Functions or secrets need changing.

Tests used synthetic PostgreSQL data for Hellen's KES 525/date example, period/branch exclusions, manager access, other-business denial, repeated installation, actual Accounting rendering, separate waived totals and unchanged profit calculations. Production checks remain necessary after deployment.
