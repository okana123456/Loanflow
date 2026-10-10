# Bripta rollover penalty record visibility

1. Run `bripta-penalty-record-reader-20261010.sql` only in Supabase project `nngscmpsxtqqjzcnsrbi`. Expected output: `penalty_record_reader_installed = true`. It is safe to rerun.
2. Commit and push the changed website and new SQL/test/documentation files in this Loanflow repository through GitHub Desktop. Refresh Bripta after the deployment finishes.
3. Open a loan known to have rolled over. In its loan details, select **Rollover Penalties**. The amount charged, charge date, reason and applied/waived status are displayed. The same record is shown above the client's payment history and included in both existing PDF statement paths.

No Edge Functions or secrets need changing. Do not rerun the old penalty-repair migration for this display fix.

The four record/statement views now use a read-only, authenticated penalty reader. It checks the parent loan's business, branch and officer ownership. Older penalties with missing penalty branch metadata remain visible through their authorized parent loan. Admins can read Bripta loans across branches; branch managers and cashiers are limited to their branch; officers to their loans in their branch. Waived records remain visible with their original amount and date. Read errors are displayed rather than silently represented as an empty history.

The display uses `date_charged`, falling back to `created_at` where available. If neither date exists, it shows **Date not recorded**. If no penalty record exists, it reports that explicitly. The fix does not infer missing charges, apply a rollover, change financial totals or modify the one-time penalty rule.

Tests used synthetic PostgreSQL data with direct penalty reads denied. They exercised role/branch/business/portfolio restrictions, missing penalty branch metadata, waived history, repeated SQL installation and unchanged amounts/loan balances. Actual loan and client payment-history renderers were tested for amount/date visibility, explicit errors and missing-date/empty-history states. Existing dashboard/reporting tests passed. Live records need checking after deployment.
