# Bripta documents → private Backblaze B2 bucket

Scope: Bripta only (Supabase `nngscmpsxtqqjzcnsrbi`, business `BIZ-B3F5E5D9`). Nyilora, the POS, other projects and `client-photos` are untouched.

**Why Backblaze B2 rather than Google Cloud:** Google Cloud Storage cannot be enabled without a payment card on file. B2 gives 10 GB free without a card, and it stops at the free limits rather than billing. The bucket is private and is reached through its S3-compatible API with short-lived signed links.

## Preflight result (22 Nov 2026, read-only)

* 543 linked documents, 1,059,725,272 bytes; 544 rows, of which 1 is broken.
* 15 unlinked objects, 30,087,091 bytes. That is the 14 previously reviewed (29,891,618 bytes) plus **1 staged copy** (`5afe3fad-e5e2-409c-88ba-6d9c5fe36aa0`, 195,473 bytes, guarantor passport) left by the failed optimizer run on 22 Nov. Its original is still linked. The optimizer ledger is empty, so it switched no documents. No earlier compression cleanup is pending.
* Known broken link: client `583310a7-3834-4b6e-8c99-a9d2d993f681`, `client_id_front`. The object is missing and there is no other file in that slot. It is tracked separately and the migration does not touch it.
* The live access, validation and audit functions and the storage policies match `bripta-client-documents-20261005.sql`.
* After migration and approved cleanup, Supabase would hold only `client-photos`: 7,391,743 bytes, about 7 MiB.

## What changed in code (branch `bripta/gcs-documents`)

| Path | Before | After |
|---|---|---|
| Upload / replace | Browser uploads to Supabase Storage | The `bripta-documents` Edge Function checks access (the live rules, as the signed-in staff member), file type by content, 8 MB limit, SHA-256 and duplicates. It uploads to B2: B2 verifies the signed SHA-256 of the body, and the function confirms size, checksum and version. It switches the row with an optimistic-concurrency check. A replaced B2 object is deleted only after the switch. A replaced Supabase original is queued for approved cleanup. |
| Preview | Browser signs a Supabase URL | The function returns a 60-second signed link (B2, or Supabase for not-yet-migrated rows) |
| Delete client | Ignored errors; failed when documents existed | Admin-only, no loans, done in one transaction, then the objects are removed via the APIs |
| Fallback | — | Until the function is deployed, the browser keeps using Supabase |
| Old optimizer | Workflow present | Workflow removed. It failed because the validation trigger rejects service-role writes; the new trigger has a trusted service-role branch. |

B2 object keys are `bripta/docs/v1/<random-uuid>`: no names, IDs or phone numbers. Logs hold only counts, numeric ledger ids and error codes. Document bytes never touch a developer computer or a GitHub artifact.

## Keys (least privilege)

Create two B2 application keys, both restricted to this one bucket. One is for the Supabase function (Supabase secrets) and one for the GitHub migration job (GitHub secrets). Neither is ever committed or printed. The B2 web console cannot create a key that can write but not delete, so both keys are *Read and Write*. Deletion in code is always version-exact.

| Where | Name | Value |
|---|---|---|
| Supabase → Edge Functions → Secrets | `BRIPTA_B2_ENDPOINT`, `BRIPTA_B2_BUCKET`, `BRIPTA_B2_KEY_ID`, `BRIPTA_B2_APP_KEY` | key 1 |
| GitHub → Settings → Secrets and variables → Actions → **Variables** | `BRIPTA_B2_ENDPOINT`, `BRIPTA_B2_BUCKET` | — |
| GitHub → … → **Secrets** | `BRIPTA_B2_MIGRATOR_KEY_ID`, `BRIPTA_B2_MIGRATOR_APP_KEY` | key 2 |

## Order of operations

1. ✅ Preflight (`bripta-documents-preflight-audit-20261122.sql`).
2. Create the B2 bucket: Private, encryption on.
3. Create the two keys and set the secrets and variables above.
4. Deploy the function: `npx supabase functions deploy bripta-documents --project-ref nngscmpsxtqqjzcnsrbi`.
5. Run `bripta-b2-documents-20261122.sql`. It only adds things, is safe to run twice, and captures the bucket baseline.
6. Merge the branch. The website now uses the function, and the workflow becomes runnable.
7. Pilot: Actions → *Bripta documents to Backblaze B2* → `migrate`, `5`. Then run `bripta-b2-status-20261122.sql`. Check that staff open the five documents and that an officer from another branch cannot.
8. After a staff upload shows as B2 in the status query, run `bripta-b2-stop-supabase-uploads-20261122.sql`.
9. Full run: `migrate`, `600`. Rerun until no rows are `claimed`, `copied` or `failed`.
10. Run `bripta-b2-cleanup-manifest-20261122.sql`. Review it, then approve each category by its digest.
11. Run `cleanup`, then `bripta-b2-status-20261122.sql`. Target: under 150 MiB, no new broken links.

## Rollback

* Before cleanup: `bripta-b2-documents-rollback-20261122.sql` points migrated rows back to their Supabase originals.
* Undo step 8: re-run `bripta-client-documents-20261005.sql`.
* Website: revert the merge. Old code still reads the Supabase originals until they are deleted.
