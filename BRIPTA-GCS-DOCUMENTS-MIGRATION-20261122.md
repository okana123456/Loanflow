# Bripta documents → private Google Cloud Storage

Scope: Bripta only (Supabase `nngscmpsxtqqjzcnsrbi`, business `BIZ-B3F5E5D9`). Nyilora, the POS and other projects are untouched.
`client-photos` is left alone.

## What changed in code (branch `bripta/gcs-documents`)

| Path | Before | After |
|---|---|---|
| Upload / replace (`bripta-client-documents.js`) | Browser uploads to Supabase Storage, upserts row, deletes old object | Browser sends the file to the `bripta-documents` Edge Function. The function checks access, file type (magic bytes), size and SHA-256, and skips duplicates. It uploads to GCS create-only (CRC32C-checked by Google), then switches the row with an optimistic-concurrency check. It deletes the previous **GCS** object only after the switch commits. A replaced **Supabase** original is queued for the approved cleanup, never deleted inline. |
| Preview | Browser signs a Supabase URL directly (assumes Supabase) | Function returns a 60-second link: a V4-signed GCS URL, or a Supabase signed URL for not-yet-migrated rows |
| Permanent client delete (`index.html`) | Deleted `loan_clients` only. It failed for clients with documents, because of the foreign key, and the error was ignored | Admin-only, no-loans check, then rows and client deleted in one transaction. Objects are then removed via the GCS / Storage APIs. |
| Fallback | — | Until the function is deployed, the browser keeps using the old Supabase path, so the site works throughout |
| Old optimizer | GitHub workflow present | Workflow removed (not re-runnable). Root cause of its failure: the validation trigger requires a signed-in staff user, so a service-role link switch is always rejected. The new trigger has an explicit trusted service-role branch. |

Access rules are **not rewritten**. Server RPCs call the live `bripta_can_access_client_documents()` as the signed-in staff member. Admin sees all clients. A branch manager sees their own branch. A loan officer sees their own clients, or clients they have loans for, in their branch. Everyone else is refused.

GCS object keys are `bripta/docs/v1/<random-uuid>`: no names, IDs or phone numbers. Logs contain only counts, numeric ledger ids and error codes.

## Order of operations

1. **Preflight (read-only):** run `bripta-gcs-preflight-audit-20261122.sql` and send back the result. It shows the failed optimizer's ledger, any staged copy it left, the 14 unlinked files, the known broken link, and the live function definitions the migration builds on.
2. **Google Cloud setup (you):** see below.
3. **Supabase function secrets:** `BRIPTA_GCS_BUCKET`, `BRIPTA_GCS_SIGNER_KEY`. Then deploy the function:
   `npx supabase functions deploy bripta-documents --project-ref nngscmpsxtqqjzcnsrbi`
4. **GitHub → Settings → Secrets and variables → Actions → Variables:** `BRIPTA_GCS_BUCKET`, `BRIPTA_GCP_WIF_PROVIDER`, `BRIPTA_GCP_MIGRATOR_SA`. The existing secret `BRIPTA_SUPABASE_SERVICE_ROLE_KEY` is reused.
5. **Schema:** run `bripta-gcs-documents-20261122.sql` (additive, idempotent; it captures the bucket baseline).
6. **Website:** merge the branch. Uploads and previews now use the function.
7. **Pilot:** Actions → *Bripta documents to Google Cloud Storage* → mode `migrate`, max_files `5`. Then run `bripta-gcs-status-20261122.sql`. Then check that staff can open those five documents and that an officer from another branch cannot.
8. **Stop Supabase growth:** after one staff upload shows as GCS in the status query, run `bripta-gcs-stop-supabase-uploads-20261122.sql`.
9. **Full migration:** run `migrate` with max_files `600`. It is resumable, so rerun until the status query shows no `claimed`, `copied` or `failed` rows.
10. **Manifest:** run `bripta-gcs-cleanup-manifest-20261122.sql` and review it. Approval is per category, by digest. Nothing is deleted before this.
11. **Cleanup:** mode `cleanup` deletes only approved objects via the Storage API. It re-verifies each GCS copy first. Then run the status query (target: Supabase below 150 MiB, no new broken links).

## Google Cloud setup (Cloud Shell)

```bash
PROJECT=bripta-documents          # new dedicated project; link billing in the console
BUCKET=bripta-docs-$(openssl rand -hex 4)
REPO=okana123456/Loanflow
gcloud config set project $PROJECT
gcloud services enable storage.googleapis.com iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com
gcloud storage buckets create gs://$BUCKET --location=africa-south1 \
  --uniform-bucket-level-access --public-access-prevention --soft-delete-duration=7d
# Runtime signer (Edge Function): read/write/delete objects in this bucket only.
gcloud iam service-accounts create bripta-docs-signer
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
  --member=serviceAccount:bripta-docs-signer@$PROJECT.iam.gserviceaccount.com --role=roles/storage.objectUser
# Migration job (GitHub Actions, keyless): create + read only, no delete.
gcloud iam service-accounts create bripta-docs-migrator
for ROLE in roles/storage.objectCreator roles/storage.objectViewer; do
  gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
    --member=serviceAccount:bripta-docs-migrator@$PROJECT.iam.gserviceaccount.com --role=$ROLE; done
gcloud iam workload-identity-pools create github --location=global
gcloud iam workload-identity-pools providers create-oidc loanflow --location=global --workload-identity-pool=github \
  --issuer-uri=https://token.actions.githubusercontent.com \
  --attribute-mapping=google.subject=assertion.sub,attribute.repository=assertion.repository \
  --attribute-condition="assertion.repository=='$REPO'"
NUM=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
gcloud iam service-accounts add-iam-policy-binding bripta-docs-migrator@$PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member=principalSet://iam.googleapis.com/projects/$NUM/locations/global/workloadIdentityPools/github/attribute.repository/$REPO
echo "BRIPTA_GCS_BUCKET=$BUCKET"
echo "BRIPTA_GCP_WIF_PROVIDER=projects/$NUM/locations/global/workloadIdentityPools/github/providers/loanflow"
echo "BRIPTA_GCP_MIGRATOR_SA=bripta-docs-migrator@$PROJECT.iam.gserviceaccount.com"
```

**Signer key.** This is the one unavoidable key, because Supabase Edge Functions have no Google workload identity. Create it in Cloud Shell and put it straight into the Supabase secret `BRIPTA_GCS_SIGNER_KEY`. Do not keep a copy:

```bash
gcloud iam service-accounts keys create key.json --iam-account=bripta-docs-signer@$PROJECT.iam.gserviceaccount.com
npx supabase login && npx supabase secrets set --project-ref nngscmpsxtqqjzcnsrbi \
  BRIPTA_GCS_BUCKET=$BUCKET BRIPTA_GCS_SIGNER_KEY="$(cat key.json)"
shred -u key.json
```

If an organization policy blocks key creation, do not override it. The alternative is a small keyless Cloud Run signer (I can prepare it).

`verify_jwt = true` assumes the project's service-role key is the legacy JWT form, which is what the existing workflows use. If it is an `sb_secret_…` key, tell me and I will switch the verify call to a dedicated secret.

## Rollback

* Before the approved cleanup: `bripta-gcs-documents-rollback-20261122.sql` points every migrated row back to its Supabase original. Documents uploaded straight to GCS keep working only while the function stays deployed.
* Undo phase 3: re-run `bripta-client-documents-20261005.sql`.
* Website: revert the merge. The old code still reads Supabase originals until they are deleted.
* GCS deletions are recoverable for 7 days (soft delete).
