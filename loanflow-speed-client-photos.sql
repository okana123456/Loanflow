-- Bripta speed and client photo setup
-- Run this once in Supabase SQL Editor for the Loanflow/Bripta project.

alter table public.loan_clients
  add column if not exists photo_path text;

-- Store compressed profile photos in Storage, not inside the database.
-- The database only keeps the small file path above.
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'client-photos',
  'client-photos',
  true,
  524288,
  array['image/jpeg', 'image/png', 'image/webp']
)
on conflict (id) do update
set public = excluded.public,
    file_size_limit = excluded.file_size_limit,
    allowed_mime_types = excluded.allowed_mime_types;

do $$
begin
  create policy "client photos read"
  on storage.objects for select
  to authenticated
  using (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

do $$
begin
  create policy "client photos upload"
  on storage.objects for insert
  to authenticated
  with check (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

do $$
begin
  create policy "client photos update"
  on storage.objects for update
  to authenticated
  using (bucket_id = 'client-photos')
  with check (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

do $$
begin
  create policy "client photos delete"
  on storage.objects for delete
  to authenticated
  using (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

-- Bripta uses the browser publishable key for logged-in app screens, so allow
-- the anon browser role to manage these small profile files through the app UI.
do $$
begin
  create policy "client photos app read"
  on storage.objects for select
  to anon
  using (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

do $$
begin
  create policy "client photos app upload"
  on storage.objects for insert
  to anon
  with check (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

do $$
begin
  create policy "client photos app update"
  on storage.objects for update
  to anon
  using (bucket_id = 'client-photos')
  with check (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

do $$
begin
  create policy "client photos app delete"
  on storage.objects for delete
  to anon
  using (bucket_id = 'client-photos');
exception when duplicate_object then null;
end $$;

-- Performance indexes for the screens the client uses daily.
create index if not exists idx_loan_clients_business_created
  on public.loan_clients (business_id, created_at desc);

create index if not exists idx_loan_clients_business_phone
  on public.loan_clients (business_id, phone);

create index if not exists idx_loan_clients_business_id_number
  on public.loan_clients (business_id, id_number);

create index if not exists idx_loans_business_status_client
  on public.loans (business_id, status, client_id);

create index if not exists idx_loans_client_created
  on public.loans (client_id, created_at desc);

create index if not exists idx_loans_business_officer_status
  on public.loans (business_id, loan_officer_id, status);

create index if not exists idx_loan_repayments_business_payment_date
  on public.loan_repayments (business_id, payment_date desc);

create index if not exists idx_loan_repayments_loan_payment_date
  on public.loan_repayments (loan_id, payment_date desc);

create index if not exists idx_loan_schedules_business_due_date
  on public.loan_schedules (business_id, due_date);

create index if not exists idx_loan_schedules_loan_status_due
  on public.loan_schedules (loan_id, status, due_date);

create index if not exists idx_loan_applications_business_status_created
  on public.loan_applications (business_id, status, created_at desc);

create index if not exists idx_loan_staff_business_active
  on public.loan_staff (business_id, is_active);
