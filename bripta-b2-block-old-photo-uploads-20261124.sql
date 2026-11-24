-- Bripta only. Run after the updated bripta-documents Edge Function and site are live.
-- Browser uploads to the old Supabase client-photos bucket are blocked; service-role
-- cleanup and existing photo reads continue to work.
drop policy if exists bripta_no_new_client_photo_storage on storage.objects;
create policy bripta_no_new_client_photo_storage on storage.objects
  as restrictive for insert to public
  with check (bucket_id<>'client-photos' or name not like 'BIZ-B3F5E5D9/%' or auth.role()='service_role');
