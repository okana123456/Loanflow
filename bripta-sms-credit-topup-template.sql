-- Example: grant Bripta five internal test credits before going live.
-- For a paid bundle, replace 5, 0 and INTERNAL-TEST with the credits sold,
-- Bripta's retail payment amount and their payment reference.
-- Your TalkSasa purchase cost is intentionally not stored.

select public.bripta_owner_add_sms_credits(
  'BIZ-B3F5E5D9'::text,
  5::integer,
  0::numeric,
  'INTERNAL-TEST'::text,
  'Bripta SMS integration test credits'::text
);
