-- Example: grant Bripta five internal test credits before going live.
-- For a paid bundle, replace 5, 0 and INTERNAL-TEST with the credits sold,
-- Bripta's retail payment amount and their payment reference.
-- Your TalkSasa purchase cost is intentionally not stored.

select public.bripta_owner_add_sms_credits(
  p_business_id := 'BIZ-B3F5E5D9',
  p_credits := 5,
  p_customer_amount_paid := 0,
  p_customer_reference := 'INTERNAL-TEST',
  p_note := 'Bripta SMS integration test credits'
);
