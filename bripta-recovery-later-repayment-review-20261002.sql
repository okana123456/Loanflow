-- READ ONLY: examine every reviewed loan for later repayments, including
-- Millicent's loan 333352 / missing callback UJ1DD89X72 (KES 50).
-- Nothing is credited, deleted, marked confirmed, or sent as SMS.
-- Compare both payment_date and created_at: older callbacks may have stored
-- a Kenya local time without a timezone offset. A later payment_date alone
-- does not prove this missing transaction was already credited.

with manifest(transaction_code,queue_id,client_id,loan_id,amount,account_phone) as (
  values
  ('UJ1HX8H640','32968bec-5772-4f8c-afb2-9c5a40032e07','03869c45-4dbd-42e6-8959-232993a87e2a','05f03fd9-0e9f-446a-aca0-a0785d326e4b','150','0729477489'),
('UJ1568Z2M8','9b16b625-c47e-49d7-9b92-57cee9296767','42156493-6556-46a3-8cd8-330e71bd5078','545a5ce1-f315-4d76-89ff-3e4cb00e1dc2','500','0715098798'),
('UJ1DD89X72','871b9eb9-4488-4b5c-b31c-33a15ba57212','6a39af36-0401-4067-a2a7-27ba025d3857','bda3616a-3c1a-4239-ad0a-7f9308b96461','50','0705097683'),
('UJ1LR7XG7X','504deef4-8df3-4699-95ff-a86ba65d1e3c','79c2065e-5c8e-4cf7-b0cf-0ee5f3c27893','0c312181-2f3d-4943-87c4-ba488ab8af50','200','0701028331'),
('UJ29S8L591','e3a9f597-f80b-447c-bc87-e9e4a2bc60e6','b18f7099-51e4-448f-b9f0-bfe244244bca','c82480e9-8605-4281-aacf-95c53c7553ab','1000','0113311245'),
('UJ2OC8MELO','fe5f1f3c-4a4c-4b69-b5c5-ca1d144dde9c','fe472920-b179-4549-8abd-9019d8bf6f61','6dc783b8-8ff9-4efe-8808-2b4da3df2a3d','1000','0742254908'),
('UJ2GW8OEX7','fa0a5d18-6289-4c99-bb49-75115d22b188','0dc9a8ec-f503-4592-a5a3-f7de5ef4d61c','e7464e02-c3fb-4922-942a-4099519cf7dd','1600','0715976965'),
('UJ2HL8AWHC','de9185ea-b21a-4dc0-98f7-5fbf459b29ae','fa650085-5391-4c59-9b98-887b6d724b56','b5d8e443-c1a0-43d8-9961-dbfc9bcb575a','220','0797580080'),
('UJ2BR8G2WP','c3aacf1c-1adf-40cf-a6b4-18908cc283f8','d018c2ab-1937-411a-adce-5253fea9ca63','8bfbab89-ac6b-46e3-b4af-fb65c3245d7f','200','0723104693'),
('UJ2AL8H8E3','3a62217a-c86c-4ed9-9ca8-2a494013466b','fce4729a-442e-4b10-bd8f-2c41d8b2fe74','0b2a678a-ecd5-4c7f-b33e-610421ca0534','100','0716415327'),
('UJ2JL8ISV4','a55fee85-2775-46f5-8c4f-58b8290c20ca','b5b698de-a643-40c7-80af-6ca17fd3541f','abc683fb-570a-4c59-ab1e-f9ce3ad40bc6','300','0768468715'),
('UJ2GJ8QZMH','3ab560a7-d0dd-42a9-8d94-0aaf89f78bd4','6064863b-cfe3-42e8-ad1f-9a3a8d39d1e6','ad0b30c6-62a2-46d9-b1fb-fee488f3a5a1','295','0793521244'),
('UJ2AL8HEDN','fc5dd9f6-3394-47a8-8cef-480e8ecfcbd2','fce4729a-442e-4b10-bd8f-2c41d8b2fe74','0b2a678a-ecd5-4c7f-b33e-610421ca0534','100','0716415327'),
('UJ2AL8HCV7','18a1faa2-effe-4a33-9432-bdc70c13db63','fce4729a-442e-4b10-bd8f-2c41d8b2fe74','0b2a678a-ecd5-4c7f-b33e-610421ca0534','50','0716415327'),
('UJ22O99NS9','777ea545-c6ee-4515-9b0f-a90868af9ef4','3d0d64f1-07fc-4fc1-814c-8c3da6911d77','f7f649c5-338f-4dee-a3c6-50c706390e5c','300','0703182794'),
('UJ2098Y6O0','beebe029-6d21-4580-ae8c-3d13942e46da','ccebebbe-1523-450c-8ab4-a6e26b4946f1','b3635111-1782-4305-b907-b9e9d6191a72','300','0742420510'),
('UJ20L8DGO8','05fdbbdc-c8ce-4c83-99ec-2e120f85e02a','fa0d6e16-e368-442f-98f5-676bfad58bdd','12018065-3a0d-4434-a9b4-995c735a6968','520','0714739319'),
('UJ2GR8A3P3','95ddc6e7-003b-4807-8e97-650b5ecd870e','ebf5fc6a-995f-4ed3-8842-12407fe34c9b','97c1293d-2dc6-4cf6-b7ae-ad3d469d94c9','600','0724172916'),
('UJ2GR8A1Q0','e5dd61f4-7c4e-4a84-a4c8-8c7cbfc9cb50','ebf5fc6a-995f-4ed3-8842-12407fe34c9b','97c1293d-2dc6-4cf6-b7ae-ad3d469d94c9','100','0724172916'),
('UJ2HX8I6A2','322a8b0e-7ea6-4a8b-81b9-576edf8b9c10','03869c45-4dbd-42e6-8959-232993a87e2a','05f03fd9-0e9f-446a-aca0-a0785d326e4b','100','0729477489')
), pending as (
  select m.transaction_code,m.amount::numeric as missing_amount,
    m.loan_id::uuid as target_loan_id,q.*,
    case when coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text) ~ '^[0-9]{14}$'
      then make_timestamptz(
        substring(coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text),1,4)::int,
        substring(coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text),5,2)::int,
        substring(coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text),7,2)::int,
        substring(coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text),9,2)::int,
        substring(coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text),11,2)::int,
        substring(coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text),13,2)::int,
        'Africa/Nairobi')
      else q.created_at end as original_payment_at
  from manifest m join public.mpesa_callback_queue q on q.id=m.queue_id::uuid
  where q.business_short_code='BIZ-B3F5E5D9'
)
select p.transaction_code as missing_transaction,
  p.missing_amount,
  p.original_payment_at as missing_payment_at_utc,
  p.created_at as missing_callback_received_at_utc,
  p.confirmed as missing_callback_confirmed,
  l.loan_no,c.full_name as client_name,l.outstanding_balance as current_loan_balance,
  r.id as existing_repayment_id,r.amount as existing_amount,
  r.payment_reference as existing_reference,r.receipt_no as existing_receipt,
  r.payment_method as existing_method,r.payment_date as existing_payment_date,
  pg_typeof(r.payment_date)::text as payment_date_column_type,
  r.created_at as existing_record_created_at_utc,
  (r.amount=p.missing_amount) as same_amount,
  (r.created_at<p.created_at) as existing_record_created_before_missing_callback,
  existing_q.trans_id as existing_payment_callback_reference,
  existing_q.trans_amount as existing_payment_callback_amount,
  existing_q.created_at as existing_payment_callback_received_at_utc,
  existing_q.trans_time as existing_payment_mpesa_time,
  s.status as existing_sms_status,
  left(r.notes,700) as existing_payment_notes
from pending p
join public.loans l on l.id=p.target_loan_id and l.business_id='BIZ-B3F5E5D9'
join public.loan_clients c on c.id=l.client_id and c.business_id=l.business_id
left join public.loan_repayments r on r.loan_id=l.id and r.business_id=l.business_id
  and (r.payment_date>=p.original_payment_at or r.created_at>=p.created_at
       or r.payment_reference=p.transaction_code or r.receipt_no=p.transaction_code)
left join lateral (
  select q2.trans_id,q2.trans_amount,q2.created_at,q2.trans_time
  from public.mpesa_callback_queue q2
  where q2.business_short_code='BIZ-B3F5E5D9'
    and (q2.repayment_id=r.id or q2.trans_id=r.payment_reference or q2.trans_id=r.receipt_no)
  order by q2.confirmed desc nulls last,q2.created_at limit 1
) existing_q on true
left join public.bripta_sms_outbox s on s.repayment_id=r.id and s.business_id=l.business_id
order by case when p.transaction_code='UJ1DD89X72' then 0 else 1 end,
  p.created_at,p.transaction_code,r.created_at,r.id;
