-- READ ONLY. Check three older KES 700 callbacks, later repayments and
-- schedule capacity before any recovery. No financial or SMS writes.
with targets(reference,queue_id,client_id,loan_id,amount) as (
  values
  ('UJ1JL8EY56','7d9c6ab8-74ed-450d-acdf-a70bd91aa1de'::uuid,'a98c5a96-e4c0-4947-91f7-0b222526226a'::uuid,'e61ed62b-864d-4d22-9b26-315445f6ec07'::uuid,700::numeric),
  ('UJ1BZ8H7SH','2c02a620-2b67-45e4-8958-2acb61697d2c'::uuid,'3650f2c0-5ab5-427e-8530-c9f8fb9cd8e3'::uuid,'992a3ee7-1ff1-4c50-938c-f0054236ff76'::uuid,700::numeric),
  ('UJ16F8CN12','4f8adaa9-7720-46c3-beb3-dba9f8b1a05c'::uuid,'289a3b5e-fe95-4646-a46f-ebe94f41745e'::uuid,'4f883d53-4099-4dd6-a9e7-4b2f301f0a5a'::uuid,700::numeric)
)
select t.reference,t.amount as expected_amount,
  q.id as queue_id,q.trans_amount as callback_amount,q.created_at as callback_at,
  q.confirmed,q.bill_ref_number,q.loan_id as callback_loan_id,q.repayment_id,
  to_jsonb(q)->>'bripta_review_reason' as review_reason,
  coalesce(q.raw_payload->>'TransTime',to_jsonb(q)->>'trans_time') as mpesa_time,
  c.id as client_id,c.full_name,c.phone,
  l.id as loan_id,l.loan_no,l.status,l.outstanding_balance,l.total_paid,l.total_payable,
  l.total_interest,l.disbursed_amount,l.arrears_amount,l.branch_id,
  (select count(*) from public.loan_repayments r
    where r.business_id='BIZ-B3F5E5D9' and (r.payment_reference=t.reference or r.receipt_no=t.reference)) as already_recorded,
  (select count(*) from public.unmatched_payments u
    where u.business_id='BIZ-B3F5E5D9' and u.mpesa_reference=t.reference) as existing_suspense,
  (select count(*) from public.loans a
    where a.business_id='BIZ-B3F5E5D9' and a.client_id=c.id and a.status='active' and a.outstanding_balance>0) as active_loan_count,
  (select coalesce(sum(greatest(0,coalesce(s.total_due,0)-coalesce(s.total_paid,0))),0)
    from public.loan_schedules s where s.loan_id=l.id) as schedule_unpaid,
  (select coalesce(jsonb_agg(jsonb_build_object(
    'id',s.id,'due_date',s.due_date,'installment',s.installment_no,
    'due',s.total_due,'paid',s.total_paid,'status',s.status
  ) order by s.due_date,s.installment_no),'[]'::jsonb)
    from public.loan_schedules s where s.loan_id=l.id) as schedules,
  (select coalesce(jsonb_agg(jsonb_build_object(
    'id',r.id,'amount',r.amount,'reference',r.payment_reference,'receipt',r.receipt_no,
    'method',r.payment_method,'payment_date',r.payment_date,'created_at',r.created_at,'notes',r.notes
  ) order by r.created_at),'[]'::jsonb)
    from public.loan_repayments r where r.business_id='BIZ-B3F5E5D9' and r.loan_id=l.id
      and (r.created_at>=q.created_at or r.payment_date>=q.created_at)) as possibly_later_repayments,
  (select jsonb_agg(jsonb_build_object('shortcode',s.mpesa_shortcode,'auto_confirm',s.mpesa_auto_confirm))
    from public.loan_settings s where s.business_id='BIZ-B3F5E5D9') as payment_settings,
  to_regprocedure('public.bripta_callback_phone_candidates(text,text)') is not null as phone_lookup_installed
from targets t
left join public.mpesa_callback_queue q on q.id=t.queue_id
  and q.business_short_code='BIZ-B3F5E5D9' and q.trans_id=t.reference
left join public.loan_clients c on c.id=t.client_id and c.business_id='BIZ-B3F5E5D9'
left join public.loans l on l.id=t.loan_id and l.client_id=t.client_id and l.business_id='BIZ-B3F5E5D9'
order by t.reference;
