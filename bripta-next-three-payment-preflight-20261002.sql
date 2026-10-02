-- READ ONLY. Check schedule capacity and possible later/manual credits BEFORE
-- recovering the next three phone-account matches. No financial or SMS writes.
with targets(reference,queue_id,client_id,loan_id,amount) as (
  values
  ('UJ2DH8FO3R','cd572f1f-013e-469c-8bfa-4ecc923252cc'::uuid,'5e6f3bb3-d430-43f3-8e52-01db58ee7c8b'::uuid,'d943e06f-262a-4fc7-9054-d73ac415aebc'::uuid,450::numeric),
  ('UJ26E87N0Q','44cd7b9f-56eb-4acd-8165-63b402fd9a3a'::uuid,'4f69699b-e5f1-4b5e-a3fd-21713479e531'::uuid,'6adf2cf1-e49b-4ebb-a0bf-79a64be003d0'::uuid,1400::numeric),
  ('UJ26F8GSQD','d6511e3f-27c3-4d25-abe8-603682322602'::uuid,'289a3b5e-fe95-4646-a46f-ebe94f41745e'::uuid,'4f883d53-4099-4dd6-a9e7-4b2f301f0a5a'::uuid,300::numeric)
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
