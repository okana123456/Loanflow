-- BRIPTA ONLY. Recover the THREE OLDER reviewed payments, KES 2,100.
-- Requires the previous recovery setup. Run the complete file once; safe to rerun.
-- Keeps the later Mercy KES 150 and Alice KES 300 and all unrelated payments/SMS unchanged.
-- Leaves the other unmatched payments and the held KES 300 unallocated.
-- Any changed baseline or unexpected trigger rolls back this entire batch.
begin;
set local lock_timeout='10s';
set local statement_timeout='120s';
set local timezone='Africa/Nairobi';

create temporary table bripta_recovery_manifest (
  transaction_code text primary key,
  queue_id uuid not null unique,
  client_id uuid not null,
  loan_id uuid not null,
  amount numeric(14,2) not null,
  account_phone text not null
) on commit drop;

-- REVIEWED_MANIFEST
insert into bripta_recovery_manifest(transaction_code,queue_id,client_id,loan_id,amount,account_phone) values
('UJ1JL8EY56','7d9c6ab8-74ed-450d-acdf-a70bd91aa1de','a98c5a96-e4c0-4947-91f7-0b222526226a','e61ed62b-864d-4d22-9b26-315445f6ec07','700','0714422902'),
('UJ1BZ8H7SH','2c02a620-2b67-45e4-8958-2acb61697d2c','3650f2c0-5ab5-427e-8530-c9f8fb9cd8e3','992a3ee7-1ff1-4c50-938c-f0054236ff76','700','0708067207'),
('UJ16F8CN12','4f8adaa9-7720-46c3-beb3-dba9f8b1a05c','289a3b5e-fe95-4646-a46f-ebe94f41745e','4f883d53-4099-4dd6-a9e7-4b2f301f0a5a','700','0791025565');

do $$
declare
  m record;
  q public.mpesa_callback_queue%rowtype;
  c public.loan_clients%rowtype;
  l public.loans%rowtype;
  after_l public.loans%rowtype;
  r public.loan_repayments%rowtype;
  sched record;
  before_schedules jsonb;
  after_schedules jsonb;
  receipt_time timestamptz;
  mpesa_time text;
  interest_amount numeric;
  remaining numeric;
  applied numeric;
  new_paid numeric;
  expected_balance numeric;
  expected_paid numeric;
  arrears numeric;
  oldest_due date;
  kenya_today date := (now() at time zone 'Africa/Nairobi')::date;
  schedule_available numeric;
  schedule_exception boolean;
  schedule_review_reason text;
begin
  perform pg_advisory_xact_lock(hashtextextended('BRIPTA-PAYMENT-RECOVERY-20261002',0));
  if (select count(*) from bripta_recovery_manifest)<>3
     or (select sum(amount) from bripta_recovery_manifest)<>2100 then
    raise exception 'Recovery manifest must contain exactly 3 payments totalling KES 2,100';
  end if;

  for m in
    select manifest.* from bripta_recovery_manifest manifest
    join public.mpesa_callback_queue source on source.id=manifest.queue_id
    order by source.created_at,manifest.transaction_code
  loop
    select * into q from public.mpesa_callback_queue where id=m.queue_id for update;
    if not found or q.business_short_code::text is distinct from 'BIZ-B3F5E5D9'
       or q.trans_id is distinct from m.transaction_code or q.trans_amount is distinct from m.amount
       or public.bripta_callback_phone(q.bill_ref_number::text) is distinct from public.bripta_callback_phone(m.account_phone)
       or q.created_at<timestamptz '2026-10-01 00:00:00+03:00'
       or nullif(btrim(q.bripta_review_reason),'') is not null then
      raise exception 'Callback no longer matches reviewed payment %',m.transaction_code;
    end if;

    select * into c from public.loan_clients where id=m.client_id for share;
    if not found or c.business_id is distinct from 'BIZ-B3F5E5D9'
       or public.bripta_callback_phone(c.phone::text) is distinct from public.bripta_callback_phone(m.account_phone)
       or (select count(*) from public.loan_clients candidate
           where candidate.business_id='BIZ-B3F5E5D9'
             and public.bripta_callback_phone(candidate.phone::text)=public.bripta_callback_phone(m.account_phone))<>1 then
      raise exception 'Client phone is missing, changed or ambiguous for %',m.transaction_code;
    end if;
    select * into l from public.loans where id=m.loan_id for update;
    if not found or l.business_id is distinct from 'BIZ-B3F5E5D9' or l.client_id is distinct from c.id then
      raise exception 'Loan/client/business mismatch for %',m.transaction_code;
    end if;

    if (select count(*) from public.loan_repayments x
        where x.payment_reference=m.transaction_code or x.receipt_no=m.transaction_code)>1 then
      raise exception 'Duplicate repayment reference already exists: %',m.transaction_code;
    end if;
    select * into r from public.loan_repayments x
    where x.payment_reference=m.transaction_code or x.receipt_no=m.transaction_code;
    if found then
      if r.business_id is distinct from 'BIZ-B3F5E5D9' or r.loan_id is distinct from m.loan_id
         or r.amount is distinct from m.amount or not coalesce(q.confirmed,false)
         or q.repayment_id is distinct from r.id then
        raise exception 'Existing repayment needs reconciliation for %; no second payment inserted',m.transaction_code;
      end if;
      continue;
    end if;
    if coalesce(q.confirmed,false) or q.repayment_id is not null
       or coalesce((to_jsonb(q)->>'dismissed')::boolean,false) then
      raise exception 'Callback already confirmed or dismissed: %',m.transaction_code;
    end if;
    if l.status is distinct from 'active' or coalesce(l.outstanding_balance,0)<m.amount
       or (select count(*) from public.loans x where x.business_id='BIZ-B3F5E5D9'
           and x.client_id=c.id and x.status='active' and x.outstanding_balance>0)<>1 then
      raise exception 'Loan state requires fresh review for %; no excess or new fees will be invented',m.transaction_code;
    end if;
    if l.branch_id is null or not exists (
      select 1 from public.bripta_branches b where b.id=l.branch_id and b.business_id='BIZ-B3F5E5D9'
    ) then raise exception 'Loan branch requires review for %',m.transaction_code; end if;

    mpesa_time:=coalesce(nullif(q.raw_payload->>'TransTime',''),q.trans_time::text);
    if mpesa_time ~ '^[0-9]{14}$' then
      receipt_time:=make_timestamptz(substring(mpesa_time,1,4)::int,substring(mpesa_time,5,2)::int,
        substring(mpesa_time,7,2)::int,substring(mpesa_time,9,2)::int,
        substring(mpesa_time,11,2)::int,substring(mpesa_time,13,2)::int,'Africa/Nairobi');
    else
      receipt_time:=q.created_at;
    end if;
    if receipt_time is null or abs(extract(epoch from (receipt_time-q.created_at)))>86400 then
      raise exception 'Original transaction date requires review for %',m.transaction_code;
    end if;
    -- Baselines reviewed from the live preflight. Refuse new activity rather
    -- than silently overwriting it. Existing correctly posted references skip above.
    if not (
      (m.transaction_code='UJ1JL8EY56' and q.created_at=timestamptz '2026-10-01 08:24:51.885971+00' and l.outstanding_balance=5100 and l.total_paid=150 and l.total_payable=5250 and l.total_interest=1250 and l.disbursed_amount=4000)
      or (m.transaction_code='UJ1BZ8H7SH' and q.created_at=timestamptz '2026-10-01 09:35:06.495297+00' and l.outstanding_balance=5000 and l.total_paid=0 and l.total_payable=5000 and l.total_interest=1000 and l.disbursed_amount=4000)
      or (m.transaction_code='UJ16F8CN12' and q.created_at=timestamptz '2026-10-01 10:04:48.5531+00' and l.outstanding_balance=4950 and l.total_paid=300 and l.total_payable=5250 and l.total_interest=1250 and l.disbursed_amount=4000)
    ) then raise exception 'Reviewed callback or loan baseline changed for %; stop for review',m.transaction_code; end if;
    if exists (
      select 1 from public.unmatched_payments u where u.business_id='BIZ-B3F5E5D9' and u.mpesa_reference=m.transaction_code
    ) then raise exception 'Payment % now has a suspense record; review first',m.transaction_code; end if;
    if m.transaction_code='UJ1JL8EY56' and not exists (
      select 1 from public.loan_repayments x
      where x.id='d90e371d-3a2f-4bb4-a47c-cb4f95d4825e'::uuid
        and x.business_id='BIZ-B3F5E5D9' and x.loan_id=l.id and x.amount=150
        and x.payment_reference='UJ2JL8L2LD' and x.receipt_no='UJ2JL8L2LD'
        and x.payment_method='M-Pesa'
        and x.payment_date=timestamptz '2026-10-02 14:08:15+00'
        and x.created_at=timestamptz '2026-10-02 14:08:18.649903+00'
        and x.notes like 'Auto-confirmed via Daraja C2B. Matched by borrower phone account.%'
    ) then raise exception 'Mercy later KES 150 no longer matches reviewed record'; end if;
    if m.transaction_code='UJ16F8CN12' and not exists (
      select 1 from public.loan_repayments x
      where x.id='d4166fe4-9f1f-4975-9660-2ce45218327e'::uuid
        and x.business_id='BIZ-B3F5E5D9' and x.loan_id=l.id and x.amount=300
        and x.payment_reference='UJ26F8GSQD' and x.receipt_no='UJ26F8GSQD'
        and x.payment_method='M-Pesa'
        and x.payment_date=timestamptz '2026-10-02 08:29:14+00'
        and x.created_at=timestamptz '2026-10-02 11:54:28.113371+00'
        and x.notes like '[BRIPTA_RECOVERY_20261002] [NEXT_THREE] Original M-Pesa callback restored%'
    ) then raise exception 'Alice later KES 300 no longer matches reviewed record'; end if;
    if exists (
      select 1 from public.loan_repayments x where x.loan_id=l.id
        and (x.payment_date>receipt_time or x.created_at>=q.created_at)
        and not (m.transaction_code='UJ1JL8EY56' and x.id='d90e371d-3a2f-4bb4-a47c-cb4f95d4825e'::uuid)
        and not (m.transaction_code='UJ16F8CN12' and x.id='d4166fe4-9f1f-4975-9660-2ce45218327e'::uuid)
    ) then raise exception 'Unreviewed later repayment for %; no payment added',m.transaction_code; end if;

    perform 1 from public.loan_schedules s where s.loan_id=l.id order by s.id for update;
    select coalesce(jsonb_agg(to_jsonb(s) order by s.id),'[]'::jsonb)
      into before_schedules from public.loan_schedules s where s.loan_id=l.id;
    select coalesce(sum(greatest(0,coalesce(s.total_due,0)-coalesce(s.total_paid,0))),0)
      into schedule_available from public.loan_schedules s where s.loan_id=l.id;
    schedule_exception:=false;
    schedule_review_reason:=null;
    if schedule_available<m.amount or schedule_available is distinct from l.outstanding_balance
       or (select sum(total_due) from public.loan_schedules where loan_id=l.id) is distinct from l.total_payable
       or (select sum(total_paid) from public.loan_schedules where loan_id=l.id) is distinct from l.total_paid then
      raise exception 'Reviewed schedule totals changed for %',m.transaction_code;
    end if;
    expected_paid:=round(coalesce(l.total_paid,0)+m.amount,2);
    expected_balance:=round(l.outstanding_balance-m.amount,2);
    interest_amount:=round(m.amount*case when coalesce(l.total_payable,0)>0
      and coalesce(l.total_interest,0)>0 then l.total_interest/l.total_payable else 0 end,2);

    insert into public.loan_repayments(
      business_id,branch_id,loan_id,receipt_no,payment_reference,amount,payment_method,payment_date,
      principal_portion,interest_portion,penalty_portion,registration_fee_portion,
      processing_fee_portion,loan_portion,credit_portion,mpesa_confirmed,sender_name,sender_phone,notes
    ) values (
      'BIZ-B3F5E5D9',l.branch_id,l.id,m.transaction_code,m.transaction_code,m.amount,'M-Pesa',receipt_time,
      m.amount-interest_amount,interest_amount,0,0,0,m.amount,0,true,
      concat_ws(' ',nullif(q.first_name,''),nullif(q.middle_name,''),nullif(q.last_name,'')),q.msisdn,
      '[BRIPTA_RECOVERY_20261002] [OLDER_THREE] Original M-Pesa callback restored using reviewed borrower PHONE ACCOUNT reference. No new fees.'
        ||case when schedule_exception then ' [SCHEDULE_REVIEW] '||schedule_review_reason else '' end
    ) returning * into r;
    if r.amount is distinct from m.amount or r.loan_portion is distinct from m.amount
       or coalesce(r.credit_portion,0)<>0 or coalesce(r.penalty_portion,0)<>0
       or coalesce(r.registration_fee_portion,0)<>0 or coalesce(r.processing_fee_portion,0)<>0
       or r.payment_date is distinct from receipt_time
       or r.interest_portion is distinct from interest_amount
       or r.principal_portion is distinct from m.amount-interest_amount then
      raise exception 'Repayment trigger changed the expected allocation for %; recovery rolled back',m.transaction_code;
    end if;

    -- Some deployments have balance triggers; never subtract a second time.
    select * into after_l from public.loans where id=l.id;
    if not ((coalesce(after_l.total_paid,0)=coalesce(l.total_paid,0) and after_l.outstanding_balance=l.outstanding_balance)
         or (after_l.total_paid=expected_paid and after_l.outstanding_balance=expected_balance))
       or after_l.total_payable is distinct from l.total_payable
       or after_l.total_interest is distinct from l.total_interest
       or after_l.disbursed_amount is distinct from l.disbursed_amount then
      raise exception 'Unexpected loan balance trigger for %; recovery rolled back',m.transaction_code;
    end if;
    select coalesce(jsonb_agg(to_jsonb(s) order by s.id),'[]'::jsonb)
      into after_schedules from public.loan_schedules s where s.loan_id=l.id;
    if before_schedules is distinct from after_schedules then
      raise exception 'A trigger already changed schedules for %; review before applying again',m.transaction_code;
    end if;

    remaining:=m.amount;
    for sched in select * from public.loan_schedules s where s.loan_id=l.id
      and coalesce(s.total_paid,0)<coalesce(s.total_due,0)
      order by s.due_date,s.installment_no,s.id
    loop
      exit when remaining<=0;
      applied:=least(remaining,greatest(0,coalesce(sched.total_due,0)-coalesce(sched.total_paid,0)));
      new_paid:=round(coalesce(sched.total_paid,0)+applied,2);
      update public.loan_schedules set total_paid=new_paid,
        status=case when new_paid>=total_due then 'paid' when due_date<kenya_today then 'overdue' else 'partial' end,
        paid_at=case when new_paid>=total_due then greatest(receipt_time,(select max(x.payment_date) from public.loan_repayments x where x.loan_id=l.id)) else paid_at end
      where id=sched.id;
      remaining:=round(remaining-applied,2);
    end loop;
    if remaining<>0 and not schedule_exception then
      raise exception 'Incomplete schedule allocation for %',m.transaction_code;
    end if;
    if schedule_exception and (remaining<>m.amount or before_schedules is distinct from (
      select coalesce(jsonb_agg(to_jsonb(s) order by s.id),'[]'::jsonb) from public.loan_schedules s where s.loan_id=l.id
    )) then raise exception 'Reviewed legacy schedules changed unexpectedly for %',m.transaction_code; end if;

    select coalesce(sum(greatest(0,coalesce(s.total_due,0)-coalesce(s.total_paid,0))),0),
      min(s.due_date) filter(where coalesce(s.total_due,0)-coalesce(s.total_paid,0)>0.01)
    into arrears,oldest_due from public.loan_schedules s where s.loan_id=l.id and s.due_date<kenya_today;
    update public.loans set total_paid=expected_paid,outstanding_balance=expected_balance,
      arrears_amount=case when schedule_exception then least(expected_balance,greatest(0,coalesce(l.arrears_amount,0)-m.amount))
        else least(expected_balance,greatest(0,arrears)) end,
      overdue_days=case when expected_balance<=0 then 0 when schedule_exception then l.overdue_days
        when oldest_due is null then 0 else greatest(0,kenya_today-oldest_due) end,
      status=case when expected_balance<=0 then 'completed' else l.status end
    where id=l.id;
    update public.mpesa_callback_queue set confirmed=true,loan_id=l.id,repayment_id=r.id,branch_id=l.branch_id
    where id=q.id;
    update public.unmatched_payments set resolved=true,resolved_at=now()
    where business_id='BIZ-B3F5E5D9' and mpesa_reference=m.transaction_code and not coalesce(resolved,false);

    -- Existing unique repayment_id prevents a second SMS. Original sent SMS and
    -- wallet totals are untouched. The existing sender processes queued_at today.
    insert into public.bripta_sms_outbox(business_id,repayment_id,loan_id,client_id,client_name,message_type)
    values('BIZ-B3F5E5D9',r.id,l.id,c.id,c.full_name,'repayment')
    on conflict(repayment_id) do nothing;

    select * into after_l from public.loans where id=l.id;
    select coalesce(jsonb_agg(to_jsonb(s) order by s.id),'[]'::jsonb)
      into after_schedules from public.loan_schedules s where s.loan_id=l.id;
    if after_l.total_paid is distinct from expected_paid or after_l.outstanding_balance is distinct from expected_balance
       or after_l.total_payable is distinct from l.total_payable
       or after_l.total_interest is distinct from l.total_interest
       or after_l.disbursed_amount is distinct from l.disbursed_amount
       or (select sum(coalesce((x->>'total_paid')::numeric,0)) from jsonb_array_elements(after_schedules) x)
          - (select sum(coalesce((x->>'total_paid')::numeric,0)) from jsonb_array_elements(before_schedules) x)<>m.amount-remaining
       or (select sum(coalesce((x->>'total_due')::numeric,0)) from jsonb_array_elements(after_schedules) x)
          is distinct from (select sum(coalesce((x->>'total_due')::numeric,0)) from jsonb_array_elements(before_schedules) x) then
      raise exception 'Final balance or schedule verification failed for %; recovery rolled back',m.transaction_code;
    end if;
    insert into public.bripta_payment_recovery_20261002(
      transaction_code,business_id,queue_id,client_id,loan_id,repayment_id,amount,payment_date,
      loan_before,loan_after,schedules_before,schedules_after,
      schedule_applied_amount,schedule_unallocated_amount,schedule_review_reason
    ) values(m.transaction_code,'BIZ-B3F5E5D9',q.id,c.id,l.id,r.id,m.amount,receipt_time,
      to_jsonb(l),to_jsonb(after_l),before_schedules,after_schedules,m.amount-remaining,remaining,schedule_review_reason);
  end loop;

  if (select count(*) from bripta_recovery_manifest manifest join public.mpesa_callback_queue source
      on source.id=manifest.queue_id and source.confirmed=true join public.loan_repayments payment
      on payment.id=source.repayment_id and payment.loan_id=manifest.loan_id
      and payment.business_id='BIZ-B3F5E5D9' and payment.amount=manifest.amount)<>3 then
    raise exception 'Recovery verification failed; the entire transaction is rolled back';
  end if;
end $$;

commit;

-- Batch verification, including SMS state (queued is not yet sent).
select a.transaction_code,a.amount,l.loan_no,l.outstanding_balance,
  r.payment_date,q.confirmed,s.status as sms_status,
  a.schedule_unallocated_amount
from public.bripta_payment_recovery_20261002 a
join public.loan_repayments r on r.id=a.repayment_id
join public.loans l on l.id=a.loan_id
join public.mpesa_callback_queue q on q.id=a.queue_id
left join public.bripta_sms_outbox s on s.repayment_id=a.repayment_id
where a.business_id='BIZ-B3F5E5D9'
  and a.transaction_code in ('UJ1JL8EY56','UJ1BZ8H7SH','UJ16F8CN12')
order by a.transaction_code;
