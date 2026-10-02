-- BRIPTA ONLY: BIZ-B3F5E5D9 / Supabase nngscmpsxtqqjzcnsrbi.
-- Run this complete file BEFORE redeploying payment-callback.
-- Recover the 20 reviewed transactions (KES 7,685), using their original dates.
-- UJ2558DDTO (KES 300) stays in Suspense at the owner's explicit instruction.
-- Re-running this file does not apply the same payments twice.
-- Any failed guard rolls back the entire transaction, including queued SMS.

begin;
set local lock_timeout = '10s';
set local statement_timeout = '120s';
set local timezone = 'Africa/Nairobi';

alter table public.mpesa_callback_queue
  add column if not exists bripta_review_reason text;

-- Match only a complete Kenyan phone number entered in the ACCOUNT field.
-- Do not derive a borrower from the payer's phone, a hash, or a short suffix.
create or replace function public.bripta_callback_phone(p_value text)
returns text language sql immutable strict set search_path = public
as $$
  select case
    when btrim(p_value) !~ '^[+0-9()[:space:]-]+$' then null
    when regexp_replace(p_value,'[^0-9]','','g') ~ '^0[17][0-9]{8}$'
      then '254'||right(regexp_replace(p_value,'[^0-9]','','g'),9)
    when regexp_replace(p_value,'[^0-9]','','g') ~ '^254[17][0-9]{8}$'
      then regexp_replace(p_value,'[^0-9]','','g')
    when regexp_replace(p_value,'[^0-9]','','g') ~ '^[17][0-9]{8}$'
      then '254'||regexp_replace(p_value,'[^0-9]','','g')
    else null end;
$$;

-- Service-role lookup for the Edge Function, restricted to this business.
-- Return up to TWO matches so a shared phone can never select an arbitrary client.
create or replace function public.bripta_callback_phone_candidates(
  p_account_reference text, p_transaction_code text
) returns table(id uuid,business_id text,full_name text,account_credit numeric)
language sql stable security definer set search_path = public
as $$
  select c.id,c.business_id::text,c.full_name::text,coalesce(c.account_credit,0)::numeric
  from public.loan_clients c
  where c.business_id='BIZ-B3F5E5D9'
    and public.bripta_callback_phone(c.phone::text)=public.bripta_callback_phone(p_account_reference)
    and not exists (
      select 1 from public.mpesa_callback_queue q
      where q.business_short_code::text='BIZ-B3F5E5D9'
        and q.trans_id::text=p_transaction_code
        and nullif(btrim(q.bripta_review_reason),'') is not null
    )
  order by c.id limit 2;
$$;
revoke all on function public.bripta_callback_phone_candidates(text,text) from public,anon,authenticated;
grant execute on function public.bripta_callback_phone_candidates(text,text) to service_role;

-- Private before/after snapshots for this recovery; no existing history is removed.
create table if not exists public.bripta_payment_recovery_20261002 (
  transaction_code text primary key,
  business_id text not null check(business_id='BIZ-B3F5E5D9'),
  queue_id uuid not null,
  client_id uuid not null,
  loan_id uuid not null,
  repayment_id uuid not null unique,
  amount numeric(14,2) not null check(amount>0),
  payment_date timestamptz not null,
  loan_before jsonb not null,
  loan_after jsonb not null,
  schedules_before jsonb not null,
  schedules_after jsonb not null,
  recovered_at timestamptz not null default now(),
  recovered_by text not null default current_user
);
alter table public.bripta_payment_recovery_20261002 enable row level security;
alter table public.bripta_payment_recovery_20261002
  add column if not exists schedule_applied_amount numeric(14,2),
  add column if not exists schedule_unallocated_amount numeric(14,2),
  add column if not exists schedule_review_reason text;
revoke all on public.bripta_payment_recovery_20261002 from public,anon,authenticated;
grant all on public.bripta_payment_recovery_20261002 to service_role;

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
('UJ2HX8I6A2','322a8b0e-7ea6-4a8b-81b9-576edf8b9c10','03869c45-4dbd-42e6-8959-232993a87e2a','05f03fd9-0e9f-446a-aca0-a0785d326e4b','100','0729477489');

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
  if (select count(*) from bripta_recovery_manifest)<>20
     or (select sum(amount) from bripta_recovery_manifest)<>7685 then
    raise exception 'Recovery manifest must contain exactly 20 payments totalling KES 7,685';
  end if;

  -- Keep the explicitly disputed payment available for a human to match later.
  select * into q from public.mpesa_callback_queue
  where id='06ffeb78-4240-4d04-9cbb-8614620e97b2' for update;
  if not found or q.business_short_code::text is distinct from 'BIZ-B3F5E5D9'
     or q.trans_id is distinct from 'UJ2558DDTO' or q.trans_amount is distinct from 300::numeric
     or coalesce(q.confirmed,false) or q.repayment_id is not null
     or exists(select 1 from public.loan_repayments x
               where x.payment_reference='UJ2558DDTO' or x.receipt_no='UJ2558DDTO') then
    raise exception 'UJ2558DDTO changed since review. Stop and inspect it before recovering payments';
  end if;
  update public.mpesa_callback_queue
  set bripta_review_reason='Owner requested Suspense: payer George, account candidate Joseph Bhoke Colleta. Do not auto-allocate.'
  where id=q.id;

  for m in
    select manifest.* from bripta_recovery_manifest manifest
    join public.mpesa_callback_queue source on source.id=manifest.queue_id
    order by source.created_at,manifest.transaction_code
  loop
    select * into q from public.mpesa_callback_queue where id=m.queue_id for update;
    if not found or q.business_short_code::text is distinct from 'BIZ-B3F5E5D9'
       or q.trans_id is distinct from m.transaction_code or q.trans_amount is distinct from m.amount
       or public.bripta_callback_phone(q.bill_ref_number::text) is distinct from public.bripta_callback_phone(m.account_phone)
       or q.created_at<timestamptz '2026-10-01 22:00:00+03:00'
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
    -- Older callbacks sometimes stored Kenya wall-clock time as UTC (+3h).
    -- Exclude a false "later payment" only when a separate confirmed callback
    -- proves its amount, account, reference and actual earlier receipt time.
    -- Do not rewrite that historical repayment or resend its SMS.
    if exists(
      select 1 from public.loan_repayments x
      where x.loan_id=l.id and (x.payment_date>receipt_time or
        (x.created_at>=q.created_at and not exists (
          select 1 from public.bripta_payment_recovery_20261002 recovered
          where recovered.repayment_id=x.id and recovered.business_id='BIZ-B3F5E5D9'
            and recovered.payment_date<=receipt_time
        )))
        and not exists (
          select 1 from public.mpesa_callback_queue earlier
          where x.business_id='BIZ-B3F5E5D9' and earlier.business_short_code='BIZ-B3F5E5D9'
            and earlier.trans_id=x.payment_reference and earlier.trans_id<>m.transaction_code
            and earlier.trans_amount=x.amount and coalesce(earlier.confirmed,false)
            and x.created_at<receipt_time and earlier.created_at<receipt_time
            and earlier.trans_time::text=to_char(x.payment_date at time zone 'UTC','YYYYMMDDHH24MISS')
            and x.payment_date-interval '3 hours'<receipt_time
            and abs(extract(epoch from (earlier.created_at-(x.payment_date-interval '3 hours'))))<=300
            and (earlier.repayment_id=x.id or
                 public.bripta_callback_phone(earlier.bill_ref_number::text)=public.bripta_callback_phone(c.phone::text))
        )
    ) then
      raise exception 'Loan % has a later repayment; review possible manual credit before restoring %',l.loan_no,m.transaction_code;
    end if;

    perform 1 from public.loan_schedules s where s.loan_id=l.id order by s.id for update;
    select coalesce(jsonb_agg(to_jsonb(s) order by s.id),'[]'::jsonb)
      into before_schedules from public.loan_schedules s where s.loan_id=l.id;
    select coalesce(sum(greatest(0,coalesce(s.total_due,0)-coalesce(s.total_paid,0))),0)
      into schedule_available from public.loan_schedules s where s.loan_id=l.id;
    schedule_exception:=false;
    schedule_review_reason:=null;
    if schedule_available<m.amount then
      -- Reviewed legacy exception ONLY: Emily has an accepted loan balance,
      -- but all three remaining schedule rows are paid and one is dated 36238.
      -- Credit the proven payment against the existing balance; do not invent
      -- an instalment, overwrite historical schedules or recompute old totals.
      if m.transaction_code='UJ20L8DGO8' and m.amount=520
         and l.id='12018065-3a0d-4434-a9b4-995c735a6968'::uuid
         and l.outstanding_balance=719.50 and l.total_paid=6995
         and l.total_payable=7995 and l.total_interest=1500 and l.disbursed_amount=6000
         and l.arrears_amount=719.50 and schedule_available=0
         and (select count(*)=3 and sum(s.total_due)=4265 and sum(s.total_paid)=4265
              and bool_and(s.status='paid' and s.total_due=s.total_paid)
              and bool_and(s.id in ('38e694ed-b7c3-4a56-9f00-095a6ca11c1d'::uuid,
                'a57e53b4-c6ea-4eaa-a9fc-31f8f540ccb9'::uuid,'53f42ca9-006f-4fa7-98f9-6c51140c7446'::uuid))
              from public.loan_schedules s where s.loan_id=l.id)
         and exists(select 1 from public.loan_schedules s
              where s.id='53f42ca9-006f-4fa7-98f9-6c51140c7446'::uuid and s.loan_id=l.id
                and s.due_date=date '36238-08-03' and s.installment_no=1785103)
         and (select count(*)=19 and sum(x.amount)=6995 and sum(coalesce(x.loan_portion,x.amount))=6995
              from public.loan_repayments x where x.loan_id=l.id and x.business_id='BIZ-B3F5E5D9') then
        schedule_exception:=true;
        schedule_review_reason:='Reviewed legacy schedule gap: loan 222148 has three paid schedules (KES 4265), including date 36238-08-03. KES 520 credited to accepted balance 719.50; schedule allocation pending historical review. No schedules changed.';
      else
        raise exception 'Remaining schedule amounts cannot cover %; review the loan first',m.transaction_code;
      end if;
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
      '[BRIPTA_RECOVERY_20261002] Original M-Pesa callback restored using reviewed borrower PHONE ACCOUNT reference. No new fees.'
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
        paid_at=case when new_paid>=total_due then receipt_time else paid_at end
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
      and payment.business_id='BIZ-B3F5E5D9' and payment.amount=manifest.amount)<>20 then
    raise exception 'Recovery verification failed; the entire transaction is rolled back';
  end if;
end $$;

-- Report committed changes only if every guard above succeeded.
commit;

select 'recovered_payments' as check_name,count(*)::numeric as result
from public.bripta_payment_recovery_20261002 where business_id='BIZ-B3F5E5D9'
union all select 'recovered_amount',coalesce(sum(amount),0)
from public.bripta_payment_recovery_20261002 where business_id='BIZ-B3F5E5D9'
union all select 'schedule_allocation_pending_review',coalesce(sum(schedule_unallocated_amount),0)
from public.bripta_payment_recovery_20261002 where business_id='BIZ-B3F5E5D9'
union all select 'emily_balance_after_recovery',outstanding_balance::numeric
from public.loans where business_id='BIZ-B3F5E5D9' and id='12018065-3a0d-4434-a9b4-995c735a6968'::uuid
union all select 'held_in_suspense',count(*)::numeric
from public.mpesa_callback_queue where business_short_code='BIZ-B3F5E5D9' and trans_id='UJ2558DDTO'
  and not coalesce(confirmed,false) and repayment_id is null and bripta_review_reason is not null
union all select 'held_payment_amount',coalesce(sum(trans_amount),0)::numeric
from public.mpesa_callback_queue where business_short_code='BIZ-B3F5E5D9' and trans_id='UJ2558DDTO'
  and not coalesce(confirmed,false)
union all select 'other_pending_callbacks',count(*)::numeric
from public.mpesa_callback_queue q where q.business_short_code='BIZ-B3F5E5D9'
  and q.created_at>=timestamptz '2026-10-01 22:00:00+03:00'
  and not coalesce(q.confirmed,false) and q.trans_id<>'UJ2558DDTO'
  and not coalesce((to_jsonb(q)->>'dismissed')::boolean,false)
  and not exists(select 1 from public.loan_repayments r where r.business_id='BIZ-B3F5E5D9'
                 and (r.payment_reference=q.trans_id or r.receipt_no=q.trans_id))
union all select 'recovery_sms_'||s.status,count(*)::numeric
from public.bripta_sms_outbox s join public.bripta_payment_recovery_20261002 a on a.repayment_id=s.repayment_id
where a.business_id='BIZ-B3F5E5D9' group by s.status;
