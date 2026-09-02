-- Bripta SMS Centre: custom SMS and bundle requests.
-- This only updates SMS wallet/outbox/request objects. It does not change loans,
-- repayments, loan balances, savings, clients or schedules.

begin;

create extension if not exists pgcrypto;

do $$
declare
  v_constraint text;
begin
  for v_constraint in
    select conname
    from pg_constraint
    where conrelid='public.bripta_sms_outbox'::regclass
      and contype='c'
      and pg_get_constraintdef(oid) ilike '%message_type%'
  loop
    execute format('alter table public.bripta_sms_outbox drop constraint %I', v_constraint);
  end loop;

  for v_constraint in
    select conname
    from pg_constraint
    where conrelid='public.bripta_sms_outbox'::regclass
      and contype='c'
      and pg_get_constraintdef(oid) ilike '%status%'
  loop
    execute format('alter table public.bripta_sms_outbox drop constraint %I', v_constraint);
  end loop;
end $$;

alter table public.bripta_sms_outbox
  add constraint bripta_sms_outbox_message_type_check
  check (message_type in ('repayment','onboarding','test','custom','bundle_request'));

alter table public.bripta_sms_outbox
  add constraint bripta_sms_outbox_status_check
  check (status in ('queued','sending','sent','failed','delivery_unknown','blocked_no_credit','cancelled'));

create table if not exists public.bripta_sms_bundle_requests (
  id uuid primary key default gen_random_uuid(),
  business_id text not null references public.bripta_sms_wallets(business_id) on delete cascade,
  credits integer not null check (credits > 0),
  phone text,
  customer_amount numeric(14,2),
  payment_reference text,
  status text not null default 'pending'
    check (status in ('pending','paid','credited','cancelled')),
  requested_by uuid,
  credited_by uuid,
  credited_at timestamptz,
  note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists bripta_sms_bundle_requests_business_status_idx
  on public.bripta_sms_bundle_requests(business_id,status,created_at desc);

alter table public.bripta_sms_bundle_requests enable row level security;

drop policy if exists bripta_sms_bundle_requests_read on public.bripta_sms_bundle_requests;
create policy bripta_sms_bundle_requests_read on public.bripta_sms_bundle_requests
for select to authenticated using (exists (
  select 1
  from public.loan_staff s
  where (s.auth_user_id=auth.uid() or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email','')))
    and s.business_id=bripta_sms_bundle_requests.business_id
    and s.is_active=true
));

drop policy if exists bripta_sms_bundle_requests_insert on public.bripta_sms_bundle_requests;
create policy bripta_sms_bundle_requests_insert on public.bripta_sms_bundle_requests
for insert to authenticated with check (exists (
  select 1
  from public.loan_staff s
  where (s.auth_user_id=auth.uid() or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email','')))
    and s.business_id=bripta_sms_bundle_requests.business_id
    and s.is_active=true
));

grant select on public.bripta_sms_bundle_requests to authenticated;
grant insert on public.bripta_sms_bundle_requests to authenticated;
grant all on public.bripta_sms_bundle_requests to service_role;

create or replace function public.bripta_claim_custom_sms(
  p_business_id text,
  p_recipient text,
  p_message text,
  p_client_name text default null
)
returns jsonb
language plpgsql
security definer
set search_path=public
as $$
declare
  v_wallet public.bripta_sms_wallets%rowtype;
  v_digits text;
  v_tail text;
  v_recipient text;
  v_message text;
  v_segments integer;
  v_outbox_id uuid;
begin
  v_message=trim(coalesce(p_message,''));
  if v_message='' then
    return jsonb_build_object('ok',false,'code','missing_message','message','Message is required');
  end if;

  v_digits=regexp_replace(coalesce(p_recipient,''),'[^0-9]','','g');
  v_tail=right(v_digits,9);
  if v_tail !~ '^[17][0-9]{8}$' then
    return jsonb_build_object('ok',false,'code','invalid_phone','message','Recipient phone number is invalid');
  end if;
  v_recipient='254'||v_tail;
  v_segments=case when char_length(v_message)<=160 then 1 else ceil(char_length(v_message)/153.0)::integer end;

  insert into public.bripta_sms_wallets(business_id,sender_id)
  values(p_business_id,'BRIPTA')
  on conflict (business_id) do nothing;

  select * into v_wallet
  from public.bripta_sms_wallets
  where business_id=p_business_id
  for update;

  if v_wallet.business_id is null or not v_wallet.enabled then
    return jsonb_build_object('ok',false,'code','sms_disabled','message','SMS wallet is disabled');
  end if;

  if v_wallet.credits_purchased-v_wallet.credits_used < v_segments then
    insert into public.bripta_sms_outbox(
      business_id,client_name,recipient,message,message_type,segments,status,last_error
    )
    values(
      p_business_id,nullif(trim(coalesce(p_client_name,'')),''),v_recipient,v_message,'custom',
      v_segments,'blocked_no_credit','SMS credits are insufficient'
    )
    returning id into v_outbox_id;

    return jsonb_build_object(
      'ok',false,
      'code','insufficient_credits',
      'outbox_id',v_outbox_id,
      'remaining',greatest(0,v_wallet.credits_purchased-v_wallet.credits_used)
    );
  end if;

  update public.bripta_sms_wallets
  set credits_used=credits_used+v_segments,
      updated_at=now()
  where business_id=p_business_id;

  insert into public.bripta_sms_outbox(
    business_id,client_name,recipient,message,message_type,segments,credits_reserved,status,attempts
  )
  values(
    p_business_id,nullif(trim(coalesce(p_client_name,'')),''),v_recipient,v_message,'custom',
    v_segments,v_segments,'sending',1
  )
  returning id into v_outbox_id;

  return jsonb_build_object(
    'ok',true,
    'outbox_id',v_outbox_id,
    'business_id',p_business_id,
    'recipient',v_recipient,
    'message',v_message,
    'segments',v_segments,
    'sender_id',coalesce(v_wallet.sender_id,'BRIPTA')
  );
end;
$$;

revoke all on function public.bripta_claim_custom_sms(text,text,text,text) from public;
revoke all on function public.bripta_claim_custom_sms(text,text,text,text) from anon;
revoke all on function public.bripta_claim_custom_sms(text,text,text,text) from authenticated;
grant execute on function public.bripta_claim_custom_sms(text,text,text,text) to service_role;

commit;

select
  'Bripta SMS Centre custom sending and bundle requests are ready' as result,
  true as custom_sms_ready,
  true as bundle_requests_ready,
  false as loan_balances_changed,
  false as repayments_changed,
  false as client_data_changed;
