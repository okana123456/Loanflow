-- Bripta / TalkSasa transactional SMS wallet.
--
-- Bripta sees only the SMS credits sold to the business. TalkSasa wholesale
-- costs, API credentials and provider balance are never stored here.
-- This setup does not modify repayments, loans, schedules or balances.

begin;

create extension if not exists pgcrypto;

create table if not exists public.bripta_sms_wallets (
  business_id text primary key,
  credits_purchased bigint not null default 0 check (credits_purchased >= 0),
  credits_used bigint not null default 0 check (credits_used >= 0),
  low_balance_threshold integer not null default 100 check (low_balance_threshold >= 0),
  sender_id text not null default 'BRIPTA',
  enabled boolean not null default true,
  activated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.bripta_sms_topups (
  id uuid primary key default gen_random_uuid(),
  business_id text not null references public.bripta_sms_wallets(business_id) on delete cascade,
  credits integer not null check (credits > 0),
  customer_amount_paid numeric(14,2),
  customer_reference text,
  note text,
  created_at timestamptz not null default now()
);

create table if not exists public.bripta_sms_outbox (
  id uuid primary key default gen_random_uuid(),
  business_id text not null,
  repayment_id uuid unique references public.loan_repayments(id) on delete set null,
  loan_id uuid references public.loans(id) on delete set null,
  client_id uuid references public.loan_clients(id) on delete set null,
  client_name text,
  recipient text,
  message text,
  segments integer not null default 0 check (segments >= 0),
  credits_reserved integer not null default 0 check (credits_reserved >= 0),
  status text not null default 'queued'
    check (status in ('queued','sending','sent','failed','delivery_unknown','blocked_no_credit')),
  provider_uid text,
  provider_response jsonb,
  attempts integer not null default 0,
  last_error text,
  queued_at timestamptz not null default now(),
  sent_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists bripta_sms_outbox_business_status_idx
  on public.bripta_sms_outbox(business_id,status,queued_at desc);
create index if not exists bripta_sms_outbox_repayment_idx
  on public.bripta_sms_outbox(repayment_id);
create index if not exists bripta_sms_topups_business_date_idx
  on public.bripta_sms_topups(business_id,created_at desc);

-- Bripta's live business wallet starts at zero. Credits are added privately
-- only after a bundle is paid for (or when you grant test credits).
insert into public.bripta_sms_wallets(business_id,sender_id)
values('BIZ-B3F5E5D9','BRIPTA')
on conflict (business_id) do update set sender_id='BRIPTA',updated_at=now();

alter table public.bripta_sms_wallets enable row level security;
alter table public.bripta_sms_topups enable row level security;
alter table public.bripta_sms_outbox enable row level security;

drop policy if exists bripta_sms_wallet_read on public.bripta_sms_wallets;
create policy bripta_sms_wallet_read on public.bripta_sms_wallets
for select to authenticated using (exists (
  select 1 from public.loan_staff s
  where (s.auth_user_id=auth.uid() or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email','')))
    and s.business_id=bripta_sms_wallets.business_id
    and s.is_active=true
));

drop policy if exists bripta_sms_topups_read on public.bripta_sms_topups;
create policy bripta_sms_topups_read on public.bripta_sms_topups
for select to authenticated using (exists (
  select 1 from public.loan_staff s
  where (s.auth_user_id=auth.uid() or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email','')))
    and s.business_id=bripta_sms_topups.business_id
    and s.is_active=true
));

drop policy if exists bripta_sms_outbox_read on public.bripta_sms_outbox;
create policy bripta_sms_outbox_read on public.bripta_sms_outbox
for select to authenticated using (exists (
  select 1 from public.loan_staff s
  where (s.auth_user_id=auth.uid() or lower(coalesce(s.email,''))=lower(coalesce(auth.jwt()->>'email','')))
    and s.business_id=bripta_sms_outbox.business_id
    and s.is_active=true
));

grant select on public.bripta_sms_wallets,public.bripta_sms_topups,public.bripta_sms_outbox to authenticated;
grant all on public.bripta_sms_wallets,public.bripta_sms_topups,public.bripta_sms_outbox to service_role;

-- Every future repayment receives one outbox record. Historical imports and
-- payments dated before SMS activation are deliberately excluded.
create or replace function public.bripta_queue_repayment_sms()
returns trigger
language plpgsql
security definer
set search_path=public
as $$
declare
  v_activated_at timestamptz;
begin
  if coalesce(new.amount,0)<=0 or new.loan_id is null or new.business_id is null then
    return new;
  end if;
  if upper(coalesce(new.payment_reference,''))='IMPORT'
     or coalesce(new.receipt_no,'') like '%-IMP-%'
     or lower(coalesce(new.payment_method,''))='client_excess'
     or upper(coalesce(new.payment_reference,'')) like 'EXCESS-%'
     or lower(coalesce(new.notes,'')) like '%imported from excel%' then
    return new;
  end if;

  insert into public.bripta_sms_wallets(business_id)
  values(new.business_id)
  on conflict (business_id) do nothing;

  select activated_at into v_activated_at
  from public.bripta_sms_wallets where business_id=new.business_id;
  if coalesce(new.payment_date,new.created_at,now()) < v_activated_at then
    return new;
  end if;

  insert into public.bripta_sms_outbox(business_id,repayment_id,loan_id,client_id,client_name)
  select new.business_id,new.id,l.id,c.id,c.full_name
  from public.loans l
  join public.loan_clients c on c.id=l.client_id and c.business_id=l.business_id
  where l.id=new.loan_id and l.business_id=new.business_id
  on conflict (repayment_id) do nothing;

  return new;
end;
$$;

drop trigger if exists trg_bripta_queue_repayment_sms on public.loan_repayments;
create trigger trg_bripta_queue_repayment_sms
after insert on public.loan_repayments
for each row execute function public.bripta_queue_repayment_sms();

-- Atomically reserves SMS credits and returns the final, post-payment loan
-- balance. Only the Edge Function service role may execute this function.
create or replace function public.bripta_claim_repayment_sms(p_repayment_id uuid)
returns jsonb
language plpgsql
security definer
set search_path=public
as $$
declare
  v_outbox public.bripta_sms_outbox%rowtype;
  v_wallet public.bripta_sms_wallets%rowtype;
  v_repayment public.loan_repayments%rowtype;
  v_loan public.loans%rowtype;
  v_client public.loan_clients%rowtype;
  v_first_name text;
  v_amount_text text;
  v_balance_text text;
  v_message text;
  v_segments integer;
begin
  select * into v_outbox from public.bripta_sms_outbox
  where repayment_id=p_repayment_id for update;
  if v_outbox.id is null then
    return jsonb_build_object('ok',false,'code','not_queued');
  end if;
  if v_outbox.status='sent' then
    return jsonb_build_object('ok',true,'already_sent',true,'outbox_id',v_outbox.id);
  end if;
  if v_outbox.status in ('sending','delivery_unknown') then
    return jsonb_build_object('ok',false,'code',v_outbox.status,'outbox_id',v_outbox.id);
  end if;

  select * into v_wallet from public.bripta_sms_wallets
  where business_id=v_outbox.business_id for update;
  if v_wallet.business_id is null or not v_wallet.enabled then
    return jsonb_build_object('ok',false,'code','sms_disabled');
  end if;

  select * into v_repayment from public.loan_repayments where id=p_repayment_id;
  select * into v_loan from public.loans where id=v_outbox.loan_id;
  select * into v_client from public.loan_clients where id=v_outbox.client_id;
  if v_repayment.id is null or v_loan.id is null or v_client.id is null then
    update public.bripta_sms_outbox set status='failed',last_error='Linked repayment, loan or client is missing',updated_at=now()
    where id=v_outbox.id;
    return jsonb_build_object('ok',false,'code','missing_link');
  end if;
  if nullif(regexp_replace(coalesce(v_client.phone,''),'[^0-9]','','g'),'') is null then
    update public.bripta_sms_outbox set status='failed',last_error='Client phone number is missing',updated_at=now()
    where id=v_outbox.id;
    return jsonb_build_object('ok',false,'code','missing_phone');
  end if;

  v_first_name=split_part(trim(coalesce(v_client.full_name,'Client')),' ',1);
  v_amount_text=case
    when round(v_repayment.amount,2)=trunc(v_repayment.amount) then to_char(v_repayment.amount,'FM999G999G999G990')
    else to_char(v_repayment.amount,'FM999G999G999G990D00') end;
  v_balance_text=case
    when round(greatest(0,coalesce(v_loan.outstanding_balance,0)),2)=trunc(greatest(0,coalesce(v_loan.outstanding_balance,0)))
      then to_char(greatest(0,coalesce(v_loan.outstanding_balance,0)),'FM999G999G999G990')
    else to_char(greatest(0,coalesce(v_loan.outstanding_balance,0)),'FM999G999G999G990D00') end;
  v_message='Thank you '||v_first_name||', we received your KES '||v_amount_text||'. Your Bripta Enterprises balance is KES '||v_balance_text||'.';
  v_segments=case when char_length(v_message)<=160 then 1 else ceil(char_length(v_message)/153.0)::integer end;

  if v_wallet.credits_purchased-v_wallet.credits_used < v_segments then
    update public.bripta_sms_outbox
      set status='blocked_no_credit',message=v_message,segments=v_segments,
          recipient=v_client.phone,last_error='SMS credits are insufficient',updated_at=now()
    where id=v_outbox.id;
    return jsonb_build_object('ok',false,'code','insufficient_credits',
      'remaining',greatest(0,v_wallet.credits_purchased-v_wallet.credits_used));
  end if;

  update public.bripta_sms_wallets
    set credits_used=credits_used+v_segments,updated_at=now()
  where business_id=v_wallet.business_id;
  update public.bripta_sms_outbox
    set status='sending',recipient=v_client.phone,message=v_message,segments=v_segments,
        credits_reserved=v_segments,attempts=attempts+1,last_error=null,updated_at=now()
  where id=v_outbox.id;

  return jsonb_build_object('ok',true,'outbox_id',v_outbox.id,'business_id',v_outbox.business_id,
    'recipient',v_client.phone,'message',v_message,'segments',v_segments,'sender_id',v_wallet.sender_id);
end;
$$;

create or replace function public.bripta_complete_repayment_sms(
  p_outbox_id uuid,
  p_status text,
  p_provider_uid text default null,
  p_provider_response jsonb default null,
  p_error text default null,
  p_refund boolean default false
) returns jsonb
language plpgsql
security definer
set search_path=public
as $$
declare v_outbox public.bripta_sms_outbox%rowtype;
begin
  if p_status not in ('sent','failed','delivery_unknown') then raise exception 'Invalid SMS completion status'; end if;
  select * into v_outbox from public.bripta_sms_outbox where id=p_outbox_id for update;
  if v_outbox.id is null then return jsonb_build_object('ok',false,'code','not_found'); end if;
  if v_outbox.status='sent' then return jsonb_build_object('ok',true,'already_sent',true); end if;

  if p_refund and v_outbox.credits_reserved>0 then
    update public.bripta_sms_wallets
      set credits_used=greatest(0,credits_used-v_outbox.credits_reserved),updated_at=now()
    where business_id=v_outbox.business_id;
  end if;
  update public.bripta_sms_outbox
    set status=p_status,provider_uid=p_provider_uid,provider_response=p_provider_response,
        last_error=p_error,sent_at=case when p_status='sent' then now() else null end,
        credits_reserved=case when p_refund then 0 else credits_reserved end,updated_at=now()
  where id=p_outbox_id;
  return jsonb_build_object('ok',true,'status',p_status);
end;
$$;

-- Private bundle allocation function. Run this only from the Supabase SQL
-- editor after Bripta pays you. It stores Bripta's retail payment only.
create or replace function public.bripta_owner_add_sms_credits(
  p_business_id text,
  p_credits integer,
  p_customer_amount_paid numeric default null,
  p_customer_reference text default null,
  p_note text default null
) returns jsonb
language plpgsql
security definer
set search_path=public
as $$
declare v_remaining bigint;
begin
  if coalesce(p_credits,0)<=0 then raise exception 'Credits must be greater than zero'; end if;
  insert into public.bripta_sms_wallets(business_id,credits_purchased)
  values(p_business_id,p_credits)
  on conflict (business_id) do update
    set credits_purchased=bripta_sms_wallets.credits_purchased+excluded.credits_purchased,updated_at=now();
  insert into public.bripta_sms_topups(business_id,credits,customer_amount_paid,customer_reference,note)
  values(p_business_id,p_credits,p_customer_amount_paid,p_customer_reference,p_note);
  select greatest(0,credits_purchased-credits_used) into v_remaining
  from public.bripta_sms_wallets where business_id=p_business_id;
  return jsonb_build_object('ok',true,'credits_added',p_credits,'remaining',v_remaining);
end;
$$;

revoke all on function public.bripta_claim_repayment_sms(uuid) from public,anon,authenticated;
revoke all on function public.bripta_complete_repayment_sms(uuid,text,text,jsonb,text,boolean) from public,anon,authenticated;
revoke all on function public.bripta_owner_add_sms_credits(text,integer,numeric,text,text) from public,anon,authenticated;
grant execute on function public.bripta_claim_repayment_sms(uuid) to service_role;
grant execute on function public.bripta_complete_repayment_sms(uuid,text,text,jsonb,text,boolean) to service_role;
grant execute on function public.bripta_owner_add_sms_credits(text,integer,numeric,text,text) to service_role;

commit;

select
  'Bripta TalkSasa SMS wallet is ready' as result,
  false as financial_data_changed,
  false as loan_balances_changed,
  false as historical_sms_created;
