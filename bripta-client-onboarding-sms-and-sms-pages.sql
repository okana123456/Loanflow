-- Bripta onboarding SMS support.
--
-- This extends the existing TalkSasa SMS wallet/outbox so newly registered
-- clients can receive a welcome SMS. It does not change loans, repayments,
-- schedules, balances, SMS credits purchased, or SMS credits already used.

begin;

alter table public.bripta_sms_outbox
  add column if not exists message_type text not null default 'repayment';

alter table public.bripta_sms_outbox
  drop constraint if exists bripta_sms_outbox_message_type_check;

alter table public.bripta_sms_outbox
  add constraint bripta_sms_outbox_message_type_check
  check (message_type in ('repayment','onboarding','test'));

update public.bripta_sms_outbox
set message_type = 'test'
where client_name = 'System test'
  and repayment_id is null;

update public.bripta_sms_outbox
set message_type = 'repayment'
where message_type is null;

create index if not exists bripta_sms_outbox_business_type_date_idx
  on public.bripta_sms_outbox(business_id,message_type,queued_at desc);

create unique index if not exists bripta_sms_outbox_one_onboarding_per_client_uidx
  on public.bripta_sms_outbox(client_id)
  where message_type = 'onboarding';

create or replace function public.bripta_queue_client_onboarding_sms()
returns trigger
language plpgsql
security definer
set search_path=public
as $$
declare
  v_first_name text;
  v_message text;
  v_segments integer;
begin
  if new.business_id is null then
    return new;
  end if;

  -- Avoid mass-SMS when historical Excel imports create many old clients.
  if lower(coalesce(new.notes,'')) like '%imported from excel%' then
    return new;
  end if;

  if nullif(regexp_replace(coalesce(new.phone,''),'[^0-9]','','g'),'') is null then
    return new;
  end if;

  insert into public.bripta_sms_wallets(business_id)
  values(new.business_id)
  on conflict (business_id) do nothing;

  v_first_name := split_part(trim(coalesce(new.full_name,'Client')),' ',1);
  v_message := 'Welcome '||v_first_name||' to Bripta Enterprises. Your client account is active. Keep your receipts safely and contact Bripta for any support.';
  v_segments := case when char_length(v_message)<=160 then 1 else ceil(char_length(v_message)/153.0)::integer end;

  insert into public.bripta_sms_outbox(
    business_id, client_id, client_name, recipient, message, segments, message_type, status
  ) values (
    new.business_id, new.id, new.full_name, new.phone, v_message, v_segments, 'onboarding', 'queued'
  )
  on conflict do nothing;

  return new;
end;
$$;

drop trigger if exists trg_bripta_queue_client_onboarding_sms on public.loan_clients;
create trigger trg_bripta_queue_client_onboarding_sms
after insert on public.loan_clients
for each row execute function public.bripta_queue_client_onboarding_sms();

create or replace function public.bripta_claim_client_onboarding_sms(p_client_id uuid)
returns jsonb
language plpgsql
security definer
set search_path=public
as $$
declare
  v_outbox public.bripta_sms_outbox%rowtype;
  v_wallet public.bripta_sms_wallets%rowtype;
begin
  select * into v_outbox
  from public.bripta_sms_outbox
  where client_id = p_client_id
    and message_type = 'onboarding'
  order by queued_at desc
  limit 1
  for update;

  if v_outbox.id is null then
    return jsonb_build_object('ok',false,'code','not_queued');
  end if;
  if v_outbox.status = 'sent' then
    return jsonb_build_object('ok',true,'already_sent',true,'outbox_id',v_outbox.id);
  end if;
  if v_outbox.status in ('sending','delivery_unknown') then
    return jsonb_build_object('ok',false,'code',v_outbox.status,'outbox_id',v_outbox.id);
  end if;

  select * into v_wallet
  from public.bripta_sms_wallets
  where business_id = v_outbox.business_id
  for update;

  if v_wallet.business_id is null or not v_wallet.enabled then
    return jsonb_build_object('ok',false,'code','sms_disabled');
  end if;

  if v_wallet.credits_purchased - v_wallet.credits_used < greatest(1,v_outbox.segments) then
    update public.bripta_sms_outbox
      set status='blocked_no_credit',
          last_error='SMS credits are insufficient',
          updated_at=now()
    where id=v_outbox.id;
    return jsonb_build_object('ok',false,'code','insufficient_credits',
      'remaining',greatest(0,v_wallet.credits_purchased-v_wallet.credits_used));
  end if;

  update public.bripta_sms_wallets
    set credits_used = credits_used + greatest(1,v_outbox.segments),
        updated_at = now()
  where business_id = v_wallet.business_id;

  update public.bripta_sms_outbox
    set status='sending',
        credits_reserved=greatest(1,v_outbox.segments),
        attempts=attempts+1,
        last_error=null,
        updated_at=now()
  where id=v_outbox.id
  returning * into v_outbox;

  return jsonb_build_object(
    'ok',true,
    'outbox_id',v_outbox.id,
    'business_id',v_outbox.business_id,
    'recipient',v_outbox.recipient,
    'message',v_outbox.message,
    'segments',greatest(1,v_outbox.segments),
    'sender_id',v_wallet.sender_id
  );
end;
$$;

revoke all on function public.bripta_claim_client_onboarding_sms(uuid) from public,anon,authenticated;
grant execute on function public.bripta_claim_client_onboarding_sms(uuid) to service_role;

commit;

select
  'Bripta client onboarding SMS is ready' as result,
  false as business_data_changed,
  false as financial_values_changed,
  false as historical_sms_created;
