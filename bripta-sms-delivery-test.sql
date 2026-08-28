-- Service-only TalkSasa delivery test support for Bripta.
-- This reserves one SMS credit and creates an outbox audit row. It does not
-- insert or update any client, loan, repayment, schedule or financial value.

create or replace function public.bripta_claim_test_sms(
  p_business_id text,
  p_phone text
) returns jsonb
language plpgsql
security definer
set search_path=public
as $$
declare
  v_wallet public.bripta_sms_wallets%rowtype;
  v_outbox public.bripta_sms_outbox%rowtype;
  v_phone text;
  v_message constant text := 'Bripta Enterprises SMS test successful. Repayment notifications are now active.';
begin
  v_phone := regexp_replace(coalesce(p_phone,''),'[^0-9]','','g');
  if v_phone like '0%' then v_phone := '254'||substr(v_phone,2); end if;
  if length(v_phone)=9 then v_phone := '254'||v_phone; end if;
  if v_phone !~ '^254[17][0-9]{8}$' then
    return jsonb_build_object('ok',false,'code','invalid_phone');
  end if;

  select * into v_wallet
  from public.bripta_sms_wallets
  where business_id=p_business_id
  for update;
  if v_wallet.business_id is null or not v_wallet.enabled then
    return jsonb_build_object('ok',false,'code','sms_disabled');
  end if;

  select * into v_outbox
  from public.bripta_sms_outbox
  where business_id=p_business_id
    and client_name='System test'
    and regexp_replace(coalesce(recipient,''),'[^0-9]','','g')=v_phone
    and queued_at >= now()-interval '5 minutes'
    and status in ('sending','sent','delivery_unknown')
  order by queued_at desc
  limit 1;
  if v_outbox.id is not null then
    return jsonb_build_object(
      'ok',true,'already_sent',true,'outbox_id',v_outbox.id,
      'status',v_outbox.status,'recipient',v_phone
    );
  end if;

  if v_wallet.credits_purchased-v_wallet.credits_used < 1 then
    return jsonb_build_object('ok',false,'code','insufficient_credits','remaining',0);
  end if;

  update public.bripta_sms_wallets
  set credits_used=credits_used+1,updated_at=now()
  where business_id=p_business_id;

  insert into public.bripta_sms_outbox(
    business_id,client_name,recipient,message,segments,credits_reserved,
    status,attempts
  ) values (
    p_business_id,'System test',v_phone,v_message,1,1,'sending',1
  ) returning * into v_outbox;

  return jsonb_build_object(
    'ok',true,'outbox_id',v_outbox.id,'business_id',p_business_id,
    'recipient',v_phone,'message',v_message,'segments',1,
    'sender_id',v_wallet.sender_id
  );
end;
$$;

revoke all on function public.bripta_claim_test_sms(text,text)
from public,anon,authenticated;
grant execute on function public.bripta_claim_test_sms(text,text)
to service_role;

select
  'Bripta service-only SMS delivery test is ready' as result,
  false as business_data_changed,
  false as repayment_data_changed,
  false as loan_balances_changed;
