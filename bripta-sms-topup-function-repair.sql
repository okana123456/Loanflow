-- Recreate the private Bripta SMS credit function if the main setup did not
-- install it, then grant five internal test credits.

do $$
begin
  if to_regclass('public.bripta_sms_wallets') is null
     or to_regclass('public.bripta_sms_topups') is null then
    raise exception 'SMS wallet tables are missing. Run bripta-talksasa-sms-wallet.sql first.';
  end if;
end;
$$;

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
declare
  v_remaining bigint;
begin
  if coalesce(p_credits,0)<=0 then
    raise exception 'Credits must be greater than zero';
  end if;

  insert into public.bripta_sms_wallets(business_id,credits_purchased)
  values(p_business_id,p_credits)
  on conflict (business_id) do update
    set credits_purchased=bripta_sms_wallets.credits_purchased+excluded.credits_purchased,
        updated_at=now();

  insert into public.bripta_sms_topups(
    business_id,credits,customer_amount_paid,customer_reference,note
  ) values (
    p_business_id,p_credits,p_customer_amount_paid,p_customer_reference,p_note
  );

  select greatest(0,credits_purchased-credits_used)
  into v_remaining
  from public.bripta_sms_wallets
  where business_id=p_business_id;

  return jsonb_build_object(
    'ok',true,
    'credits_added',p_credits,
    'remaining',v_remaining
  );
end;
$$;

revoke all on function public.bripta_owner_add_sms_credits(text,integer,numeric,text,text)
from public,anon,authenticated;
grant execute on function public.bripta_owner_add_sms_credits(text,integer,numeric,text,text)
to service_role;

select public.bripta_owner_add_sms_credits(
  'BIZ-B3F5E5D9'::text,
  5::integer,
  0::numeric,
  'INTERNAL-TEST'::text,
  'Bripta SMS integration test credits'::text
) as result;
