-- Bripta automatic SMS sender.
--
-- This makes Supabase call the send-repayment-sms Edge Function regularly so
-- repayment SMS do not wait for a manual "process today" action.
--
-- Safe scope:
-- - sends only queued/failed/blocked_no_credit Bripta repayment SMS for today's Kenya date
-- - updates only bripta_sms_outbox statuses and bripta_sms_wallets credits_used
-- - does not update loans, repayments, schedules, clients or balances
--
-- Before running:
-- 1) Replace 4321 with the exact BRIPTA_SMS_TEST_PIN saved in Edge Function
--    secrets when you change the temporary test PIN.
-- 2) Keep Verify JWT off for send-repayment-sms, because this call is protected
--    by the private PIN and runs from Supabase cron.

begin;

create extension if not exists pg_net;
create extension if not exists pg_cron;

do $$
declare
  v_jobid bigint;
begin
  for v_jobid in
    select jobid
    from cron.job
    where jobname = 'bripta-auto-send-queued-repayment-sms'
  loop
    perform cron.unschedule(v_jobid);
  end loop;
end $$;

select cron.schedule(
  'bripta-auto-send-queued-repayment-sms',
  '*/2 * * * *',
  $$
  select net.http_post(
    url := 'https://nngscmpsxtqqjzcnsrbi.supabase.co/functions/v1/send-repayment-sms',
    headers := jsonb_build_object('Content-Type','application/json'),
    body := jsonb_build_object(
      'process_today_sms', true,
      'test_pin', '4321',
      'limit', 100
    ),
    timeout_milliseconds := 30000
  );
  $$
);

commit;

select
  'Bripta automatic queued SMS sender is scheduled' as result,
  '*/2 * * * *' as schedule,
  false as financial_data_changed,
  false as loan_balances_changed;
