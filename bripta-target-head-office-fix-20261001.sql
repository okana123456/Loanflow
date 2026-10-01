-- Bripta scope correction: branch labels only. No financial or SMS values change.
begin;

update public.bripta_branches
set business_id='BIZ-B3F5E5D9',is_head_office=true,is_active=true,updated_at=now()
where id='00000000-0000-4000-8000-000000000001';

do $$
declare t text;
begin
  foreach t in array array[
    'loan_clients','loan_applications','loans','loan_schedules','loan_repayments',
    'loan_staff','loan_penalties','unmatched_payments','journal_entries',
    'bripta_charges','bripta_excess_ledger','bripta_excess_allocations'
  ] loop
    if to_regclass('public.'||t) is not null then
      execute format(
        'update public.%I set branch_id=$1 where business_id=$2 and branch_id is distinct from $1',t
      ) using '00000000-0000-4000-8000-000000000001'::uuid,'BIZ-B3F5E5D9';
    end if;
  end loop;
end $$;

update public.bripta_staff_permissions p
set branch_id='00000000-0000-4000-8000-000000000001',business_id='BIZ-B3F5E5D9',updated_at=now()
from public.loan_staff s
where p.staff_id=s.id and s.business_id='BIZ-B3F5E5D9';

commit;

select 'staff_total' check_name,count(*)::numeric result
from public.loan_staff where business_id='BIZ-B3F5E5D9'
union all
select 'staff_in_head_office',count(*) from public.loan_staff
where business_id='BIZ-B3F5E5D9' and branch_id='00000000-0000-4000-8000-000000000001'
union all
select 'clients_total',count(*) from public.loan_clients where business_id='BIZ-B3F5E5D9'
union all
select 'clients_in_head_office',count(*) from public.loan_clients
where business_id='BIZ-B3F5E5D9' and branch_id='00000000-0000-4000-8000-000000000001'
union all
select 'loans_total',count(*) from public.loans where business_id='BIZ-B3F5E5D9'
union all
select 'repayments_total',count(*) from public.loan_repayments where business_id='BIZ-B3F5E5D9'
union all
select 'sms_messages',count(*) from public.bripta_sms_outbox where business_id='BIZ-B3F5E5D9'
union all
select 'sms_wallets',count(*) from public.bripta_sms_wallets where business_id='BIZ-B3F5E5D9';
