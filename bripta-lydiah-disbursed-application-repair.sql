-- Bripta: reconcile Lydiah Atieno's disbursed loan application and prevent
-- future application/loan status mismatches.
--
-- This script changes application status only when a real linked loan exists.
-- It does not change loan amounts, balances, repayments, schedules or charges.

begin;

create or replace function public.bripta_mark_linked_application_disbursed()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.application_id is not null then
    update public.loan_applications
       set status = 'disbursed'
     where id = new.application_id
       and business_id = new.business_id
       and client_id = new.client_id
       and status is distinct from 'disbursed';
  end if;
  return new;
end;
$$;

drop trigger if exists trg_bripta_mark_linked_application_disbursed
  on public.loans;

create trigger trg_bripta_mark_linked_application_disbursed
after insert or update of application_id on public.loans
for each row
execute function public.bripta_mark_linked_application_disbursed();

with target_clients as (
  select c.id, c.business_id
  from public.loan_clients c
  where regexp_replace(coalesce(c.phone, ''), '[^0-9]', '', 'g')
        in ('0740526168', '254740526168', '740526168')
    and lower(regexp_replace(coalesce(c.full_name, ''), '[^a-z]', '', 'g'))
        like '%lydiahatieno%'
), corrected as (
  update public.loan_applications a
     set status = 'disbursed'
    from target_clients c
   where a.client_id = c.id
     and a.business_id = c.business_id
     and a.status is distinct from 'disbursed'
     and exists (
       select 1
       from public.loans l
       where l.application_id = a.id
         and l.client_id = a.client_id
         and l.business_id = a.business_id
     )
  returning a.id
)
select count(*) as corrected_count
into temporary table bripta_lydiah_repair_count
from corrected;

commit;

select
  'Bripta application synchronization is ready' as result,
  (select corrected_count from bripta_lydiah_repair_count) as lydiah_applications_corrected,
  false as loan_balances_changed,
  false as repayments_changed;

select
  c.full_name,
  c.phone,
  a.application_no,
  a.status as application_status,
  l.loan_no,
  l.status as loan_status,
  l.disbursement_date,
  l.outstanding_balance,
  case
    when l.id is not null and a.status = 'disbursed' then 'Correct - linked loan and application agree'
    when l.id is null then 'No linked loan found - no status was changed'
    else 'Review required'
  end as verification
from public.loan_clients c
left join public.loan_applications a
  on a.client_id = c.id and a.business_id = c.business_id
left join public.loans l
  on l.application_id = a.id and l.business_id = a.business_id
where regexp_replace(coalesce(c.phone, ''), '[^0-9]', '', 'g')
      in ('0740526168', '254740526168', '740526168')
order by coalesce(l.disbursement_date, a.created_at::date) desc;
