-- READ ONLY. Bripta BIZ-B3F5E5D9 only. Finds where loan issuance stops,
-- partially created loans, officer portfolio gaps and active access rules.
-- This query changes no loans, schedules, repayments or permissions.
with recent_applications as (
  select a.id,a.application_no,a.status,a.client_id,a.loan_officer_id,
    a.branch_id,a.applied_amount,a.applied_term_weeks,a.created_at,
    c.full_name as client_name,c.branch_id as client_branch_id
  from public.loan_applications a
  left join public.loan_clients c on c.id=a.client_id and c.business_id=a.business_id
  where a.business_id='BIZ-B3F5E5D9'
    and a.created_at>=now()-interval '14 days'
  order by a.created_at desc limit 40
), application_rows as (
  select 'application'::text as section,
    jsonb_build_object(
      'application_no',a.application_no,'status',a.status,'client',a.client_name,
      'amount',a.applied_amount,'created_at',a.created_at,
      'application_branch',a.branch_id,'client_branch',a.client_branch_id,
      'officer_id',a.loan_officer_id,
      'loan_count',(select count(*) from public.loans l where l.application_id=a.id and l.business_id='BIZ-B3F5E5D9'),
      'loans',coalesce((select jsonb_agg(jsonb_build_object(
        'loan_no',l.loan_no,'status',l.status,'branch',l.branch_id,
        'balance',l.outstanding_balance,
        'schedule_count',(select count(*) from public.loan_schedules s where s.loan_id=l.id),
        'expected_schedule_count',l.term_weeks,
        'repayment_count',(select count(*) from public.loan_repayments r where r.loan_id=l.id)
      )) from public.loans l where l.application_id=a.id and l.business_id='BIZ-B3F5E5D9'),'[]'::jsonb)
    ) as details
  from recent_applications a
), staff_rows as (
  select 'officer'::text as section,
    jsonb_build_object(
      'name',s.name,'role',s.role,'active',s.is_active,
      'staff_id',s.id,'auth_linked',s.auth_user_id is not null,
      'staff_branch',s.branch_id,
      'active_loans',(select count(*) from public.loans l
        where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=s.id
          and l.status='active' and l.outstanding_balance>0.01),
      'past_due_installments',(select count(*) from public.loan_schedules d
        join public.loans l on l.id=d.loan_id
        where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=s.id
          and l.status='active' and l.outstanding_balance>0.01
          and d.due_date<current_date and d.total_due-d.total_paid>0.01),
      'loan_branch_mismatch',(select count(*) from public.loans l
        where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=s.id
          and l.status='active' and l.branch_id is distinct from s.branch_id),
      'schedule_branch_mismatch',(select count(*) from public.loan_schedules d
        join public.loans l on l.id=d.loan_id
        where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=s.id
          and l.status='active' and d.branch_id is distinct from l.branch_id)
    ) as details
  from public.loan_staff s
  where s.business_id='BIZ-B3F5E5D9' and s.is_active
    and s.role ilike '%loan_officer%'
), policy_rows as (
  select 'policy'::text as section,
    jsonb_build_object('table',tablename,'name',policyname,'mode',permissive,
      'operation',cmd,'roles',roles,'using',qual,'check',with_check) as details
  from pg_policies
  where schemaname='public'
    and tablename in ('loan_staff','loan_clients','loan_applications','loans','loan_schedules')
), trigger_rows as (
  select 'trigger'::text as section,
    jsonb_build_object('table',c.relname,'name',t.tgname,
      'enabled',t.tgenabled,'definition',pg_get_triggerdef(t.oid)) as details
  from pg_trigger t join pg_class c on c.oid=t.tgrelid
  join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname in ('loan_applications','loans','loan_schedules')
    and not t.tgisinternal
)
select section,details from (
  select section,details from application_rows
  union all select section,details from staff_rows
  union all select section,details from policy_rows
  union all select section,details from trigger_rows
) report
order by section,details->>'name',details->>'application_no';
