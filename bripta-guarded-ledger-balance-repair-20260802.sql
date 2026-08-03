-- Bripta / Loanflow guarded repayment-ledger balance repair
-- Run once after reviewing the 02 August deep audit.
--
-- This repairs only active loans with Nancy's proven rollback pattern:
--   1. The complete repayment ledger is higher than stored total_paid.
--   2. The stored balance is overstated by the same amount.
--   3. No active penalty explains the extra balance.
--   4. The loan existed in the 02 August rollback backup.
-- Completed/imported and penalty-bearing loans are deliberately excluded.

begin;

create table if not exists public.bripta_guarded_balance_backup_20260802 as
select l.*, now() as repair_backed_up_at
from public.loans l
where false;

drop table if exists pg_temp.bripta_guarded_candidates;
create temporary table bripta_guarded_candidates on commit drop as
with ledger as (
  select
    r.loan_id,
    round(sum(coalesce(r.amount,0))::numeric,2) as repayment_total,
    max(coalesce(r.payment_date,r.created_at)) as latest_payment_at
  from public.loan_repayments r
  where r.business_id='BIZ-B3F5E5D9'
  group by r.loan_id
), active_penalties as (
  select p.loan_id, round(sum(coalesce(p.penalty_amount,0))::numeric,2) as penalty_total
  from public.loan_penalties p
  where coalesce(p.is_waived,false)=false
  group by p.loan_id
)
select
  l.id as loan_id,
  l.total_payable,
  l.total_paid as old_total_paid,
  l.outstanding_balance as old_balance,
  x.repayment_total,
  greatest(0,coalesce(l.total_payable,0)-x.repayment_total) as corrected_balance,
  x.latest_payment_at
from public.loans l
join ledger x on x.loan_id::text=l.id::text
left join active_penalties p on p.loan_id::text=l.id::text
where l.business_id='BIZ-B3F5E5D9'
  and l.status='active'
  and coalesce(p.penalty_total,0)<=0.01
  and x.repayment_total > coalesce(l.total_paid,0)+0.01
  and coalesce(l.outstanding_balance,0) > greatest(0,coalesce(l.total_payable,0)-x.repayment_total)+0.01
  and abs(
    (x.repayment_total-coalesce(l.total_paid,0))
    - (coalesce(l.outstanding_balance,0)-greatest(0,coalesce(l.total_payable,0)-x.repayment_total))
  ) <= 0.02
  and exists (
    select 1 from public.bripta_safe_rollback_loans_20260802 b
    where b.id::text=l.id::text
  );

insert into public.bripta_guarded_balance_backup_20260802
select l.*, now()
from public.loans l
join bripta_guarded_candidates c on c.loan_id::text=l.id::text
where not exists (
  select 1 from public.bripta_guarded_balance_backup_20260802 b
  where b.id::text=l.id::text
);

update public.loans l
set
  total_paid=c.repayment_total,
  outstanding_balance=c.corrected_balance,
  status=case when c.corrected_balance<=0.01 then 'completed' else 'active' end,
  arrears_amount=case when c.corrected_balance<=0.01 then 0 else least(coalesce(l.arrears_amount,0),c.corrected_balance) end,
  overdue_days=case when c.corrected_balance<=0.01 then 0 else coalesce(l.overdue_days,0) end
from bripta_guarded_candidates c
where l.id::text=c.loan_id::text;

-- Rebuild only the affected schedules, oldest installment first, using the
-- exact payment total already recorded for that same loan_id.
with targets as (
  select
    s.id,
    s.due_date,
    coalesce(s.total_due,0)::numeric as total_due,
    c.latest_payment_at,
    least(
      coalesce(s.total_due,0)::numeric,
      greatest(
        0,
        least(c.repayment_total,coalesce(c.total_payable,0))
        - coalesce(sum(coalesce(s.total_due,0)::numeric) over (
            partition by s.loan_id
            order by s.due_date,s.installment_no,s.id
            rows between unbounded preceding and 1 preceding
          ),0)
      )
    ) as target_paid
  from public.loan_schedules s
  join bripta_guarded_candidates c on c.loan_id::text=s.loan_id::text
)
update public.loan_schedules s
set
  total_paid=round(t.target_paid,2),
  status=case
    when t.target_paid>=t.total_due-0.01 then 'paid'
    when t.target_paid>0 then case when t.due_date<current_date then 'partial' else 'partial' end
    when t.due_date<current_date then 'overdue'
    else 'pending'
  end,
  paid_at=case
    when t.target_paid>=t.total_due-0.01 then coalesce(s.paid_at,t.latest_payment_at,now())
    else null
  end
from targets t
where s.id::text=t.id::text;

-- Recalculate arrears and overdue days only for the repaired loans.
with overdue as (
  select
    c.loan_id,
    round(coalesce(sum(greatest(0,coalesce(s.total_due,0)-coalesce(s.total_paid,0)))
      filter (where s.due_date<current_date),0)::numeric,2) as arrears,
    min(s.due_date) filter (
      where s.due_date<current_date
        and coalesce(s.total_due,0)-coalesce(s.total_paid,0)>0.01
    ) as oldest_unpaid_due
  from bripta_guarded_candidates c
  left join public.loan_schedules s on s.loan_id::text=c.loan_id::text
  group by c.loan_id
)
update public.loans l
set
  arrears_amount=case when l.outstanding_balance<=0.01 then 0 else least(o.arrears,l.outstanding_balance) end,
  overdue_days=case
    when l.outstanding_balance<=0.01 or o.oldest_unpaid_due is null then 0
    else greatest(0,current_date-o.oldest_unpaid_due)
  end
from overdue o
where l.id::text=o.loan_id::text;

commit;

-- Verification: Nancy should show paid 8660 and balance 1340.
select
  c.full_name,
  c.phone,
  l.loan_no,
  l.status,
  l.total_payable,
  l.total_paid,
  l.outstanding_balance,
  l.arrears_amount,
  l.overdue_days,
  round(coalesce(sum(r.amount),0)::numeric,2) as repayment_ledger_total
from public.loans l
join public.loan_clients c on c.id::text=l.client_id::text
left join public.loan_repayments r on r.loan_id::text=l.id::text
where l.id in (select id from public.bripta_guarded_balance_backup_20260802)
group by c.full_name,c.phone,l.loan_no,l.status,l.total_payable,l.total_paid,
         l.outstanding_balance,l.arrears_amount,l.overdue_days
order by c.full_name,l.loan_no;

select 'remaining_guarded_pattern_mismatches' as check_name,count(*) as rows_remaining
from (
  select l.id
  from public.loans l
  join (
    select loan_id,round(sum(coalesce(amount,0))::numeric,2) repayment_total
    from public.loan_repayments where business_id='BIZ-B3F5E5D9' group by loan_id
  ) r on r.loan_id::text=l.id::text
  where l.business_id='BIZ-B3F5E5D9' and l.status='active'
    and r.repayment_total>coalesce(l.total_paid,0)+0.01
) x;

-- Review callback-only transactions without matching them automatically.
select
  q.trans_id,
  q.trans_amount,
  q.bill_ref_number,
  q.created_at,
  coalesce(m.client_matches,0) as client_matches,
  m.possible_clients,
  m.possible_active_loans,
  case
    when coalesce(m.client_matches,0)=1 and nullif(m.possible_active_loans,'') is not null
      then 'One possible client with an active loan - review before matching'
    when coalesce(m.client_matches,0)=0 then 'No client phone match'
    when nullif(m.possible_active_loans,'') is null then 'Client found but no active loan'
    else 'Multiple possibilities - manual review required'
  end as review_status
from public.mpesa_callback_queue q
left join lateral (
  select
    count(distinct c.id) as client_matches,
    string_agg(distinct concat(c.full_name,' (',c.phone,')'),', ') as possible_clients,
    string_agg(distinct l.loan_no,', ') filter (
      where l.status='active' and coalesce(l.outstanding_balance,0)>0
    ) as possible_active_loans
  from public.loan_clients c
  left join public.loans l on l.client_id::text=c.id::text and l.business_id=c.business_id
  where c.business_id='BIZ-B3F5E5D9'
    and length(regexp_replace(coalesce(q.bill_ref_number,''),'\D','','g'))>=9
    and right(regexp_replace(coalesce(c.phone,''),'\D','','g'),9)
      =right(regexp_replace(coalesce(q.bill_ref_number,''),'\D','','g'),9)
) m on true
where q.business_short_code='BIZ-B3F5E5D9'
  and coalesce(q.dismissed,false)=false
  and not exists (
    select 1 from public.loan_repayments r
    where r.business_id='BIZ-B3F5E5D9'
      and (upper(trim(coalesce(r.payment_reference,'')))=upper(trim(coalesce(q.trans_id,'')))
        or upper(trim(coalesce(r.receipt_no,'')))=upper(trim(coalesce(q.trans_id,''))))
  )
order by q.created_at desc;
