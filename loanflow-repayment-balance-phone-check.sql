-- Loanflow/Bripta repayment balance and phone-matching check
-- Run the SELECT sections first. Run the UPDATE section only after reviewing the rows.

-- 1) Check the two clients mentioned by the client.
with target_clients as (
  select
    c.id as client_id,
    c.full_name,
    c.phone,
    regexp_replace(coalesce(c.phone, ''), '\D', '', 'g') as phone_digits
  from loan_clients c
  where regexp_replace(coalesce(c.phone, ''), '\D', '', 'g') in (
    '0117433966',
    '117433966',
    '254117433966',
    '0746013713',
    '746013713',
    '254746013713'
  )
     or c.full_name ilike '%Lidia Odoyo%'
     or c.full_name ilike '%Dibora Odhiambo%'
),
loan_totals as (
  select
    l.id as loan_id,
    l.client_id,
    l.loan_no,
    l.status,
    l.total_payable,
    l.total_paid as stored_total_paid,
    l.outstanding_balance as stored_outstanding_balance,
    coalesce(sum(r.amount), 0) as repayment_total,
    greatest(0, coalesce(l.total_payable, 0) - coalesce(sum(r.amount), 0)) as expected_outstanding_balance
  from loans l
  left join loan_repayments r on r.loan_id = l.id
  where l.client_id in (select client_id from target_clients)
  group by l.id
)
select
  tc.full_name,
  tc.phone,
  lt.loan_no,
  lt.status,
  lt.total_payable,
  lt.stored_total_paid,
  lt.repayment_total,
  lt.stored_outstanding_balance,
  lt.expected_outstanding_balance,
  lt.stored_outstanding_balance - lt.expected_outstanding_balance as balance_difference
from loan_totals lt
join target_clients tc on tc.client_id = lt.client_id
order by tc.full_name, lt.loan_no;

-- 2) Show phone formats that may stop automatic matching for imported clients.
select
  full_name,
  phone,
  regexp_replace(coalesce(phone, ''), '\D', '', 'g') as phone_digits,
  right(regexp_replace(coalesce(phone, ''), '\D', '', 'g'), 9) as last_9_digits
from loan_clients
where full_name ilike '%Lidia Odoyo%'
   or full_name ilike '%Dibora Odhiambo%'
   or right(regexp_replace(coalesce(phone, ''), '\D', '', 'g'), 9) in ('117433966', '746013713')
order by full_name;

-- 3) Optional repair: recalculate stored loan balances from actual repayments.
-- This is safe for loans where total_payable is already correct.
-- Remove the comment marks below only after section 1 shows wrong stored balances.
/*
with recalculated as (
  select
    l.id,
    coalesce(sum(r.amount), 0) as repayment_total,
    greatest(0, coalesce(l.total_payable, 0) - coalesce(sum(r.amount), 0)) as expected_balance
  from loans l
  left join loan_repayments r on r.loan_id = l.id
  where l.client_id in (
    select c.id
    from loan_clients c
    where c.full_name ilike '%Lidia Odoyo%'
       or c.full_name ilike '%Dibora Odhiambo%'
       or right(regexp_replace(coalesce(c.phone, ''), '\D', '', 'g'), 9) in ('117433966', '746013713')
  )
  group by l.id
)
update loans l
set
  total_paid = round(r.repayment_total::numeric, 2),
  outstanding_balance = round(r.expected_balance::numeric, 2),
  status = case when r.expected_balance <= 0.01 then 'completed' else l.status end,
  arrears_amount = case when r.expected_balance <= 0.01 then 0 else l.arrears_amount end,
  overdue_days = case when r.expected_balance <= 0.01 then 0 else l.overdue_days end
from recalculated r
where l.id = r.id
  and (
    abs(coalesce(l.total_paid, 0) - r.repayment_total) > 0.01
    or abs(coalesce(l.outstanding_balance, 0) - r.expected_balance) > 0.01
    or (r.expected_balance <= 0.01 and l.status <> 'completed')
  );
*/
