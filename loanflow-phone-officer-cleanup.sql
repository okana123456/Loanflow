-- Bripta / Loanflow cleanup for existing imported records.
-- Safe to run more than once.

-- 1. Normalize client phones so Paybill auto-detection can match consistently.
-- Examples:
--   254792719505 -> 0792719505
--   792719505    -> 0792719505
--   0792719505   -> 0792719505
with cleaned as (
  select
    id,
    regexp_replace(coalesce(phone, ''), '\D', '', 'g') as digits
  from public.loan_clients
)
update public.loan_clients c
set phone = case
  when length(cleaned.digits) = 12 and left(cleaned.digits, 3) = '254'
    then '0' || substring(cleaned.digits from 4)
  when length(cleaned.digits) = 9 and left(cleaned.digits, 1) in ('7', '1')
    then '0' || cleaned.digits
  else cleaned.digits
end
from cleaned
where c.id = cleaned.id
  and cleaned.digits <> ''
  and c.phone is distinct from case
    when length(cleaned.digits) = 12 and left(cleaned.digits, 3) = '254'
      then '0' || substring(cleaned.digits from 4)
    when length(cleaned.digits) = 9 and left(cleaned.digits, 1) in ('7', '1')
      then '0' || cleaned.digits
    else cleaned.digits
  end;

-- 2. Fill missing loan officers from the original application record.
update public.loans l
set loan_officer_id = a.loan_officer_id
from public.loan_applications a
where l.application_id = a.id
  and l.loan_officer_id is null
  and a.loan_officer_id is not null;

-- 3. Fill missing loan officers from client notes created by the app, where present.
with tagged_clients as (
  select
    id,
    (regexp_match(notes, '\[OFFICER:([0-9a-fA-F-]{36})\]'))[1]::uuid as officer_id
  from public.loan_clients
  where notes ~ '\[OFFICER:[0-9a-fA-F-]{36}\]'
)
update public.loans l
set loan_officer_id = t.officer_id
from tagged_clients t
where l.client_id = t.id
  and l.loan_officer_id is null
  and t.officer_id is not null;

-- 4. Check what still needs manual assignment.
select
  l.id,
  l.loan_no,
  c.full_name,
  c.phone,
  l.status
from public.loans l
left join public.loan_clients c on c.id = l.client_id
where l.loan_officer_id is null
order by c.full_name nulls last, l.loan_no;
