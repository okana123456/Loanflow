-- Read-only: identify active Bripta loans still credited to George Majwa.
-- Run in the Bripta Supabase project only.
SELECT
  b.name AS branch,
  s.name AS saved_officer,
  l.loan_no,
  c.full_name AS client,
  l.status,
  l.outstanding_balance,
  l.id AS loan_id,
  l.client_id,
  l.loan_officer_id
FROM public.loans AS l
JOIN public.loan_staff AS s
  ON s.id = l.loan_officer_id
 AND s.business_id = l.business_id
LEFT JOIN public.loan_clients AS c
  ON c.id = l.client_id
 AND c.business_id = l.business_id
LEFT JOIN public.bripta_branches AS b
  ON b.id = l.branch_id
 AND b.business_id = l.business_id
WHERE l.business_id = 'BIZ-B3F5E5D9'
  AND lower(regexp_replace(s.name, '\s+', ' ', 'g')) = 'george majwa'
  AND lower(l.status) = 'active'
ORDER BY b.name, c.full_name, l.loan_no;
