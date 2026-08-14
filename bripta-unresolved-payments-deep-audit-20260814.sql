-- Bripta unresolved payment deep audit - read only
-- Checks both the account reference and the payer phone used by Daraja.
-- No repayment, loan balance, charge or suspense record is changed.

with unresolved as (
  select
    u.id,
    u.business_id,
    u.mpesa_reference,
    u.amount,
    u.payer_phone,
    u.account_number,
    u.created_at,
    right(regexp_replace(coalesce(u.account_number,''),'\D','','g'),9) as account_tail,
    right(regexp_replace(coalesce(u.payer_phone,''),'\D','','g'),9) as payer_tail
  from public.unmatched_payments u
  where coalesce(u.resolved,false)=false
), client_candidates as (
  select
    u.id as payment_id,
    c.id as client_id,
    c.full_name,
    c.phone,
    case
      when length(u.account_tail)=9
       and right(regexp_replace(coalesce(c.phone,''),'\D','','g'),9)=u.account_tail
        then 'account reference'
      when length(u.payer_tail)=9
       and right(regexp_replace(coalesce(c.phone,''),'\D','','g'),9)=u.payer_tail
        then 'payer phone'
      else 'unknown'
    end as matched_by
  from unresolved u
  join public.loan_clients c
    on c.business_id=u.business_id
   and (
     (length(u.account_tail)=9 and right(regexp_replace(coalesce(c.phone,''),'\D','','g'),9)=u.account_tail)
     or
     (length(u.payer_tail)=9 and right(regexp_replace(coalesce(c.phone,''),'\D','','g'),9)=u.payer_tail)
   )
), candidate_counts as (
  select payment_id,count(distinct client_id) as client_matches
  from client_candidates
  group by payment_id
), unique_matches as (
  select distinct on (u.id)
    u.*,
    cc.client_matches,
    c.client_id,
    c.full_name,
    c.phone as client_phone,
    c.matched_by
  from unresolved u
  join candidate_counts cc on cc.payment_id=u.id and cc.client_matches=1
  join client_candidates c on c.payment_id=u.id
  order by u.id,c.client_id
), classified as (
  select
    m.*,
    (select count(*) from public.loans l
      where l.business_id=m.business_id and l.client_id=m.client_id
        and l.status='active' and coalesce(l.outstanding_balance,0)>0.01) as active_loans,
    (select count(*) from public.bripta_charges ch
      where ch.business_id=m.business_id and ch.client_id=m.client_id
        and ch.charge_type='processing_fee'
        and ch.status in ('pending','partially_paid')) as pending_processing_fees,
    (select string_agg(l.loan_no,', ' order by l.created_at desc) from public.loans l
      where l.business_id=m.business_id and l.client_id=m.client_id
        and l.status='active' and coalesce(l.outstanding_balance,0)>0.01) as active_loan_numbers
  from unique_matches m
), ambiguous as (
  select
    u.id,u.mpesa_reference,u.amount,u.payer_phone,u.account_number,u.created_at,
    cc.client_matches,
    string_agg(distinct c.full_name||' ('||coalesce(c.phone,'')||')',', ') as possible_clients
  from unresolved u
  join candidate_counts cc on cc.payment_id=u.id and cc.client_matches>1
  join client_candidates c on c.payment_id=u.id
  group by u.id,u.mpesa_reference,u.amount,u.payer_phone,u.account_number,u.created_at,cc.client_matches
), sections as (
  select 1 as section_order,'corrected_unresolved_summary'::text as section,
    jsonb_build_object(
      'unresolved_rows',(select count(*) from unresolved),
      'unique_client_matches',(select count(*) from classified),
      'unique_active_loan_matches',(select count(*) from classified where active_loans=1),
      'unique_post_clearance_fee_matches',(select count(*) from classified where active_loans=0 and pending_processing_fees>0),
      'ambiguous_client_matches',(select count(*) from ambiguous),
      'no_client_match',(select count(*) from unresolved u where not exists(select 1 from candidate_counts cc where cc.payment_id=u.id)),
      'financial_data_changed',false
    ) as result
  union all
  select 2,'unique_matches_ready_for_review',coalesce(jsonb_agg(jsonb_build_object(
    'reference',mpesa_reference,'amount',amount,'received_at',created_at,
    'payer_phone',payer_phone,'account_number',account_number,
    'client',full_name,'client_phone',client_phone,'matched_by',matched_by,
    'active_loans',active_loans,'active_loan_numbers',active_loan_numbers,
    'pending_processing_fees',pending_processing_fees,
    'recommended_action',case
      when active_loans=1 then 'Review for matching to the active loan'
      when active_loans=0 and pending_processing_fees>0 then 'Review as a post-clearance processing-fee payment'
      when active_loans=0 then 'Client found but no active loan or pending processing fee'
      else 'Multiple active loans require manual review'
    end
  ) order by created_at desc),'[]'::jsonb) from classified
  union all
  select 3,'ambiguous_matches_do_not_auto_post',coalesce(jsonb_agg(jsonb_build_object(
    'reference',mpesa_reference,'amount',amount,'received_at',created_at,
    'payer_phone',payer_phone,'account_number',account_number,
    'possible_clients',possible_clients,'client_matches',client_matches
  ) order by created_at desc),'[]'::jsonb) from ambiguous
  union all
  select 4,'recent_no_client_match_sample',coalesce(jsonb_agg(x.row_data order by x.created_at desc),'[]'::jsonb)
  from (
    select u.created_at,to_jsonb(u)-'id'-'business_id'-'account_tail'-'payer_tail' as row_data
    from unresolved u
    where not exists(select 1 from candidate_counts cc where cc.payment_id=u.id)
    order by u.created_at desc
    limit 100
  ) x
)
select * from sections order by section_order;
