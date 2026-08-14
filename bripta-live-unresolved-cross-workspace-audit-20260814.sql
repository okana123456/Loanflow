-- Bripta live-business unresolved payment audit - READ ONLY
-- The old callback sometimes stored suspense rows under the wrong workspace.
-- This audit checks their plain account reference against Bripta's live clients.
-- It does not post payments, change balances or resolve suspense records.

with params as (
  select 'BIZ-B3F5E5D9'::text as bripta_business_id
), unresolved as (
  select
    u.id,u.business_id as stored_business_id,u.mpesa_reference,u.amount,
    u.payer_phone,u.account_number,u.created_at,
    right(regexp_replace(coalesce(u.account_number,''),'\D','','g'),9) as account_tail
  from public.unmatched_payments u
  where coalesce(u.resolved,false)=false
), candidates as (
  select
    u.id as payment_id,c.id as client_id,c.full_name,c.phone,
    count(*) over(partition by u.id) as client_matches
  from unresolved u
  cross join params p
  join public.loan_clients c
    on c.business_id=p.bripta_business_id
   and length(u.account_tail)=9
   and right(regexp_replace(coalesce(c.phone,''),'\D','','g'),9)=u.account_tail
), unique_client as (
  select distinct on (u.id)
    u.*,c.client_id,c.full_name,c.phone as client_phone,c.client_matches
  from unresolved u
  join candidates c on c.payment_id=u.id and c.client_matches=1
  order by u.id,c.client_id
), classified as (
  select
    u.*,
    (select r.id from public.loan_repayments r cross join params p
      where r.business_id=p.bripta_business_id
        and (r.payment_reference=u.mpesa_reference or r.receipt_no=u.mpesa_reference)
      limit 1) as existing_repayment_id,
    l.id as likely_loan_id,l.loan_no as likely_loan_no,l.status as current_loan_status,
    l.disbursement_date,l.maturity_date,l.outstanding_balance,
    (select count(*) from public.bripta_charges ch cross join params p
      where ch.business_id=p.bripta_business_id and ch.client_id=u.client_id
        and ch.charge_type='processing_fee'
        and ch.status in ('pending','partially_paid')) as pending_processing_fees
  from unique_client u
  left join lateral (
    select l.*
    from public.loans l cross join params p
    where l.business_id=p.bripta_business_id
      and l.client_id=u.client_id
      and coalesce(l.disbursement_date,l.created_at::date)<=u.created_at::date
    order by coalesce(l.disbursement_date,l.created_at::date) desc,l.created_at desc
    limit 1
  ) l on true
), ambiguous as (
  select
    u.id,u.mpesa_reference,u.amount,u.account_number,u.created_at,
    max(c.client_matches) as client_matches,
    string_agg(distinct c.full_name||' ('||coalesce(c.phone,'')||')',', ') as possible_clients
  from unresolved u
  join candidates c on c.payment_id=u.id and c.client_matches>1
  group by u.id,u.mpesa_reference,u.amount,u.account_number,u.created_at
), sections as (
  select 1 as section_order,'bripta_live_cross_workspace_summary'::text as section,
    jsonb_build_object(
      'bripta_business_id',(select bripta_business_id from params),
      'unresolved_rows_reviewed',(select count(*) from unresolved),
      'unique_bripta_client_matches',(select count(*) from classified),
      'already_present_in_repayments',(select count(*) from classified where existing_repayment_id is not null),
      'unique_matches_with_likely_historical_loan',(select count(*) from classified where existing_repayment_id is null and likely_loan_id is not null),
      'post_clearance_fee_candidates',(select count(*) from classified where existing_repayment_id is null and likely_loan_id is not null and current_loan_status='completed' and pending_processing_fees>0),
      'ambiguous_bripta_client_matches',(select count(*) from ambiguous),
      'no_bripta_client_match',(select count(*) from unresolved u where not exists(select 1 from candidates c where c.payment_id=u.id)),
      'financial_data_changed',false
    ) as result
  union all
  select 2,'already_recorded_mark_suspense_only',coalesce(jsonb_agg(jsonb_build_object(
    'reference',mpesa_reference,'amount',amount,'received_at',created_at,
    'client',full_name,'client_phone',client_phone,'repayment_id',existing_repayment_id
  ) order by created_at desc),'[]'::jsonb)
  from classified where existing_repayment_id is not null
  union all
  select 3,'historical_matches_needing_loan_review',coalesce(jsonb_agg(jsonb_build_object(
    'reference',mpesa_reference,'amount',amount,'received_at',created_at,
    'stored_business_id',stored_business_id,'client',full_name,'client_phone',client_phone,
    'likely_loan_id',likely_loan_id,'likely_loan_no',likely_loan_no,
    'disbursement_date',disbursement_date,'maturity_date',maturity_date,
    'current_loan_status',current_loan_status,'current_balance',outstanding_balance,
    'pending_processing_fees',pending_processing_fees,
    'warning','Review against the loan statement before posting; the client may have taken later loans'
  ) order by created_at desc),'[]'::jsonb)
  from classified where existing_repayment_id is null and likely_loan_id is not null
  union all
  select 4,'client_found_but_no_earlier_loan',coalesce(jsonb_agg(jsonb_build_object(
    'reference',mpesa_reference,'amount',amount,'received_at',created_at,
    'client',full_name,'client_phone',client_phone,'account_number',account_number
  ) order by created_at desc),'[]'::jsonb)
  from classified where existing_repayment_id is null and likely_loan_id is null
  union all
  select 5,'ambiguous_do_not_post',coalesce(jsonb_agg(to_jsonb(ambiguous) order by created_at desc),'[]'::jsonb)
  from ambiguous
)
select * from sections order by section_order;
