-- Read-only audit for former loan officers Brian Ochieng and Franklin Aran.
-- Brian Obanda Ochieng (admin) is excluded from the staff history search.
-- One result table combines staff events, recorded transfers and current
-- portfolio counts. It does not restore or move any records.

with officer_ids(staff_id,label) as (
  values
    ('d1479025-fad1-4178-a147-a0faf97764ac'::uuid,'Brian officer ID now Juma Gad'),
    ('910139c3-36b8-49bf-915b-764d7095a3ec'::uuid,'Former Franklin Aran ID'),
    ('36301a4f-0252-4de9-87fb-a0b5fb75b6c4'::uuid,'Other deleted officer ID'),
    ('74b5e8ca-fabf-4890-b17b-5f6fc80e0d99'::uuid,'George Majwa')
), staff_events as (
  select a.created_at::text event_time, 'staff_history'::text source,
         a.action::text action, a.record_id::text staff_id,
         coalesce(a.new_value->>'name',a.old_value->>'name')::text name,
         null::text target_id, null::bigint clients_now, null::bigint loans_now
  from public.loan_audit_log a
  where a.table_name='loan_staff'
    and a.action like 'staff_%'
    and (
      a.record_id in (select staff_id from officer_ids)
      or lower(coalesce(a.new_value->>'name','')) in ('brian ochieng','franklin aran','juma gad')
    )
), transfer_events as (
  select a.created_at::text, 'recorded_transfer'::text, a.action::text,
         a.new_value->>'from', null::text, a.new_value->>'to',
         null::bigint, null::bigint
  from public.bripta_domain_audit a
  where a.business_id='BIZ-B3F5E5D9' and a.action='portfolio_transferred'
    and (a.new_value->>'from' in (select staff_id::text from officer_ids)
      or a.new_value->>'to' in (select staff_id::text from officer_ids))
), current_portfolios as (
  select null::text, 'current_portfolio'::text, 'current_assignment'::text,
         o.staff_id::text, coalesce(s.name,o.label), null::text,
         (select count(*) from public.loan_clients c where c.business_id='BIZ-B3F5E5D9' and c.loan_officer_id=o.staff_id),
         (select count(*) from public.loans l where l.business_id='BIZ-B3F5E5D9' and l.loan_officer_id=o.staff_id)
  from officer_ids o left join public.loan_staff s on s.id=o.staff_id
)
select * from staff_events
union all select * from transfer_events
union all select * from current_portfolios
order by source,event_time desc nulls last;
