drop table if exists dm_operations.conversation_history_open_close;
create table dm_operations.conversation_history_open_close AS
with assg as (
select distinct conversation_id,
		convert_timezone ('Europe/Berlin',created_at::timestamp) as assign_date
from ods_operations.intercom_assignments ia 
)
,reopen as (
select distinct conversation_id,
				convert_timezone ('Europe/Berlin',conversation_part_created_at::timestamp) as reopen_date
from ods_data_sensitive.intercom_conversation_parts icp 
where conversation_part_type in ('open','assign_and_unsnooze', 'unsnoozed','timer_unsnooze','note_and_reopen','assign_and_reopen')
and is_team_cs=true
)
, closed as (
select distinct conversation_id,
				convert_timezone ('Europe/Berlin',conversation_part_created_at::timestamp) as closed_date
from ods_data_sensitive.intercom_conversation_parts icp 
where conversation_part_type in ('close')
and is_team_cs=true)
, conv as (
select distinct id as conversation_id,
created_at::date from ods_data_sensitive.intercom_first_conversation ifc 
)
, dates as (
select  convert_timezone('Europe/Berlin',datum::timestamp) as datum,conversation_id from public.dim_dates dd, conv 
where datum < current_date 
and datum >= created_At
) 
, src_tbl as (
select d.datum::date,
d.conversation_id,
assign_date::date,
reopen_date::date,
closed_date::date,
ifc.market,
ifc.card_contact_reason ,
ifc.b2b_contact_reason,
ifc.time_to_admin_reply,
ifc.time_to_last_close,
ifc.contact_reason,
ifc.resolution_path,
ifc.customer_type,
ifc.contact_item_1,
ifc.contact_item_2,
case when ifc.tags like '%B2B%' or ifc.b2b_contact_reason<>'' then 'B2B' else 'B2C' end as component,
ifc."language"
from dates d
left join closed c on d.datum::date =  c.closed_date::date and d.conversation_id  = c.conversation_id
left join reopen r on d.datum::date =  r.reopen_date::date and d.conversation_id  = r.conversation_id
left join assg a on d.datum::date =  a.assign_date::date and d.conversation_id  = a.conversation_id
left join ods_data_sensitive.intercom_first_conversation ifc on ifc.id = d.conversation_id)
, final_ as (
select * from src_tbl  
where (assign_date is not null or reopen_date is not null or closed_date is not null )
)
select * from final_;

grant select on dm_operations.conversation_history_open_close to cs_redash;
