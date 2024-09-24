drop table if exists dm_recommerce.return_stages_financial;
create table dm_recommerce.return_stages_financial as
with cost as(
select  
distinct ri.outbound_date as fact_date, 
rs.serial_number, 
ri.repair_price as value,
rs._3pl,
'Cost' as KPI
from dm_recommerce.return_stages rs 
inner join dm_recommerce.repair_invoices ri on rs.serial_number = ri.serial_number
where has_scanned_position),

saved_value_in_stock as(
with in_stock_events as(
select rs.event_timestamp,
rs.serial_number,
rs.asset_id,
rs._3pl
from dm_recommerce.return_stages rs
where rs.ref_status in('stock','STOCK--> AVAILABLE, GROVER')
and rs.has_scanned_position
)

select ie.event_timestamp as fact_date,
ie.serial_number,
r.residual_value as value,
ie._3pl,
'Saved Value In Stock' as KPI
from in_stock_events ie 
inner join dm_finance.depreciation_monthend_report r on ie.asset_id = r.asset_id 
--if an asset put it back to stock in the current month, use the previous month's residual_value
and (case when date_trunc('month',ie.event_timestamp)=date_trunc('month', current_date)
then date_add('month',-1,last_day(ie.event_timestamp::date)) else last_day(ie.event_timestamp::date) end)::date=r.report_date::date

),

value_in_process as(
select reporting_date::date as fact_date,
rs.serial_number,
r.residual_value as value,
_3pl,
'Value in Process' as KPI
from dm_recommerce.return_stages_final rs 
inner join dm_finance.depreciation_monthend_report r on rs.asset_id = r.asset_id 
--if an asset is still in process in the current month, use the previous month's residual_value
and (case when date_trunc('month',rs.reporting_date)=date_trunc('month', current_date)
then date_add('month',-1,last_day(rs.reporting_date::date)) else last_day(rs.reporting_date::date) end)::date=r.report_date::date
where return_stages='In Process'
and rn_month=1
and has_scanned_position
),

revenue as(
with incoming_events as(
select
serial_number,
event_timestamp,
asset_id,
_3pl
from(
select 
rs.serial_number,
event_timestamp,
a.asset_id,
rs._3pl,
--rn is created to get first incoming event's date for each asset, therefore it can be seen how much revenue earned after refurbishment
row_number() over(partition by rs.serial_number order by event_timestamp asc) rn
from dm_recommerce.return_stages rs
left join master.asset a on rs.serial_number = a.serial_number 
where return_stages ='Incoming') where rn =1
),

subs as(
select asset_id,
amount_paid - amount_tax as value, 
billing_period_start,
created_at,
subscription_id
from ods_production.payment_subscription
)

select billing_period_start::date as fact_date,
serial_number,
value,
_3pl,
'Revenue' as KPI
from incoming_events ie 
left join subs s on ie.asset_id = s.asset_id and ie.event_timestamp::date<=s.billing_period_start::date
where billing_period_start<=current_date 
),

value_sent_to_recommerce as(
select  event_timestamp::date as fact_date,
rs.serial_number,
r.residual_value as value,
_3pl,
'Asset Value-Sent to Recommerce' as KPI
from dm_recommerce.return_stages rs 
left join dm_finance.depreciation_monthend_report r on rs.asset_id = r.asset_id 
--if an asset was sent to recommerce in the current month, use the previous month's residual_value
and (case when date_trunc('month',rs.event_timestamp)=date_trunc('month', current_date)
then date_add('month',-1,last_day(rs.event_timestamp::date)) else last_day(rs.event_timestamp::date) end)::date=r.report_date::date
where rs.ref_status in('RECOMMERCE--> AVAILABLE, RECOMMERCE','b2b bulky','irrepearable (optical)','irrepearable (technical)','sell')
)

select * from cost
union all
select * from saved_value_in_stock
union all
select * from value_in_process
union all
select * from revenue
union all
select * from value_sent_to_recommerce;

GRANT SELECT ON dm_recommerce.return_stages_financial TO tableau;
