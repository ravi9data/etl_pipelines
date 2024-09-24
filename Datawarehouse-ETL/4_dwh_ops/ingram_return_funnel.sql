drop table if exists dm_recommerce.ingram_return_funnel;
create table dm_recommerce.ingram_return_funnel as

with ingram_unique_events as(
select distinct order_number 
,serial_number 
,item_number 
,disposition_code
,status_code
,source_timestamp 
from recommerce.ingram_micro_send_order_grading_status imsogs)

select  o.end_status_grouped_order as asset_status_in_ingram,
o."end status - grouped" as asset_status_order_in_ingram,
source_timestamp::date,
count(distinct serial_number)
from ingram_unique_events i
left join staging_airbyte_bi.ingram_return_funnel_order o on i.disposition_code = o.disposition_code and i.status_code = o.status_code  
where o.include_ind = '1'
group by 1,2,3