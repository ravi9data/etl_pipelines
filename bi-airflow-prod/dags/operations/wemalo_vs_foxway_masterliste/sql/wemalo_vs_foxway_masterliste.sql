drop table if exists dm_recommerce.wemalo_vs_foxway_masterliste;
create table dm_recommerce.wemalo_vs_foxway_masterliste as
with refurbishment_data_latest_record as(
select sub_q.*, 
datediff('day', sub_q.sent_to_recommerce, sub_q.last_report_date) as elapsed_time  
from(
select widm.reporting_date as last_report_date,
widm.asset_id,
widm.asset_name,
widm.product_sku,
widm.brand,
widm.category_name,
widm.subcategory_name,
widm.seriennummer,
widm.stellplatz,
widm.spl_position,
min(reporting_date) over( partition by seriennummer) as sent_to_recommerce,
row_number() over(partition by seriennummer order by reporting_date desc) as rn
from dm_recommerce.wemalo_inventory_daily_movement widm where UPPER(stellplatz) like '%SELL-NR%') as sub_q
where rn=1),

latest_record_in_masterliste as(
select serial_number from(
select coalesce(trim(grover_id),trim(external_id)) as serial_number,
row_number() over(partition by serial_number order by reporting_date desc) as rn
from recommerce.foxway_masterliste) where rn=1
)
select * from refurbishment_data_latest_record where seriennummer in(
select * from(
select seriennummer as serial_number from refurbishment_data_latest_record
minus
select serial_number from latest_record_in_masterliste));

grant select on dm_recommerce.wemalo_vs_foxway_masterliste to tableau;
grant select on table dm_recommerce.wemalo_vs_foxway_masterliste to GROUP bi;

