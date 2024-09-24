drop table if exists dm_sustainability.asset_repair_refurbish
create table dm_sustainability.asset_repair_refurbish as
select 
	date as fact_date ,
	category_name ,
	date_part('year', created_at) purchase_year, 
	count ( case when asset_status_original in ('WARRANTY', 'IN REPAIR') then asset_id end ) as total_assets_in_repair,
	count ( case when asset_status_original in ('INCOMPLETE' ,'RETURNED','LOCKED DEVICE','IN TRANSIT','IRREPARABLE' ,'SEND FOR REFURBISHMENT','SENT FOR REFURBISHMENT','WAITING FOR REFURBISHMENT') then asset_id end ) as total_assets_in_refurbishment,
	count( case when asset_status_original not in ('WRITTEN OFF DC','WRITTEN OFF OPS','SOLD','REPORTED AS STOLEN','LOST') then  asset_id end) as total_assets
from master.asset_historical 
where date > current_date - 365
	and date_part(dayofweek, date) = 1 --take monday
group by 1,2,3;