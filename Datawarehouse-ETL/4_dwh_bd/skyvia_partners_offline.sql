drop table if exists skyvia.partners_offline_order_subscription;
create table skyvia.partners_offline_order_subscription as
select
	*
from dwh.date_store_category_plan_kpis o
where store_type='offline';


drop table if exists skyvia.partners_offline_assetinvestment;
create table skyvia.partners_offline_assetinvestment as
select
	created_at::date,
	purchased_date::date,
	first_allocation_store ,
	first_allocation_store_name,
	category_name,
	subcategory_name,
	sum(initial_price ) as asset_investment
from master.asset
where first_allocation_store ilike '%offline%'
group by 1,2,3,4,5,6;


GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  skyvia;
