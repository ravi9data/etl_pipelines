delete from dwh.daily_fact_asset_sku_supplier_status where fact_date = CURRENT_DATE-1;
insert into dwh.daily_fact_asset_sku_supplier_status
select
	date as fact_date, 
	a.warehouse,
	a.product_sku, 
	a.supplier,
	a.category_name,
	a.subcategory_name,
	a.asset_status_original,
	a.asset_status_new,
	a.asset_status_detailed,
	count(1) as number_of_assets,
	sum(a.initial_price) as total_initial_price,
	a.purchased_date 
from master.asset_historical as a
where fact_date=CURRENT_DATE-1
group by 1,2,3,4,5,6,7,8,9,purchased_date
