drop table if exists monitoring.bd1;
create table monitoring.bd1 as 
with sh as (
		select variant_sku,
		sum(CASE when (("date" = CURRENT_DATE-1)  and supplier in ('Media Markt') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_mm_yesterday,
		sum(CASE when (("date" = CURRENT_DATE-2)  and supplier in ('Media Markt') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_mm_2days_before,
		sum(CASE when (("date" = CURRENT_DATE-3)  and supplier in ('Media Markt') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_mm_3days_before,
		sum(CASE when (("date" = CURRENT_DATE-1) and supplier in ('Saturn') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_saturn_yesterday,
		sum(CASE when (("date" = CURRENT_DATE-2) and supplier in ('Saturn') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_saturn_2days_before,
		sum(CASE when (("date" = CURRENT_DATE-3) and supplier in ('Saturn') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_saturn_3days_before
	from master.asset_historical
	group by 1),
	acquired_subscriptions as(
		select variant_sku,
		count(case when store_label = 'Media Markt online' then subscription_id end) as acquired_last_week_MM,
		count(case when store_label = 'Saturn online' then subscription_id end) as acquired_last_week_saturn
	from master.subscription s
	where datediff('d',start_date,current_date) <= 7
	group by 1),
 	new_stock as (     
		select variant_sku, 
		sum(CASE when (supplier in ('Media Markt') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_mm,
		sum(CASE when (supplier in ('Saturn') and asset_status_original = 'IN STOCK' and asset_condition = 'NEW') THEN 1 else 0 end) as in_stock_new_saturn
		from master.asset
		group by 1)
		select v.variant_sku,
		product_name,
		product_sku,
		ean,
		requested_mm,
		requested_saturn,
		pending_allocation_mm,
		pending_allocation_saturn,
		coalesce(in_stock_new_mm,0) as in_stock_new_mm,
		coalesce(in_stock_new_saturn,0) as in_stock_new_saturn ,
		coalesce(acquired_last_week_MM,0) as acquired_last_week_MM,
		coalesce(acquired_last_week_saturn,0) as acquired_last_week_saturn,
		coalesce(in_stock_new_mm_yesterday,0) as in_stock_new_mm_yesterday,
		coalesce(in_stock_new_mm_2days_before,0) as in_stock_new_mm_2days_before,
		coalesce(in_stock_new_mm_3days_before,0) as in_stock_new_mm_3days_before,
		coalesce(in_stock_new_saturn_yesterday,0) as in_stock_new_saturn_yesterday,
		coalesce(in_stock_new_saturn_2days_before,0) as in_stock_new_saturn_2days_before,
		coalesce(in_stock_new_saturn_3days_before,0) as in_stock_new_saturn_3days_before
	from
	master.variant v 
	left join new_stock ns on v.variant_sku = ns.variant_sku
	left join acquired_subscriptions acs on acs.variant_sku = v.variant_sku 
	left join sh sh on sh.variant_sku = v.variant_sku