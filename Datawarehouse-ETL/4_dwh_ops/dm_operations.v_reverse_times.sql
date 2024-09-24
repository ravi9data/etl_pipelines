CREATE OR REPLACE VIEW dm_operations.v_reverse_times AS
WITH reverse_delivery_time AS (
	SELECT 
		ash.customer_type,
		ash.shipping_country,
		CASE
			WHEN ash.shipment_service = 'Hermes 2MH Standard'
				THEN '2 Man Handling (Bulky)'
			ELSE 'Small Parcel'
			END AS shipment_service_grouping,
		ash.shipment_service,
		a.allocation_id,
		a.return_shipment_at::date AS customer_drop_off_at, -- WHEN customer DROP-OFF the asset TO RETURN it
		a.return_delivery_date::date AS asset_arrive_in_warehouse_at,
		greatest (datediff('hour', a.return_shipment_at, a.return_delivery_date ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= a.return_shipment_at and d.datum < a.return_delivery_date
					  and d.week_day_number in (6, 7)) * 24
			, 0)::float / 24 as reverse_delivery_time
	FROM ods_production.allocation a
	LEFT JOIN ods_operations.allocation_shipment ash 
		ON a.allocation_id = ash.allocation_id
	WHERE a.return_shipment_at IS NOT NULL 
		AND a.return_shipment_at <> a.return_delivery_date --DELETE the cases WHERE we ONLY have 1 date, so BOTH would be the same.
		AND a.return_delivery_date::date > dateadd('month',-2,date_trunc('month', current_date))
		AND ash.customer_type IS NOT NULL
		AND ash.store <> 'Partners Offline'
)
, assets_back_in_stock_prep as (
	SELECT DISTINCT 
		ash.customer_type,
		ash.shipping_country,
		CASE
			WHEN ash.shipment_service = 'Hermes 2MH Standard'
				THEN '2 Man Handling (Bulky)'
			ELSE 'Small Parcel'
			END AS shipment_service_grouping,
		ash.shipment_service,
		a.allocation_id,
		a.return_shipment_at::date AS customer_drop_off_at,
		CASE WHEN ah.asset_id IS NOT NULL THEN 1 ELSE 0 END is_asset_in_repair,		
        		- (select count(1) from public.dim_dates d
					  and d.week_day_number in (6, 7)) * 24
			, 0)::float / 24 as reverse_net_ops_cycle,			
		row_number() OVER (PARTITION BY i.order_number ORDER BY a.return_shipment_at DESC) AS rn,
	FROM ods_production.allocation a
	LEFT JOIN ods_operations.ingram_micro_orders i
		ON a.serial_number = i.serial_number
	LEFT JOIN master.asset_historical ah 
		ON a.asset_id = ah.asset_id
		AND ah.asset_status_original IN ('LOCKED DEVICE','IN REPAIR') --checking assets that were/ARE IN repair
	LEFT JOIN ods_operations.allocation_shipment ash 
		ON a.allocation_id = ash.allocation_id
	WHERE a.return_shipment_at IS NOT NULL 	
			-- since our cohort IS WHEN the customer DROP the asset AND IF the asset will be IN repair
			--it will be for many months, so, for the reverse, it makes sense to have it longer, otherwise, we will exclude many cases,
			-- since we only show the cases where assets are already available to be rented again
		AND ash.customer_type IS NOT NULL
		AND ash.store <> 'Partners Offline'
)
, reverse_net_ops_cycle AS (
	SELECT 
		customer_drop_off_at,
		customer_type,
		shipping_country,
		shipment_service_grouping,
		shipment_service,
		allocation_id,
		reverse_net_ops_cycle,
		is_asset_in_repair,
		asset_available_to_be_rented_again_at
	FROM assets_back_in_stock_prep
	WHERE rn = 1 AND rn_2 = 1
)
SELECT 
	'Reverse Delivery Process' AS kpi,
	dt.customer_drop_off_at,
	dt.customer_type,
	dt.shipping_country,
	dt.shipment_service_grouping,
	dt.shipment_service,
	dt.allocation_id,
	dt.asset_arrive_in_warehouse_at AS end_reverse_process_at,
	dt.reverse_delivery_time,
	NULL::float AS reverse_net_ops_cycle,
	NULL::int AS is_asset_in_repair
FROM reverse_delivery_time dt 
UNION 
SELECT 
	'Reverse Net Ops Cycle' AS kpi,
	customer_drop_off_at,
	customer_type,
	shipping_country,
	shipment_service_grouping,
	shipment_service,
	allocation_id,
	asset_available_to_be_rented_again_at AS end_reverse_process_at,
	NULL::float AS reverse_delivery_time,
	reverse_net_ops_cycle,
	is_asset_in_repair
FROM reverse_net_ops_cycle
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_operations.v_reverse_times TO tableau;

