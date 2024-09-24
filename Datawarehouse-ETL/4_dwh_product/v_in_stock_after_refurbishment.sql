CREATE OR REPLACE VIEW dm_commercial.v_in_stock_after_refurbishment AS 
WITH relevant_assets AS (
	SELECT 
		asset_id, 
		MIN("date") AS first_refurbishment
	FROM master.asset_historical 
	WHERE asset_status_original IN ('IN REPAIR','INCOMPLETE','LOCKED DEVICE','SENT FOR REFURBISHMENT','WAITING FOR REFURBISHMENT','WARRANTY')
	GROUP BY 1
)	
, asset_list AS (
	SELECT
		a.asset_id,
		CASE 
			WHEN asset_status_original NOT IN ('RESERVED','IN STOCK') 
				THEN COALESCE(subscription_id, last_active_subscription_id) 
		END AS sub_id,
		product_sku,
		asset_status_original,
		date,
		lag(sub_id) OVER (PARTITION BY a.asset_id ORDER BY date) prev_sub_id
	FROM relevant_assets ra 
	INNER JOIN master.asset_historical a
	  ON ra.asset_id = a.asset_id
	 AND ra.first_refurbishment <= a."date"
	WHERE a.asset_status_original IN ('RESERVED','IN STOCK','IN REPAIR','INCOMPLETE','LOCKED DEVICE','SENT FOR REFURBISHMENT','WAITING FOR REFURBISHMENT','WARRANTY')
)
, assets_in_refurbishment AS (
	SELECT
		asset_id,
		sub_id,
		min("date") AS min_date,
		max("date") AS max_date
	FROM asset_list
	WHERE asset_status_original NOT IN ('IN STOCK', 'RESERVED')
	GROUP BY 1,2
)
, assets_in_stock AS (
	SELECT 
		al.asset_id,
		prev_sub_id,
		MIN("date") min_date
	FROM asset_list al
	INNER JOIN assets_in_refurbishment ar
	  ON al.asset_id = ar.asset_id
	  AND al.date >= ar.max_date
	WHERE asset_status_original IN ('IN STOCK', 'RESERVED')
	  AND prev_sub_id IS NOT NULL 
	GROUP BY 1,2
)
SELECT 
	air.asset_id,
	air.sub_id,
	air.min_date refurbishment_start,
	air.max_date refurbishment_end,
	ais.min_date in_stock_start,
	a.category_name,
	a.subcategory_name,
	a.product_sku 
FROM assets_in_refurbishment air
LEFT JOIN assets_in_stock ais
	ON ais.asset_id = air.asset_id
	 AND ais.prev_sub_id = air.sub_id
LEFT JOIN master.asset a 
	ON a.asset_id = air.asset_id
WITH NO SCHEMA BINDING
;