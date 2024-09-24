CREATE OR REPLACE VIEW dm_commercial.v_refurbishment_back_to_stock AS 
WITH allocation_start_end AS (
-- GET ALL asset allocations (asset - subscription combination) WHERE subs started IN LAST 6 months (in order to have less history to load)
	SELECT 
		a.subscription_id,
		a.created_at::date AS allocation_start_date,
		COALESCE(dateadd('day',-1,LAG(a.created_at) OVER (PARTITION BY a.asset_id ORDER BY a.created_at DESC)::date),'2099-12-31')::date AS allocation_end_date,
		a.asset_id,
		a.refurbishment_start_at,
		a.refurbishment_end_at,
		s.start_date
	FROM ods_production.allocation a
	LEFT JOIN master.subscription s
		ON s.subscription_id = a.subscription_id	
	WHERE s.start_date > dateadd('month',-8,current_date)
)
, asset_history AS (
-- Filtering historical asset data by previous CTE. Limiting to Assets with (a) start dates in last 6 months and (b) asset data after those start dates.
	SELECT 
		ah.asset_id,
		ah.asset_status_original,
		date
	FROM master.asset_historical ah
	INNER JOIN allocation_start_end e
		ON e.asset_id = ah.asset_id 
		AND ah.date > e.start_date
)
, number_of_subscriptions_per_asset AS (
-- Left joining the asset history onto the allocation table. This creates duplicates on purpose so that we have
-- (a) repeated allocation information for each row and (b) a row for each fact date (and daily status info) within the allocation period.
	SELECT 
		e.asset_id,
		e.subscription_id,
		e.refurbishment_start_at,
		e.refurbishment_end_at,
		h.date AS fact_day,
		h.asset_status_original,
		DENSE_RANK() OVER (PARTITION BY e.asset_id ORDER BY e.allocation_start_date) AS num_subs_for_asset, -- To tell IF the Sub IS FIRST, SECOND, etc FOR asset WITHIN LAST 6 months.
		ROW_NUMBER() OVER (PARTITION BY e.subscription_id, e.asset_id ORDER BY fact_day DESC) AS last_status -- Use this later TO GET LAST asset status.
	FROM allocation_start_end e
	LEFT JOIN asset_history h
		ON e.asset_id = h.asset_id
		AND h.date BETWEEN e.allocation_start_date AND allocation_end_date
)
, max_number_of_subs AS (
	SELECT *,
		max(num_subs_for_asset) OVER (PARTITION BY asset_id) AS max_subs_per_asset  -- Adding max sub number per Asset_id WITHIN 6 MONTH PERIOD. Redshift required separation OF previous CTE. 
	FROM number_of_subscriptions_per_asset
)
, max_number_of_subs AS
(
SELECT *,
	max(num_subs_for_asset) OVER (PARTITION BY asset_id) AS max_sub
FROM number_of_subscriptions_per_asset
)
, refurbishment_time AS (
-- For each Allocation (asset-sub combination), return the start and end of refurbishment. Intended to reconcile other start/end dates for assets with refurbishment labels. 
-- Note: Logic based only on the refurbishment labels but incomplete, will use the start/end coming from allocation table 
	SELECT 
		asset_id,
		subscription_id,
		min(fact_day) AS refurbishment_start,
		max(fact_day) AS refurbishment_end
	FROM number_of_subscriptions_per_asset
	WHERE asset_status_original IN ('IN REPAIR','INCOMPLETE','LOCKED DEVICE','SENT FOR REFURBISHMENT','WAITING FOR REFURBISHMENT','WARRANTY')
	GROUP BY 1,2
)
SELECT DISTINCT
	f.asset_id,
	f.subscription_id,
	f.num_subs_for_asset,
	f.max_subs_per_asset,
	CASE 
		WHEN f.num_subs_for_asset < max_subs_per_asset
			THEN 'IN STOCK'
		WHEN f.refurbishment_end_at IS NULL AND f.asset_status_original <> 'IRREPARABLE' AND f.refurbishment_start_at IS NOT NULL 
			THEN 'STILL IN REFURBISHMENT' 
		WHEN f.asset_status_original IN ('WRITTEN OFF DC','WRITTEN OF DC','WRITTEN OFF OPS','RETURNED TO SUPPLIER', 'DELETED','REPORTED AS STOLEN','IRREPARABLE')
			THEN 'DECOMMISSIONED'
		WHEN f.asset_status_original IN ('SOLD','SELLING')
			THEN 'SOLD'
		ELSE f.asset_status_original	
	END AS final_status_per_subscription,
	t.refurbishment_start AS refurbishment_start_calc,
	t.refurbishment_end AS refurbishment_end_calc,
	a.asset_status_original AS asset_current_status,
	a.category_name,
	a.subcategory_name,
	a.brand,
	a.product_sku,
	s.cancellation_date::date,
	f.refurbishment_start_at::date,
	f.refurbishment_end_at::date
FROM max_number_of_subs f
LEFT JOIN refurbishment_time t 
	ON t.subscription_id = f.subscription_id 
	AND t.asset_id = f.asset_id
LEFT JOIN master.asset a 
	ON a.asset_id = f.asset_id
LEFT JOIN master.subscription s 
	ON s.subscription_id = f.subscription_id
WHERE last_status = 1 -- We take our fact-date based field AND RETURN it TO a allocation UNIQUE TABLE (each row a unique asset-subs id combination).
WITH NO SCHEMA BINDING
;