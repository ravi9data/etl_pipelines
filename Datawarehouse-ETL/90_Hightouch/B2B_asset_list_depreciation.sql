CREATE OR REPLACE VIEW hightouch_sources.b2b_list_of_asset_depreciation AS 
WITH statuses AS (
	SELECT 
		asset_id,
		country,
		asset_status_new status,
		min(date) min_date,
		max(date) max_date
	FROM master.asset_historical a
	GROUP BY 1,2,3
)
, in_stock AS (
	SELECT DISTINCT
		asset_id,
		country
	FROM statuses
	WHERE status = 'IN STOCK' 
	  AND max_date IN (SELECT MAX(date) FROM master.asset_historical)
)
, on_rent_once AS (
	SELECT 
		asset_id,
		country,
		max(max_date) AS max_date_perf
	FROM statuses
	WHERE status = 'PERFORMING'
	GROUP BY 1,2
)
, sub_info AS (
	SELECT DISTINCT
		ah.date,
		ah.asset_id,
		ah.country,
		COALESCE(ah.subscription_id,ah.last_active_subscription_id) AS subscription_id
	FROM master.asset_historical ah 
	INNER JOIN on_rent_once oro
	  ON ah.asset_id = oro.asset_id
	AND ah.date = oro.max_date_perf
	WHERE EXISTS (SELECT NULL FROM in_stock stock WHERE stock.asset_id = ah.asset_id AND stock.country = ah.country)
)
, latest_subscription AS (
SELECT 
	subscription_id,
	asset_id,
	ROW_NUMBER () OVER (PARTITION BY asset_id ORDER BY allocated_at DESC) AS rowno 
FROM master.allocation
)
SELECT DISTINCT
	s.asset_id,
	s.country,
 	s2.product_sku,
 	s2.variant_sku,
	CASE
		WHEN s2.store_commercial LIKE ('%B2B%')
		THEN 'B2B'
		WHEN s2.store_commercial LIKE ('%Partnerships%') 
  		THEN 'Retail'
		ELSE 'B2C'
	END AS channel_type
FROM statuses s
INNER JOIN in_stock stock
  ON s.asset_id = stock.asset_id
  AND s.country = stock.country
INNER JOIN on_rent_once oro
  ON s.asset_id = oro.asset_id
  AND s.country = oro.country
LEFT JOIN sub_info a
  ON s.asset_id = a.asset_id 
  AND s.country = a.country
LEFT JOIN latest_subscription ah
 ON ah.asset_id = a.asset_id 
 AND rowno = 1
LEFT JOIN master.subscription s2 
  ON ah.subscription_id = s2.subscription_id
WITH NO SCHEMA BINDING
;

ALTER TABLE hightouch_sources.b2b_list_of_asset_depreciation OWNER TO matillion;