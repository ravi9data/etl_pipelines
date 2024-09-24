CREATE OR REPLACE VIEW dm_operations.v_lost_assets_cs AS (
WITH assets AS (
	SELECT 
		asset_id ,
		asset_name ,
		serial_number,
		variant_sku ,
		COALESCE(last_active_subscription_id ,subscription_id) AS subscription_id,
		product_sku,
		purchase_price_commercial
	FROM master.asset a 
	WHERE asset_status_original IN ('ON LOAN','IN DEBT COLLECTION','REPORTED AS STOLEN','LOST','RESERVED')
	)
,asset_hist AS (
		SELECT DISTINCT 
		asset_id ,
		asset_status_original,
		initial_price ,
		asset_value_linear_depr ,
		residual_value_market_price ,
		date
	FROM master.asset_historical ah 
	WHERE TRUE 
	AND (date = LAST_DAY(date) OR date = DATE_TRUNC('MONTH',CURRENT_DATE) OR date = CURRENT_DATE - 1)
	AND date>= dateadd(MONTH,-1,current_date)
	)
,subs AS (
	SELECT 
		subscription_id ,
		s.subscription_sf_id ,
		s.order_id ,
		s.customer_id ,
		s.start_date ,
		s.subscription_plan ,
		s.minimum_cancellation_date,
		s.country_name
	FROM master.subscription s )
	,FINAL AS (
SELECT 	
		a.asset_id ,
		a.asset_name ,
		a.serial_number,
		s.customer_id ,
		s.order_id ,
		a.subscription_id ,
		s.subscription_sf_id,
		s.country_name,
		s.start_date,
		ah.asset_status_original,
		a.product_sku ,
		a.variant_sku ,
		ah.initial_price ,
		a.purchase_price_commercial,
		ah.asset_value_linear_depr ,
		ah.residual_value_market_price ,
		s.minimum_cancellation_date,
		AH.DATE,
		ROW_NUMBER()OVER(PARTITION BY a.asset_id,date_trunc('month',ah.date) ORDER BY ah.date DESC )AS idx_desc
FROM assets a 
LEFT JOIN subs s
ON a.subscription_id =s.subscription_id 
LEFT JOIN asset_hist ah 
ON ah.asset_id =a.asset_id)
SELECT *
FROM FINAL WHERE idx_desc =1
)
WITH NO SCHEMA binding;