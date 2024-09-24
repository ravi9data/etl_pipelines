
DROP TABLE IF EXISTS dm_operations.lost_report_current_losses ;
CREATE TABLE dm_operations.lost_report_current_losses AS
-- 1. Get previous status' and additional lost dates
WITH history_of_lost_assets AS 
(
SELECT
	ah.asset_id,
	ah."date",
	ah.asset_status_original AS asset_status,
	LAG(ah.asset_status_original) OVER (PARTITION BY ah.asset_id ORDER BY "date"::DATE ) prev_asset_status
FROM master.asset_historical ah 
QUALIFY COUNT(CASE WHEN ah.asset_status_original = 'LOST' THEN ah.asset_id END) OVER (PARTITION BY ah.asset_id) >= 1 -- Assets Lost AT LEAST once
)
, history_asset_status_changes AS 
(
SELECT
	asset_id,
	"date",
	asset_status,
	prev_asset_status,
	LAG(asset_status, 2) OVER (PARTITION BY asset_id ORDER BY "date"::DATE ) twice_prior_asset_status, -- Taking the status 2 status' ago TO see status BEFORE "INVESTIGATION" (not particularly useful)
	LAG(asset_status, 3) OVER (PARTITION BY asset_id ORDER BY "date"::DATE ) thrice_prior_asset_status, -- Taking the status 2 status' ago TO see status BEFORE "INVESTIGATION" (not particularly useful)
	CASE
		WHEN asset_status = 'LOST'
			THEN ROW_NUMBER() OVER ( PARTITION BY asset_id, asset_status ORDER BY "date" DESC )  -- asset can be lost multiple times, ONLY looking AT the most recent lost event
	END AS lost_row_num                                                                          -- One asset has been lost three times! 181 Assets lost twice!
FROM history_of_lost_assets
WHERE asset_status != prev_asset_status
	OR prev_asset_status IS NULL 
)
, asset_previous_status AS
(
SELECT
	asset_id,
	"date" AS lost_date_statusupdate, -- WHEN asset FIRST recorded AS lost 
	prev_asset_status,
	CASE
		WHEN prev_asset_status ILIKE '%INVESTIGATION%'
			THEN
				CASE
					WHEN twice_prior_asset_status ILIKE '%INVESTIGATION%'
						THEN thrice_prior_asset_status
					ELSE twice_prior_asset_status
				END
		ELSE prev_asset_status
	END AS 	prev_asset_status_filtered -- removing investigation status' because they don't provide value
FROM history_asset_status_changes
WHERE lost_row_num = 1 -- ONLY FOR lost cases WHERE was LAST lost event
)
-- 2. Get Prices
--, last_residual_market_prices AS  -- TO GET LAST positive residual price BEFORE asset gets lost (and residual price set to 0 in luxco)
--(
--SELECT DISTINCT 
--	l.asset_id,
--	LAST_VALUE(l.final_price) OVER (PARTITION BY l.asset_id ORDER BY l.reporting_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_residual_market_value
--FROM ods_production.spv_report_master l 
--INNER JOIN asset_previous_status a 
--	ON a.asset_id = l.asset_id 
--		AND l.reporting_date <= a.lost_date_statusupdate
--WHERE l.final_price > 0
--GROUP BY 1, l.final_price, l.reporting_date 
--)
, clean_last_allocation AS
(
SELECT
	al.asset_id,
	-- Dates for return logic
	al.return_delivery_date,
	al.return_delivery_date_old,
	al.refurbishment_start_at,
	al.return_shipment_at,
	al.delivered_at,
	al.shipment_at,
	-- Warehouse data
	al.warehouse ,
	al.return_warehouse,
	-- Shipping data
	al.customer_type,
	al.shipping_country,
	al.carrier,
	al.return_carrier,
	ROW_NUMBER() OVER (PARTITION BY al.asset_id ORDER BY al.allocated_at DESC ) row_num
FROM ods_operations.allocation_shipment al
)
, base AS 
(
SELECT
	a.asset_id,
	-- Asset status
	a.asset_status_original AS asset_status_master, -- Can obtain Assets IN DC, IN Investigation, etc. IN Tableau
	CASE WHEN a.asset_status_original = 'LOST' THEN 1 ELSE 0 END AS is_asset_lost,
	-- Lost information
	CASE WHEN is_asset_lost = 1 THEN a.lost_date END::DATE lost_date,
	CASE WHEN is_asset_lost = 1 THEN st.lost_date_statusupdate END AS lost_date_statusupdate, -- will SHOW lost dates FOR assets NO longer lost
	CASE WHEN is_asset_lost = 1 THEN a.lost_reason END lost_reason,
	 -- we can use this TO GET complete lost dates (i.e. coalesce (lost date, lost date from asset status) )
	st.prev_asset_status,
	st.prev_asset_status_filtered, -- removing "IN INVESTIGATION" FROM pre-LOST 
	-- Asset prices
	a.initial_price AS purchase_price,
--	CASE WHEN is_asset_lost = 1 THEN spv.last_residual_market_value END AS residual_market_price_before_lost,
	COALESCE( rmv.residual_value_market_price_written_off_and_lost, a.residual_value_market_price) residual_market_price_current,
	-- Warehouse data
	a.warehouse,	-- we ALSO have warehouse IN allocation TABLE (last per aset) which may contradict master asset.... we will assume master asset TABLE IS updated AND TRUE here.
	al.warehouse warehouse_al,
	al.return_warehouse return_warehouse_al,
	-- Shipping data
	al.customer_type,
	al.shipping_country,
	al.carrier,
	al.return_carrier,
	-- Location Logic ( Iteration 1 - needs boosting... "approximate" is important, this location will never be highly accurate)
	CASE
		WHEN al.asset_id IS NULL
				THEN 'AT WAREHOUSE'  
		WHEN COALESCE(al.return_delivery_date_old, al.return_delivery_date, al.refurbishment_start_at) IS NOT NULL
				THEN 'AT RETURN WAREHOUSE'
		WHEN al.return_shipment_at IS NOT NULL -- NO need TO ADD delivery date NOT NULL I suppose.
				THEN 'IN RETURN TRANSIT'
		--
		-- This one IS LARGE UNKNOWN. Customer can Ship back AND handle their own shipments. Asset could also be with Customer (DC case?).
		WHEN al.delivered_at IS NOT NULL AND al.return_shipment_at IS NULL
				THEN 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED)' 
		--
		WHEN al.shipment_at IS NOT NULL AND al.delivered_at IS NULL -- outbound transit
				THEN 'IN OUTBOUND TRANSIT'  
		ELSE 'OTHER'
	END AS asset_location_approximate, -- dates METHOD.... we can make this MORE robust later 
	CASE
		WHEN asset_location_approximate = 'AT WAREHOUSE' 
			THEN 'AT WAREHOUSE: ' || COALESCE(a.warehouse, 'NO WH Record')  
		WHEN asset_location_approximate = 'AT RETURN WAREHOUSE'
			THEN 'AT RETURN WAREHOUSE: ' || COALESCE(al.return_warehouse, 'No WH Record')
		WHEN asset_location_approximate = 'IN RETURN TRANSIT'
				THEN 'IN RETURN TRANSIT: ' || COALESCE(al.return_carrier, 'No Carrier Record')  
		WHEN asset_location_approximate = 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED)'
			THEN 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED): Carrier ' || COALESCE('possibly ' || al.return_carrier, 'not recorded')    
		WHEN asset_location_approximate = 'IN OUTBOUND TRANSIT' 
				THEN 'IN OUTBOUND TRANSIT: ' || COALESCE(al.carrier, 'No Carrier Record')
		ELSE 'OTHER'
	END AS asset_location_approximate_detailed,
	CASE
		-- uncertain attribution
		WHEN asset_location_approximate = 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED)'          
			THEN 'Uncertain'
		-- shortening for warehouse names
		WHEN asset_location_approximate_detailed ILIKE '%UPS%'
			THEN 'UPS'
		WHEN asset_location_approximate_detailed ILIKE '%ingram%'
			THEN 'IM'
		WHEN asset_location_approximate_detailed ILIKE '%syner%'
			OR asset_location_approximate_detailed ILIKE '%kiel%' 
				THEN 'Synerlogis'
		WHEN asset_location_approximate_detailed ILIKE '%office%'
			THEN 'Grover'
		-- carriers
		WHEN asset_location_approximate_detailed ILIKE '%record%'
			THEN 'Uncertain'
		WHEN asset_location_approximate_detailed ILIKE '%TRANSIT%'
			THEN LTRIM(SPLIT_PART(asset_location_approximate_detailed, ':', 2), ' ')
		ELSE 'Uncertain'
	END AS asset_location_approximate_holder,
	ROW_NUMBER() OVER (PARTITION BY a.asset_id ORDER BY a.updated_at DESC ) row_num -- future proof FOR dupicates
FROM master.asset a
LEFT JOIN clean_last_allocation al
	ON al.asset_id = a.asset_id
		AND al.row_num = 1 -- GET LAST allocation per asset
LEFT JOIN asset_previous_status st
	ON st.asset_id = a.asset_id
--LEFT JOIN last_residual_market_prices spv
--	ON spv.asset_id = a.asset_id
LEFT JOIN dm_risk.v_asset_value_written_off_and_lost rmv
	ON rmv.asset_id = a.asset_id
		AND a.asset_status_original IN ('WRITTEN OFF DC','LOST')
WHERE 
	currency = 'EUR' -- WE ONLY want EU market. 3 FILTERs TO be MORE robust.
		AND (a.country IS NULL OR a.country NOT ILIKE '%united states%')
			AND	(a.shipping_country IS NULL OR a.shipping_country NOT ILIKE '%united states%')   
)
SELECT
	-- Asset status
	asset_status_master, 
	is_asset_lost,
	-- Lost information
	lost_date,
	lost_date_statusupdate,
	lost_reason,
	CASE WHEN is_asset_lost = 1 THEN 'LOST ' || asset_location_approximate END AS lost_reason_enhanced,
	CASE WHEN is_asset_lost = 1 THEN 'LOST ' || asset_location_approximate_detailed END AS lost_reason_enhanced_detailed,
	CASE WHEN is_asset_lost = 1 THEN asset_location_approximate_holder END AS lost_reason_enhanced_brief, -- this IS directly comparable TO lost_reason field (just blamed party mentioned)
	prev_asset_status,
	prev_asset_status_filtered,
	-- Warehouse data
	warehouse,
	warehouse_al,
	return_warehouse_al,
	-- Shipping data
	customer_type,
	shipping_country,
	carrier,
	return_carrier,	
	asset_location_approximate, 
	asset_location_approximate_detailed,
	asset_location_approximate_holder,
	-- Asset Counts
	COUNT(DISTINCT asset_id) num_assets,
	-- Asset prices
	SUM(purchase_price) AS purchase_price,
--	SUM(residual_market_price_before_lost) AS residual_market_price_before_lost
	SUM(residual_market_price_current) residual_market_price_current
	-- Compensated Amount
	-- Add when available
FROM base
WHERE row_num = 1 -- Remove duplicates 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
;

GRANT ALL ON dm_operations.lost_report_current_losses TO tableau ; 
