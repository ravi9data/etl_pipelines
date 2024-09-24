
DROP TABLE IF EXISTS dm_operations.lost_report_historical_losses ;
CREATE TABLE dm_operations.lost_report_historical_losses AS
WITH reporting_dates AS 
(
-- Note: Important to keep beginning of time interval (-3 yrs) consistent with allocation and asset history time filters below.
SELECT dd.datum
FROM public.dim_dates dd
WHERE dd.datum BETWEEN DATE_ADD('year', -3, CURRENT_DATE) AND DATE_ADD('day', -1, CURRENT_DATE) -- Will need TO aggrgate TO weekly, monthly, etc.. consider flooring it WITH a date trunc.
)
, allocation_history AS -- GET allocation fields AND allocation time PERIOD FOR later join
(
SELECT
	asset_id,
	allocation_id,
	allocated_at,
	LEAD(allocated_at) OVER (PARTITION BY asset_id ORDER BY allocated_at ASC) allocation_end,
	-- Allocation fields necessary for return reason logic
	shipping_country,
	return_delivery_date, 
	return_delivery_date_old,
	refurbishment_start_at,
	return_shipment_at,
	delivered_at,
	shipment_at,
	return_warehouse,
	warehouse,
	carrier,
	return_carrier
FROM ods_operations.allocation_shipment al 
WHERE shipping_country IS NULL 
	OR shipping_country NOT ILIKE '%united states%'
-- We can trim 177k allocations with this qualify statement. ENSURE TIME WINDOW (-3 yrs) IS THE SAME AS BEGINNING OF REPORTING WINDOW ! 
QUALIFY LEAD(allocated_at) OVER (PARTITION BY asset_id ORDER BY allocated_at ASC) IS NULL -- Ensure either last allocation per asset
	OR LEAD(allocated_at) OVER (PARTITION BY asset_id ORDER BY allocated_at ASC) > DATE_ADD('year', -3, CURRENT_DATE) -- OR allocation was active during time window
)
, asset_and_allocation_history AS -- JOIN historical asset DATA WITH the allocation that was active AT the snapshot date (i.e. last allocation at the time)
(
SELECT
	ah."date" AS snapshot_date,
	-- asset data
	ah.asset_id,
	ah.initial_price,
	ah.asset_status_original AS asset_status,
	-- allocation_data 
	alh.allocation_id,
	alh.shipping_country,
	alh.return_delivery_date,
	alh.return_delivery_date_old,
	alh.refurbishment_start_at,
	alh.return_shipment_at,
	alh.delivered_at,
	alh.shipment_at,
	alh.return_warehouse,
	alh.warehouse,
	alh.carrier,
	alh.return_carrier,
	-- check for duplicates
	ROW_NUMBER() OVER (PARTITION BY ah.asset_id, ah."date" ) row_num -- remove ANY duplicates that might have been caused by allocation join (indiscriminately)
FROM master.asset_historical ah 
LEFT JOIN allocation_history alh
	ON alh.asset_id = ah.asset_id
		AND ah."date" >= alh.allocated_at::DATE 
			AND ah."date" < COALESCE(alh.allocation_end::DATE, DATE_ADD('day', 1, CURRENT_DATE))
WHERE ah.currency = 'EUR'
		AND ah."date" >= DATE_ADD('year', -3, CURRENT_DATE) -- ENSURE TIME WINDOW IS THE SAME AS BEGINNING OF REPORTING WINDOW ! 
				AND (ah.country IS NULL OR ah.country NOT ILIKE '%united states%')
)
, time_series AS 
(
SELECT
	dd.datum,
	-- Lost statistics
--	CASE WHEN ah.asset_status = 'LOST' THEN REPLACE(COALESCE(ah.lost_reason, 'N/A'), 'Lost by ', '') END lost_reason, -- OLD lost reason, cleaned
	a.asset_status,
	-- Lost reason new (note - logic is duplicated across current and historic scripts)
	-- Location Logic ( Iteration 1 - needs boosting... "approximate" is important, this location will never be highly accurate)
	CASE
		WHEN a.allocation_id IS NULL
				THEN 'AT WAREHOUSE'  
		WHEN COALESCE(a.return_delivery_date_old, a.return_delivery_date, a.refurbishment_start_at) IS NOT NULL
				THEN 'AT RETURN WAREHOUSE'
		WHEN a.return_shipment_at IS NOT NULL -- NO need TO ADD delivery date NOT NULL I suppose.
				THEN 'IN RETURN TRANSIT'
		--
		-- This one IS LARGE UNKNOWN. Customer can Ship back AND handle their own shipments. Asset could also be with Customer (DC case?).
		WHEN a.delivered_at IS NOT NULL AND a.return_shipment_at IS NULL
				THEN 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED)' 
		--
		WHEN a.shipment_at IS NOT NULL AND a.delivered_at IS NULL -- outbound transit
				THEN 'IN OUTBOUND TRANSIT'  
		ELSE 'OTHER'
	END AS asset_location_approximate, -- dates METHOD.... we can make this MORE robust later 
	CASE
		WHEN asset_location_approximate = 'AT WAREHOUSE' 
			THEN 'AT WAREHOUSE: ' || COALESCE(a.warehouse, 'NO WH Record')  
		WHEN asset_location_approximate = 'AT RETURN WAREHOUSE'
			THEN 'AT RETURN WAREHOUSE: ' || COALESCE(a.return_warehouse, 'No WH Record')
		WHEN asset_location_approximate = 'IN RETURN TRANSIT'
				THEN 'IN RETURN TRANSIT: ' || COALESCE(a.return_carrier, 'No Carrier Record')  
		WHEN asset_location_approximate = 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED)'
			THEN 'WITH CUSTOMER or IN TRANSPORT (NOT TRACKED): Carrier ' || COALESCE('possibly ' || a.return_carrier, 'not recorded')    
		WHEN asset_location_approximate = 'IN OUTBOUND TRANSIT' 
				THEN 'IN OUTBOUND TRANSIT: ' || COALESCE(a.carrier, 'No Carrier Record')
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
	-- MEASURES	
	count(DISTINCT a.asset_id) num_assets,
--	count(DISTINCT CASE WHEN ah.asset_status = 'LOST' THEN ah.asset_id END) num_assets_lost,
	ROUND(SUM(a.initial_price),0) purchase_price
	-- include compensated amount later... may be able to backpopulate for asset_historical if we put it there. 
FROM reporting_dates dd
LEFT JOIN asset_and_allocation_history a
	ON a.snapshot_date = dd.datum
		AND a.row_num = 1
GROUP BY 1,2,3,4,5
)
SELECT *
FROM time_series
; 

GRANT ALL ON dm_operations.lost_report_historical_losses TO tableau ; 

