CREATE OR REPLACE VIEW dm_commercial.v_ingram_micro_assets_cycle AS 
WITH base_ingram_micro AS (
	SELECT 
		*,
		lag(asset_serial_number) OVER (PARTITION BY order_number ORDER BY source_timestamp DESC) AS following_serial_number,
		COALESCE(CASE WHEN asset_serial_number = '' THEN NULL ELSE asset_serial_number END,following_serial_number) AS asset_serial_number_combined,
		COALESCE(CASE 
					WHEN asset_serial_number_combined = '' 
						THEN NULL
					WHEN len(asset_serial_number_combined) < 8
						THEN serial_number
					ELSE asset_serial_number_combined END,serial_number) AS asset_serial_number_mapped
	FROM recommerce.ingram_micro_send_order_grading_status im
	WHERE status_code IS NOT NULL 
		AND status_code <> ''
		AND item_number <> 'GROVER_01'
)
, returned_assets AS (
	SELECT DISTINCT 
		source_timestamp AS returned_time,
		order_number,
		asset_serial_number_mapped,
		status_code AS return_status,
		ROW_NUMBER () OVER (PARTITION BY asset_serial_number_mapped,order_number ORDER BY source_timestamp) AS rowno
	FROM base_ingram_micro im
	WHERE status_code IN ('RETURNED RETURN', 'RETURN RECEIVED')
)
, disposed_assets AS (
	SELECT
        ra.asset_serial_number_mapped,
        ra.returned_time,
        ra.return_status,
        ra.order_number,
        gs1.source_timestamp AS channeling_time,
        gs2.source_timestamp AS channeling_time_backup,
        gs1.status_code AS channeling_status,
        gs2.status_code AS channeling_status_backup,   
        datediff('day',ra.returned_time,COALESCE( gs1.source_timestamp,gs2.source_timestamp)) AS return_to_channeling_time,
        ROW_NUMBER () OVER (PARTITION BY ra.asset_serial_number_mapped, ra.returned_time ORDER BY COALESCE( gs1.source_timestamp,gs2.source_timestamp)) AS row_no --channeling attribution, only to the closest returned time  
    FROM returned_assets ra
    LEFT JOIN base_ingram_micro gs1
        ON gs1.order_number = ra.order_number
        	AND gs1.asset_serial_number_mapped = ra.asset_serial_number_mapped   
            AND gs1.disposition_code = 'CHANNELING'
            AND gs1.source_timestamp > ra.returned_time --additional CONDITION here ON the time AS well
    LEFT JOIN base_ingram_micro gs2
        ON gs2.asset_serial_number_mapped = ra.asset_serial_number_mapped
            AND gs2.source_timestamp > ra.returned_time 
            AND gs2.disposition_code = 'CHANNELING'
    WHERE rowno = 1
)
, assets_in_repair_cycle AS ( 
	SELECT 
		da.asset_serial_number_mapped,
		da.returned_time,
		da.return_status,
		da.order_number,
		CASE
			WHEN da.return_to_channeling_time <= 20 AND (da.order_number IS NULL OR da.order_number ='None') 
				THEN da.channeling_time_backup
				ELSE channeling_time
		END AS channeling_time,
		CASE 
			WHEN da.return_to_channeling_time <= 20 AND (da.order_number IS NULL OR da.order_number ='None') 
				THEN da.channeling_status_backup
				ELSE da.channeling_status
		END AS channeling_status,
		gs.source_timestamp AS repair_event_time,
		gs.status_code AS repair_status,
		ROW_NUMBER () OVER (PARTITION BY da.asset_serial_number_mapped,da.returned_time ORDER BY gs.source_timestamp) AS rowno --also first event to attribute 
	FROM disposed_assets da
	LEFT JOIN base_ingram_micro gs
		ON gs.asset_serial_number_mapped = da.asset_serial_number_mapped
			AND gs.order_number = da.order_number  
			AND gs.status_code IN (  
								'PICKED UP BY CARRIER REPAIR',
								'SENT TO REPAIR PARTNER',
								'REPAIR PREALERT MISSING',
								'REPAIR PREALERT RECEIVED',
								--'AVAILABLE, LOCKED', --exclusion as per stakeholder request in BI-8746
								'REPAIR RECEIVED',
								'RETURNED REPAIR'
						    	)
	WHERE row_no = 1
)
, IM_asset_history AS (
	SELECT 
		rc.asset_serial_number_mapped,
		rc.returned_time,
		rc.return_status,
		rc.order_number,
		rc.channeling_time,
		rc.channeling_status,
		rc.repair_event_time,
		rc.repair_status,
		gs.source_timestamp AS after_repair_event_time,
		gs.status_code AS after_repair_status,
		ROW_NUMBER () OVER (PARTITION BY rc.asset_serial_number_mapped,rc.returned_time ORDER BY gs.source_timestamp) AS row_no
	FROM assets_in_repair_cycle rc
	LEFT JOIN base_ingram_micro gs
		ON gs.asset_serial_number_mapped = rc.asset_serial_number_mapped
			AND gs.source_timestamp > rc.returned_time
			AND gs.status_code IN (
								'TRANSFER TO WH',
								'SENT TO B2C CUSTOMER',
								'SENT TO B2B CUSTOMER',
								'PICKED UP BY CARRIER',
								'ALLOCATED',
								'AVAILABLE, GROVER',    --previously excluded, confirm with Biagio once back 
								'ALLOCATED WAREHOUSE'  --previously excluded, confirm with Biagio once back
								)
	WHERE rowno = 1 
)
, serial_number_base AS (
	SELECT DISTINCT 
		asset_id, 
		serial_number,
		ROW_NUMBER () OVER (PARTITION BY serial_number ORDER BY date DESC) AS rowno --removing cases of assets with asset_id associated to multiple serial numbers   
	FROM master.asset_historical ah
	WHERE EXISTS (SELECT NULL FROM IM_asset_history im WHERE im.asset_serial_number_mapped = ah.serial_number)
)	
, complete_cycle AS (
	SELECT 
		a.asset_id,
		a.category_name,
		a.subcategory_name, 
		a.product_sku, 
		a.product_name,
		a.purchased_date, 
		ah.asset_serial_number_mapped,
		ah.returned_time,
		ah.return_status,
		ah.order_number,
		ah.channeling_time,
		ah.channeling_status,
		ah.repair_event_time,
		ah.repair_status,
		ah.after_repair_event_time,
		ah.after_repair_status
	FROM IM_asset_history ah
	LEFT JOIN serial_number_base sn
		ON sn.serial_number = ah.asset_serial_number_mapped
		AND sn.rowno = 1
	LEFT JOIN master.asset a 
		ON a.asset_id = sn.asset_id   
	WHERE row_no = 1
)
, last_status_raw AS (
	SELECT 
		asset_serial_number_mapped,
		order_number,
		status_code AS current_status,
		source_timestamp,
		ROW_NUMBER() OVER (PARTITION BY asset_serial_number_mapped,order_number ORDER BY source_timestamp DESC) row_no --in use for join at row 195  --CHECK ON this asset_serial_number
	FROM base_ingram_micro im
)
, last_status AS (
	SELECT * 
	FROM last_status_raw
	WHERE row_no = 1
)
, awaiting_missing_parts AS (
	SELECT 
		asset_serial_number_mapped,
		order_number,
		CASE WHEN sum(CASE WHEN status_code = 'AWAITING PARTS' THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END AS had_awaiting_parts_phase,
		CASE WHEN sum(CASE WHEN status_code = 'PARTS ARRIVED' THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END AS had_parts_arrived_phase
	FROM base_ingram_micro 
	WHERE status_code IN ('PARTS ARRIVED','AWAITING PARTS')
	GROUP BY 1,2
)
, final_ AS (	
	SELECT 
		f.*,
		CASE WHEN had_awaiting_parts_phase IS TRUE THEN TRUE ELSE FALSE END AS had_awaiting_parts_phase,
		CASE WHEN had_parts_arrived_phase IS TRUE THEN TRUE ELSE FALSE END AS had_parts_arrived_phase
	FROM complete_cycle f 	
	LEFT JOIN awaiting_missing_parts p
		ON f.asset_serial_number_mapped = p.asset_serial_number_mapped
			AND f.order_number = p.order_number	  
)
SELECT 
	f.*,
	g.status_code_grouped
FROM final_ f
LEFT JOIN last_status s
	ON s.asset_serial_number_mapped = f.asset_serial_number_mapped
		AND s.order_number = f.order_number
LEFT JOIN staging_airbyte_bi.status_code_grouped g --in order to link to last status one of the newly mapped ones coming from commercial  
	ON s.current_status = g.status_code
WITH NO SCHEMA binding	
;

GRANT SELECT ON dm_commercial.v_ingram_micro_assets_cycle TO tableau;