--ALL assets
--when there is no event for Ingram Micro then we pull the SF info
--assets with SOLD as status, however we bring in sold info for all as potentially there are assets in different status but with sold info
--column to display for how long each asset is "locked" in the status (SF or IM) we are reporting with the mapping (x axis in tableau)

DROP TABLE dm_commercial.portfolio_overview_sf_and_im;
CREATE TABLE dm_commercial.portfolio_overview_sf_and_im AS
WITH salesforce_asset_history AS (
	SELECT 
		assetid AS asset_id,
		createddate AS last_status_time_salesforce,
		CASE WHEN oldvalue IS NULL THEN 'IN STOCK' ELSE newvalue END AS last_status_salesforce,
		ROW_NUMBER () OVER (PARTITION BY assetid ORDER BY createddate DESC) AS rowno 
	FROM stg_salesforce.asset_history ah 
	WHERE field = 'Status'
		OR field = 'in_stock_date__c' ---for first ever time in stock, otherwise filtered out
)
, asset_last_status_salesforce AS (
	SELECT 
		ah.asset_id,
		a.category_name,
		a.subcategory_name,
		a.brand,
		a.product_sku,
		a.serial_number,
		ah.last_status_time_salesforce,
		ah.last_status_salesforce,
		a.asset_status_original,
		a.asset_status_new,
		a.dpd_bucket AS debt_collection_status,
		a.warehouse,
		a.sold_price,
		a.sold_date,
		a.initial_price,
		a.purchase_price_commercial
	FROM salesforce_asset_history ah
	LEFT JOIN master.asset a
		ON a.asset_id = ah.asset_id
	WHERE ah.rowno = 1
		AND a.asset_id IS NOT NULL
)
, base_ingram_micro AS (
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
		AND shipping_number NOT LIKE 'KD%'
)
, ingram_micro_asset_history AS (
	SELECT 
		asset_serial_number_mapped,
		source_timestamp,
		disposition_code,
		status_code,
		ROW_NUMBER () OVER (PARTITION BY asset_serial_number_mapped ORDER BY source_timestamp DESC) AS row_no
	FROM base_ingram_micro 
)
, ingram_micro_last_status AS (
	SELECT 
		im.asset_serial_number_mapped,
		im.source_timestamp,
		im.disposition_code,
		im.status_code,
		n.status_code_grouped
	FROM ingram_micro_asset_history im
	LEFT JOIN staging_airbyte_bi.status_code_grouped n
		ON n.status_code = im.status_code
	WHERE im.row_no = 1
)
, serial_number_base AS (
	SELECT DISTINCT 
		asset_id, 
		serial_number,
		ROW_NUMBER () OVER (PARTITION BY serial_number ORDER BY date DESC) AS rowno   
	FROM master.asset_historical ah
	WHERE EXISTS (SELECT NULL FROM ingram_micro_last_status im WHERE im.asset_serial_number_mapped = ah.serial_number)
)
SELECT 
	sf.asset_id,
	COALESCE(sf.serial_number,sn.serial_number) AS serial_number,
	sf.category_name,
	sf.subcategory_name,
	sf.brand,
	sf.product_sku,
	sf.last_status_time_salesforce,
	sf.last_status_salesforce,
	sf.asset_status_original, --ideally should be matching the last status from SF history table, almost 300 cases for which is not the case   
	sf.asset_status_new,
	sf.debt_collection_status,
	sf.warehouse,
	im.source_timestamp AS last_status_ingram_time,
	im.status_code AS last_status_ingram,
	im.status_code_grouped AS last_status_ingram_new_bucket,
	datediff('month',im.source_timestamp,current_date) AS months_in_last_status_ingram_until_today,
	--assets only available in Salesforce and no Ingram events:	
	CASE 
		WHEN im.source_timestamp IS NULL 
		 AND im.status_code IS NULL 
		 AND sf.asset_status_original = 'ON LOAN'  
			THEN sf.asset_status_new --ON LOAN assets to be provided with the detail within the asset_status_new 
		WHEN im.source_timestamp IS NULL 
		 AND im.status_code IS NULL 
		 AND sf.asset_status_original <> 'ON LOAN'  
			THEN sf.asset_status_original
	--assets with Ingram events:  		
		WHEN sf.last_status_time_salesforce > im.source_timestamp --2)
		 AND im.source_timestamp IS NOT NULL 
		 AND sf.asset_status_original = 'ON LOAN'
			THEN sf.asset_status_new
		WHEN sf.last_status_time_salesforce::date = im.source_timestamp::date --3)
			THEN im.status_code
		WHEN sf.last_status_time_salesforce < im.source_timestamp --4)
		 AND im.source_timestamp IS NOT NULL 
		 AND sf.asset_status_original = 'ON LOAN' 
		 AND s.status = 'ACTIVE'
			THEN sf.asset_status_new
		WHEN sf.last_status_time_salesforce > im.source_timestamp --5)
		 AND im.source_timestamp IS NOT NULL 
		 AND sf.asset_status_new IN ('IN REPAIR','REFURBISHMENT') 
		 AND sf.warehouse = 'ingram_micro_eu_flensburg'
		 AND datediff('day',im.source_timestamp,sf.last_status_time_salesforce) < 7
			THEN im.status_code 
		 --general rule is: latest timestamp among Salesforce or Ingram 
		WHEN sf.last_status_time_salesforce > im.source_timestamp --1)
			THEN sf.asset_status_original --for everything ON LOAN status will be given the "detail" of the asset_status_new as mapped with previous conditions 
		WHEN sf.last_status_time_salesforce < im.source_timestamp
			THEN im.status_code
	--no else condition, everything is mapped	
	END AS mapping_level_2,    
	CASE 
		WHEN mapping_level_2 = im.status_code
			THEN im.status_code_grouped --some missing events in the mapping done by commercial, by refreshing the airbyte job we should be okay 
		ELSE --everything that is not Ingram: 
			CASE 
				WHEN sf.asset_status_original IN ('ON LOAN','IN STOCK','IN DEBT COLLECTION','TRANSFERRED TO WH','SELLING','SOLD') 
					THEN sf.asset_status_original
				WHEN sf.asset_status_original = 'RETURNED' 
					THEN 'RETURNS'
				WHEN sf.asset_status_original IN ('WRITTEN OFF DC','LOST','LOST SOLVED','IRREPARABLE','WRITTEN OFF OPS','REPORTED AS STOLEN','INSOLVENCY') --sold not remapped as filtered out below   
					THEN 'WRITTEN OFF'
				WHEN sf.asset_status_original IN ('INCOMPLETE','WARRANTY','WAITING FOR REFURBISHMENT','SENT FOR REFURBISHMENT') 
					THEN 'KITTING by Ingram'
				WHEN sf.asset_status_original IN ('INVESTIGATION WH','INBOUND QUARANTINE','INVESTIGATION CARRIER','INBOUND QUARANTINE','INVESTIGATION CARRIER','INBOUND DAMAGED','INBOUND UNALLOCABLE') 
					THEN 'QUARANTINE/CLEARING CASES'
				WHEN sf.asset_status_original = 'RETURNED' 
					THEN 'RETURNS'
				WHEN sf.asset_status_original = 'RESERVED' 
					THEN 'TRANSFER TO CUSTOMERS'
				WHEN sf.asset_status_original IN ('IN REPAIR','LOCKED DEVICE') 
					THEN 'REPAIR'
				WHEN sf.asset_status_original IN ('OFFICE','RECOVERED') 
					THEN 'OTHERS'   
			END	
	END	AS mapping_level_1,  --22 assets not mapped with weird states
	--date column as combination of last timestamp of Salesforce if that is the status prevaling, otherwise we take Ingram timestamp if Ingram status prevails
	--same logic on mapping_level_2:
	--assets only available in Salesforce and no Ingram events: 
	CASE 
		WHEN im.source_timestamp IS NULL
			THEN sf.last_status_time_salesforce
	--assets with Ingram events:		
		WHEN sf.last_status_time_salesforce > im.source_timestamp --2)
		 AND im.source_timestamp IS NOT NULL 
		 AND sf.asset_status_original = 'ON LOAN'
			THEN sf.last_status_time_salesforce
		WHEN sf.last_status_time_salesforce::date = im.source_timestamp::date --3)
			THEN im.source_timestamp
		WHEN sf.last_status_time_salesforce < im.source_timestamp --4)
		 AND im.source_timestamp IS NOT NULL 
		 AND sf.asset_status_original = 'ON LOAN' 
		 AND s.status = 'ACTIVE'
			THEN sf.last_status_time_salesforce
		WHEN sf.last_status_time_salesforce > im.source_timestamp --5)
		 AND im.source_timestamp IS NOT NULL 
		 AND sf.asset_status_new IN ('IN REPAIR','REFURBISHMENT') 
		 AND sf.warehouse = 'ingram_micro_eu_flensburg'
		 AND datediff('day',im.source_timestamp,sf.last_status_time_salesforce) < 7
			THEN im.source_timestamp 	
		WHEN sf.last_status_time_salesforce > im.source_timestamp --1)
			THEN sf.last_status_time_salesforce 
		WHEN sf.last_status_time_salesforce < im.source_timestamp
			THEN im.source_timestamp
	END AS fact_date_mapping_SF_or_IM,	
	datediff('month',fact_date_mapping_SF_or_IM,current_date) AS month_of_last_status_SF_or_IM,
	datediff('day',im.source_timestamp,sf.last_status_time_salesforce) AS days_among_last_im_status_and_last_sf_status,
	sf.sold_date,
	sf.sold_price,
	CASE 
		WHEN sf.sold_date >= sf.last_status_time_salesforce::date AND sf.sold_price IS NOT NULL--sold date available as date, not as timestamp 
			THEN TRUE 
		ELSE FALSE	
	END AS is_asset_sold,
	sf.initial_price,
	sf.purchase_price_commercial
FROM asset_last_status_salesforce sf
LEFT JOIN serial_number_base sn
	ON sn.asset_id = sf.asset_id
		AND sn.rowno = 1
LEFT JOIN ingram_micro_last_status im
	ON im.asset_serial_number_mapped = sn.serial_number
LEFT JOIN master.allocation a 
	ON a.asset_id = sf.asset_id
LEFT JOIN master.subscription s 
	ON s.subscription_id = a.subscription_id
WHERE a.is_last_allocation_per_asset IS TRUE 
	OR a.is_last_allocation_per_asset IS NULL --to include first time in stock with no allocation yet 
	--AND sf.asset_status_original <> 'SOLD' --sold assets back in the logic as Gabriel's request on 21.06.24
;

GRANT SELECT ON dm_commercial.portfolio_overview_sf_and_im TO tableau;


DELETE FROM dm_commercial.portfolio_overview_sf_and_im_historical
WHERE portfolio_overview_sf_and_im_historical.date = current_date::date
	OR portfolio_overview_sf_and_im_historical.date <= dateadd('year', -1, date_trunc('week',current_date));
	
INSERT INTO dm_commercial.portfolio_overview_sf_and_im_historical 
SELECT 
	si.*,
	current_date AS date
FROM dm_commercial.portfolio_overview_sf_and_im si	
;


GRANT SELECT ON dm_commercial.portfolio_overview_sf_and_im_historical TO  GROUP commercial, GROUP pricing;

GRANT SELECT ON dm_commercial.portfolio_overview_sf_and_im TO  GROUP commercial, GROUP pricing;

GRANT USAGE ON SCHEMA dm_commercial TO redash_commercial;
GRANT SELECT ON TABLE dm_commercial.portfolio_overview_sf_and_im TO redash_commercial;
GRANT SELECT ON TABLE dm_commercial.portfolio_overview_sf_and_im_historical TO redash_commercial;