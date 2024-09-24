DROP TABLE IF EXISTS skyvia.procurement_data_livefeed;
CREATE TABLE skyvia.procurement_data_livefeed AS
WITH pr_info AS (
			SELECT request_id,
	 		count(purchase_request_item_id) AS total_PRIs,
	 		count(CASE WHEN net_quantity = 0 THEN  purchase_request_item_id END ) AS fully_booked_PRIs_in_PR,
	 		count(CASE WHEN (net_quantity > 0) AND (delivered_quantity > 0) THEN purchase_request_item_id END ) AS partially_booked_PRIs_in_PR,
	 		count(CASE WHEN (net_quantity) > 0 AND (delivered_quantity = 0) THEN purchase_request_item_id END ) AS fully_unbooked_PRIs_in_PR,
	 		sum(final_quantity) AS final_quantity_pr,
	 		sum(delivered_quantity) AS delivered_quantity_pr,
	 		sum(net_quantity) AS net_quantity_pr,
	 		sum(net_quantity*purchase_price) AS pending_delivery_asset_value_PR,
	 		sum(purchase_price*(delivered_quantity+net_quantity)) AS total_value_PR
	 		FROM ods_production.purchase_request_item
	 		GROUP BY 1)
,
first_book AS (
    SELECT purchase_request_item_id,
        min(created_date) AS first_assets_booked_date,
        max(created_date) AS fully_booked_date
    FROM ods_production.asset
    GROUP BY 1
)
,
duplicate_payments_eu AS (
	SELECT 
		round(amount_eur,2) AS amount_eur,
		count(*)
	FROM dm_commercial.sap_down_payments_tracking_eu_snapshot snap
	WHERE snap.loaded_at = (SELECT max(sdp.loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_eu_snapshot sdp
	GROUP BY 1,2
	HAVING count(*) > 1
)
,
down_payments_eu AS (
	SELECT 
		sum(amount_eur) AS amount_eur
	FROM dm_commercial.sap_down_payments_tracking_eu_snapshot eu
	WHERE eu.loaded_at = (SELECT max(loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_eu_snapshot sdp
	 AND NOT EXISTS (
	 				FROM duplicate_payments_eu dp
	 				)
	GROUP BY 1
)
,
unique_payments_eu AS (
	SELECT 
		amount_eur
	FROM duplicate_payments_eu  
	UNION 
	SELECT 
		amount_eur
	FROM down_payments_eu
)
,
duplicate_supplier_payments_eu AS (
	SELECT 
		sap_supplier_reference_nr,
		round(amount_eur,2) AS amount_eur,
		count(*)
	FROM dm_commercial.sap_down_payments_tracking_eu_snapshot snap
	WHERE snap.loaded_at = (SELECT max(loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_eu_snapshot sdp
	GROUP BY 1,2
	HAVING count(*) > 1
)
,
supplier_payments_eu AS (
	SELECT 
		sap_supplier_reference_nr,
		sum(amount_eur) AS amount_eur
	FROM dm_commercial.sap_down_payments_tracking_eu_snapshot eu
	WHERE eu.loaded_at = (SELECT max(loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_eu_snapshot snap
	 AND NOT EXISTS (
					SELECT sap_supplier_reference_nr
	 				FROM duplicate_supplier_payments_eu ds
	 				WHERE ds.sap_supplier_reference_nr = eu.sap_supplier_reference_nr
	 				)
	GROUP BY 1 
)
,
unique_supplier_payments_eu AS (
	SELECT 
		sap_supplier_reference_nr,
		amount_eur
	FROM duplicate_supplier_payments_eu
	UNION
	SELECT 
		sap_supplier_reference_nr,
		amount_eur
	FROM supplier_payments_eu
)
,
duplicate_payments_us AS (
	SELECT 
		round(amount_usd,2) AS amount_usd,
		count(*)
	FROM dm_commercial.sap_down_payments_tracking_us_snapshot snap
	WHERE loaded_at = (SELECT max(loaded_at)
						FROM dm_commercial.sap_down_payments_tracking_us_snapshot sdp
	GROUP BY 1,2
	HAVING count(*) > 1
)
,
down_payments_us AS (
	SELECT 
		sum(amount_usd) AS amount_usd
	FROM dm_commercial.sap_down_payments_tracking_us_snapshot us
	WHERE loaded_at = (SELECT max(loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_us_snapshot snap
	 AND NOT EXISTS (
	 				FROM duplicate_payments_us dp
	 				)
	GROUP BY 1
)
,
unique_payments_us AS (
	SELECT 
		amount_usd
	FROM duplicate_payments_us  
	UNION 
	SELECT 
		amount_usd
	FROM down_payments_us
)
,
duplicate_supplier_payments_us AS (
	SELECT 
		sap_supplier_reference_nr,
		round(amount_usd,2) AS amount_usd,
		count(*)
	FROM dm_commercial.sap_down_payments_tracking_us_snapshot snap
	WHERE loaded_at = (SELECT max(loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_us_snapshot sdp
	GROUP BY 1,2
	HAVING count(*) > 1
)
,
supplier_payments_us AS (
	SELECT 
		sap_supplier_reference_nr,
		sum(amount_usd) AS amount_usd
	FROM dm_commercial.sap_down_payments_tracking_us_snapshot us
	WHERE loaded_at = (SELECT max(loaded_at) 
						FROM dm_commercial.sap_down_payments_tracking_us_snapshot snap
	 AND NOT EXISTS (
					SELECT sap_supplier_reference_nr
	 				FROM duplicate_supplier_payments_us du
	 				WHERE du.sap_supplier_reference_nr = us.sap_supplier_reference_nr
	 				)
	GROUP BY 1 
)
,
unique_supplier_payments_us AS (
	SELECT 
		sap_supplier_reference_nr,
		amount_usd
	FROM duplicate_supplier_payments_us
	UNION
	SELECT 
		sap_supplier_reference_nr,
		amount_usd
	FROM supplier_payments_us
)
		SELECT 	i.purchase_request_item_id,
				i.request_id AS purchase_request_id,
				i.purchase_order_number,
				i.purchased_date,
				i.delivered_to_warehouse,
				i.delivery_number,
				i.supplier_name,
				CASE 
					WHEN i.supplier_account = 'Others' 
						THEN 'Grover Suppliers' 
					ELSE 'Partners' 
				END AS supplier_type,
				i.variant_sku,
				i.product_sku,
				i.product_name,
				i.product_brand,
				p.category_name,
				p.subcategory_name,
				i.eta,
				datediff('day',i.eta,CURRENT_DATE) AS days_since_eta,
				i.final_quantity,
				i.delivered_quantity,
				i.net_quantity,  
				i.request_status,
				CASE WHEN (net_quantity > 0) AND (delivered_quantity = 0) THEN 'Fully Unbooked'
					 WHEN (net_quantity > 0) AND (delivered_quantity > 0) THEN 'Partially booked'
					 WHEN (net_quantity < 1) AND (delivered_quantity > 0) THEN 'Fully Booked'
						 END is_booked,
				pri.total_PRIs,
				pri.fully_booked_PRIs_in_PR,
				pri.partially_booked_PRIs_in_PR,
				pri.fully_unbooked_PRIs_in_PR,
				pri.final_quantity_pr,
				pri.delivered_quantity_pr, 
				pri.net_quantity_pr,
				f.first_assets_booked_date,
				CASE
					WHEN is_booked = 'Fully Booked' 
						THEN f.fully_booked_date
    			END AS fully_booked_date,
    			i.purchase_price,
    			CASE 
	    			WHEN i.net_quantity > 0 
	    				THEN net_quantity * purchase_price 
	    		END AS pending_delivery_asset_value,
    			pending_delivery_asset_value_PR,
    			total_value_PR,
    			CASE
	    			WHEN i.purchase_order_number LIKE '%GRUS%'
	    				THEN coalesce(ups.amount_usd,spu.amount_usd)
	    			ELSE coalesce(up.amount_eur,sp.amount_eur)
    			END AS payment_amount,
    			total_value_pr - payment_amount AS delta_pr_vs_payment
				FROM ods_production.purchase_request_item i 
				LEFT JOIN pr_info pri 
					ON pri.request_id = i.request_id
				LEFT JOIN ods_production.product p 
					ON p.product_sku = i.product_sku
				LEFT JOIN first_book f 
					ON f.purchase_request_item_id = i.purchase_request_item_sfid
    			LEFT JOIN stg_salesforce.purchase_request__c pr 
    				ON pr.name = i.request_id
    			LEFT JOIN unique_payments_eu up 
    			LEFT JOIN unique_supplier_payments_eu sp 
    				ON i.purchase_order_number = sp.sap_supplier_reference_nr
    			LEFT JOIN unique_payments_us ups 
    			LEFT JOIN unique_supplier_payments_us spu
    				ON i.purchase_order_number = spu.sap_supplier_reference_nr
    			WHERE i.purchase_order_number != 'TEST-1'
				--where i.purchased_date >= current_date - 365
				--and i.net_quantity > 0
				--and request_status != 'CANCELLED'
				ORDER BY purchase_request_id DESC,
    			i.purchase_request_item_id DESC;
   
    	
BEGIN TRANSACTION;

DELETE FROM skyvia.procurement_data_livefeed_income_historical
WHERE procurement_data_livefeed_income_historical.date = current_date::date
	OR procurement_data_livefeed_income_historical.date <= dateadd('year', -2, current_date);

INSERT INTO skyvia.procurement_data_livefeed_income_historical
SELECT
	product_sku,
	COALESCE(SUM(net_quantity), 0) AS "Incoming (Proc Data)",
	current_date AS date
FROM skyvia.procurement_data_livefeed
WHERE net_quantity > 0
  AND request_status != 'CANCELLED'
  AND days_since_eta <= 60
GROUP BY 1;
 
END TRANSACTION;

GRANT SELECT ON skyvia.procurement_data_livefeed TO  redash_pricing ;
GRANT SELECT ON skyvia.procurement_data_livefeed TO  group pricing ;
GRANT SELECT ON skyvia.procurement_data_livefeed TO  hightouch_pricing ;
GRANT SELECT ON skyvia.procurement_data_livefeed TO  hightouch;
GRANT SELECT ON skyvia.procurement_data_livefeed TO tableau;
GRANT SELECT ON skyvia.procurement_data_livefeed TO GROUP mckinsey;

GRANT SELECT ON skyvia.procurement_data_livefeed_income_historical TO  redash_pricing ;
GRANT SELECT ON skyvia.procurement_data_livefeed_income_historical TO  group pricing ;
GRANT SELECT ON skyvia.procurement_data_livefeed_income_historical TO  hightouch_pricing ;
GRANT SELECT ON skyvia.procurement_data_livefeed_income_historical TO  hightouch;
GRANT SELECT ON skyvia.procurement_data_livefeed_income_historical TO tableau;
