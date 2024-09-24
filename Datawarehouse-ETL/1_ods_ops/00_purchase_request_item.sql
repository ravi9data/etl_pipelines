DROP TABLE IF EXISTS ods_production.purchase_request_item;
CREATE TABLE ods_production.purchase_request_item as 
WITH asset AS (
	SELECT 
		purchase_request_item__c ,
		count(id) AS booked_quantity,
		min(createddate) AS first_asset_in_pri_booked_at
	FROM stg_salesforce.asset 
	GROUP BY 1	
)
SELECT 
		item."Name" AS purchase_request_item_id,
		item.id AS purchase_request_item_sfid,
		item.createddate AS purchase_item_created_date,
		request."Name" AS request_id,
		request."CreatedDate" AS request_date,
		request.purchase_date__c AS purchase_date,
        request.purchased__c AS purchased_date,
		in_delivery_date__c AS in_delivery_date,
		request.delivery_number__c AS delivery_number,
        delivered_to_warehouse_at__c AS delivered_to_warehouse,
		s.supplier_name AS supplier_name,
		s.supplier_account AS supplier_account,
		eta__c AS eta,
		purchase_order_number__c AS purchase_order_number,
		CASE 
			WHEN purchase_order_number LIKE '%US%'
				THEN 'US'
			ELSE 'EU'
		END AS supplier_country,
		request.wh_goods_receipt__c AS wh_goods_receipt,
		item.delivered_text__c AS delivered,
		item.total__c AS total_price,
		(item.purchase_quantity__c-item.delivered__c) AS pENDing_delivery,
		coalesce(s.request_partner,'N/A') AS request_partner,
		item.purchase_price__c AS purchase_price,
        item.rrp__c AS rrp,
		item.purchase_quantity__c AS purchase_quantity,
		item.final_quantity__c AS final_quantity,
		item.effective_quantity__c AS effective_quantity,
		item.delivered__c AS delivered_quantity,
		item.effective_quantity__c - coalesce(item.delivered__c, 0) AS net_quantity,
		a.booked_quantity,
		request.status__c AS request_Status,
		CASE 
			WHEN (net_quantity > 0) AND (delivered_quantity = 0) 
				THEN 'Fully Unbooked'
			WHEN (net_quantity > 0) AND (delivered_quantity > 0) 
				THEN 'Partially Booked'
			WHEN (net_quantity < 1) AND (delivered_quantity > 0) 
				THEN 'Fully Booked'
		END is_pri_booked,
		count(purchase_request_item_id) 
			OVER 
				(PARTITION BY request_id) AS total_pris_in_pr,
		count(CASE 
				WHEN is_pri_booked = 'Fully Unbooked' 
					THEN purchase_request_item_id END) 
			OVER 
				(PARTITION BY request_id) AS unbooked_pri_in_pr,
		count(CASE 
				WHEN is_pri_booked = 'Partially Booked' 
					THEN purchase_request_item_id END) 
			OVER 
				(PARTITION BY request_id) AS partially_booked_pri_in_pr,
		count(CASE 
				WHEN is_pri_booked = 'Fully Booked' 
					THEN purchase_request_item_id END) 
			OVER 
				(PARTITION BY request_id) AS fully_booked_pri_in_pr,
		CASE 
			WHEN total_pris_in_pr = unbooked_pri_in_pr 
				THEN 'Fully Unbooked'
			WHEN total_pris_in_pr = fully_booked_pri_in_pr 
				THEN 'Fully Booked'
			ELSE 'Partially Booked' END AS is_pr_booked,
		a.first_asset_in_pri_booked_at,
		min(a.first_asset_in_pri_booked_at) 
			OVER 
				(PARTITION BY request_id) AS first_asset_in_pr_booked_at,
		p.sku_variant__c AS variant_sku,
		p.sku_product__c AS product_sku,
		p.name AS product_name,
        p.brand__c AS product_brand,
        request.purchase_payment_method__c AS capital_source_name,
        pr.category_name AS category
FROM stg_salesforce.purchase_request_item__c item
LEFT JOIN stg_salesforce.purchase_request__c request 
	ON request."Id" = item.purchase_request__c
LEFT JOIN ods_production.supplier s 
	ON request.supplier__c = s.supplier_id
LEFT JOIN stg_salesforce."Product2" p 
	ON p."Id"=item.variant__c
LEFT JOIN ods_production.product pr 
	ON p.sku_product__c = pr.product_sku 
LEFT JOIN asset a 
	ON a.purchase_request_item__c = item.id;

GRANT SELECT ON ods_production.purchase_request_item TO accounting_redash, operations_redash;

GRANT SELECT ON ods_production.purchase_request_item TO tableau;
GRANT SELECT ON ods_production.purchase_request_item TO GROUP recommerce;
