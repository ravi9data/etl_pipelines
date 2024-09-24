SELECT
       oi.order_id AS order_id,
       o.customer_id as customer_id,
       o.submitted_date AS order_submission,
       o.store_country AS shipping_country,        
       opo.shippingpostalcode AS shipping_pc,                
       oi.quantity AS item_quantity,
       oi.product_name AS asset_name,
       oi.brand,        
       oi.subcategory_name,
       oi.variant_sku AS sku_variant,
       oi.product_sku AS sku_product,
       LEAST(p2."CreatedDate", spp.created_at) AS asset_introduction_date
FROM ods_production.order_item AS oi
LEFT JOIN ods_production.order opo ON opo.order_id = oi.order_id
LEFT JOIN master.order o ON oi.order_id = o.order_id
LEFT JOIN stg_api_production.spree_users AS su ON su.id = o.customer_id        
LEFT JOIN stg_salesforce.product2 AS p2 ON p2."sku_variant__c" = oi.variant_sku       
LEFT JOIN stg_api_production.spree_products AS spp ON spp.sku = oi.product_sku        
WHERE o.submitted_date >= '{start_date}'
       AND NOT o.status IN ('CART', 'ADDRESS')
       AND NOT (su.email like '%%@grover.com' or su.email like '%%@getgrover.com')
       AND NOT o.customer_type IN ('business_customer')
       AND NOT o.store_type IN ('offline')
       AND oi.order_id IS NOT NULL
       AND o.customer_id IS NOT NULL
       AND o.submitted_date IS NOT NULL
       AND oi.brand IS NOT NULL
       AND oi.subcategory_name IS NOT NULL
       AND o.store_country IN ('Germany', 'Austria', 'Netherlands', 'Spain');
