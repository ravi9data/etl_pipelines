SELECT o.spree_customer_id__c::int AS customer_id,
    o.spree_order_number__c AS order_id,
    json_extract_path_text(nd."data", 'info', 'fingerprint', 'signals', TRUE)
        AS signal_string,
    o.createddate AS order_submission,
    o.shippingpostalcode AS shipping_pc,
    o.shippingcountry AS shipping_country
FROM stg_salesforce."order" o
LEFT JOIN s3_spectrum_rds_dwh_order_approval.nethone_data nd ON
    o.spree_order_number__c = nd.order_id
LEFT JOIN stg_api_production.spree_orders AS so ON so."number" = o.spree_order_number__c
LEFT JOIN stg_api_production.spree_users su ON su.id = so.user_id
LEFT JOIN stg_api_production.spree_stores AS ss ON so.store_id = ss.id
WHERE NOT ss.offline
    AND NOT (o.shippingcountry || o.shippingpostalcode || o.shippingstreet LIKE 'Germany10179Holzmarktstr%11')
    AND su.user_type = 'normal_customer'
    AND o.spree_customer_id__c IS NOT NULL
    AND o.spree_order_number__c IS NOT NULL
    AND NOT (su.email LIKE '%@grover.com' OR su.email LIKE '%@getgrover.com')
    AND o.createddate >= '{start_date}';
