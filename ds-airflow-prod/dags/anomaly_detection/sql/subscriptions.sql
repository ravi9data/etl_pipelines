SELECT o.spree_customer_id__c::int AS customer_id,
    o.spree_order_number__c AS order_id,
    o."Status" AS order_status,
    o."ShippingPostalCode" AS shipping_pc,
    o.shippingcity || ' ' || o.shippingcountry || ' ' || o.shippingpostalcode || ' ' || o.shippingstreet AS shipping_address,
    o."ShippingCountry" AS shipping_country,
    su.user_type,
    su.email,
    so.created_at AS order_creation,
    o."CreatedDate" AS order_submission,
    ss.offline AS is_offline_shop
FROM
    stg_salesforce."order" o
LEFT JOIN stg_api_production.spree_orders AS so ON so."number" = o.spree_order_number__c
LEFT JOIN stg_api_production.spree_users su ON su.id = so.user_id
LEFT JOIN stg_api_production.spree_stores ss ON so.store_id = ss.id
WHERE o.createddate >= '{start_date}';
