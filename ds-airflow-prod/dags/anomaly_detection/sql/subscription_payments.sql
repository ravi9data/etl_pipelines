SELECT o.spree_customer_id__c::int AS customer_id,
       o.spree_order_number__c AS order_id,
       oi."Quantity" AS item_quantity,
       o."ShippingPostalCode" AS shipping_pc,
       o."ShippingCountry" AS shipping_country,
       o."CreatedDate" AS order_submission,
       o.payment_method__c AS payment_method
FROM stg_salesforce."orderitem" AS oi
LEFT JOIN stg_salesforce."order" AS o ON oi."OrderId" = o."Id"
LEFT JOIN stg_api_production.spree_orders AS so ON so."number" = o.spree_order_number__c
LEFT JOIN stg_api_production.spree_users AS su ON su.id = so.user_id
LEFT JOIN stg_api_production.spree_stores AS ss ON so.store_id = ss.id
WHERE NOT ss.offline
    AND NOT (o.shippingcountry || o.shippingpostalcode || o.shippingstreet LIKE 'Germany10179Holzmarktstr%%11')
    AND NOT (o.shippingcountry || o.shippingpostalcode || o.shippingstreet LIKE 'Germany10783Potsdamer%%125')    
    AND NOT (su.email like '%%@grover.com' or su.email like '%%@getgrover.com')
    AND su.user_type = 'normal_customer'
    AND o.spree_customer_id__c IS NOT NULL
    AND o.spree_order_number__c IS NOT NULL
    AND o."CreatedDate" >= '{start_date}';
