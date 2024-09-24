DROP VIEW IF EXISTS marketing.v_braze_user_price_drop;
CREATE VIEW marketing.v_braze_user_price_drop AS
/*Base for wishlist items*/
WITH user_event_base AS (
    SELECT DISTINCT
        CASE WHEN te.event_name = 'Product Added to Wishlist' THEN 'add_to_wishlist'
             WHEN lower(te.event_name) = 'product removed from wishlist' THEN 'remove_from_wishlist'
             WHEN te.event_name IN ('Product Added', 'Product Added to Cart') THEN 'addToCart'
             WHEN te.event_name = 'Removed From Cart' THEN 'removeFromCart'
            END AS se_action,
        te.event_time AS collector_tstamp,
        ss.platform AS platform,
        te.device_type AS dvce_type,
        te.anonymous_id AS domain_user_id,
        te.user_id,
        te.order_id,
        CASE WHEN IS_VALID_JSON(te.properties) THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'sku'),''),
                                                             NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'eventProperty', 'productData', 'productSku'),''),
                                                             NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'product_sku'),'')) ELSE NULL END AS product_sku,
        CASE WHEN IS_VALID_JSON(te.properties) THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'variant'),''),
                                                             NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'eventProperty', 'productData', 'productVariant'),''),
                                                             NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'product_variant'),'')) ELSE NULL END AS product_variant,
        CASE WHEN s.country_name = 'Germany' THEN 'DE'
             WHEN s.country_name = 'Spain' THEN 'ES'
             WHEN s.country_name = 'Austria' THEN 'AT'
             WHEN s.country_name = 'Netherlands' THEN 'NL'
             WHEN s.country_name = 'United States' THEN 'US'
        END AS user_country_code,
        CASE WHEN IS_VALID_JSON(te.properties) THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'subscription_length'),''),
                                                             NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'eventProperty', 'productData','subscriptionLength'),'')) ELSE NULL END AS plan_duration,
        CASE WHEN IS_VALID_JSON(te.properties) THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'price'),''),
                                                             NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties,'eventProperty', 'productData','price'),'')) ELSE NULL END AS price,
        CASE WHEN IS_VALID_JSON(te.properties) THEN CASE WHEN JSON_EXTRACT_PATH_TEXT(te.properties,'store_id') IN (621,14) THEN TRUE ELSE FALSE END END AS is_us_store,
        LAST_VALUE(user_country_code) OVER (PARTITION BY te.anonymous_id ORDER BY te.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_country_last
    FROM segment.track_events te
        LEFT JOIN segment.sessions_web ss ON te.session_id = ss.session_id
        LEFT JOIN ods_production.store s ON te.store_id = s.id
    WHERE LOWER(te.event_name) IN ('product added to wishlist','product removed from wishlist','product added','removed from cart', 'product added to cart')
      AND te.event_time >= current_date  - 90
)

,products_in_cart_us_only AS (
    SELECT
        seb.user_id,
        seb.order_id,
        seb.product_sku,
        seb.product_variant AS variant_id,
        va.variant_sku,
        pro.product_name,
        seb.plan_duration,
        seb.price,
        pro.slug,
        SUM(CASE WHEN seb.se_action = 'addToCart'
                     THEN 1
                 WHEN seb.se_action = 'removeFromCart'
                     THEN -1
            END) > 0 AS is_items_in_cart
    FROM user_event_base seb
        LEFT JOIN ods_production.product pro
            ON seb.product_sku = pro.product_sku
        LEFT JOIN ods_production.variant va
            ON pro.product_id = va.product_id
                AND seb.product_variant = va.variant_id
    WHERE seb.se_action IN ('removeFromCart', 'addToCart')
      AND seb.user_id IS NOT NULL
      AND seb.product_sku IS NOT NULL
      AND is_us_store = TRUE
    GROUP BY 1,2,3,4,5,6,7,8,9
    HAVING is_items_in_cart = TRUE
)

   ,wishlist AS (
    SELECT
        'Wishlist' AS source_,
        seb.user_id AS customer_id,
        NULL AS order_id,
        NULL AS quantity,
        seb.product_sku,
        seb.product_variant AS variant_id,
        va.variant_sku,
        pro.product_name,
        NULL AS plan_duration,
        NULL::DECIMAL(8,2) AS price,
        pro.slug,
        seb.geo_country_last AS geo_country,
        SUM(CASE WHEN seb.se_action = 'add_to_wishlist'
                     THEN 1
                 WHEN seb.se_action = 'remove_from_wishlist'
                     THEN -1
            END) > 0 AS is_items_in_wishlist
    FROM user_event_base seb
        LEFT JOIN ods_production.product pro
            ON seb.product_sku = pro.product_sku
        LEFT JOIN ods_production.variant va
            ON pro.product_id = va.product_id
                AND seb.product_variant = va.variant_id
    WHERE TRUE
      AND seb.user_id IS NOT NULL
      AND seb.product_sku IS NOT NULL
      AND seb.se_action IN ('add_to_wishlist', 'remove_from_wishlist')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    HAVING is_items_in_wishlist IS TRUE
)

/*Attribute each customer to the online, non-B2B store id
 We will use this info to assign a store id for wishlist items*/
   ,customer_store_attribution AS (
    SELECT DISTINCT
        s.id AS store_id,
        s.store_label,
        s.country_name,
        c.customer_id
    FROM ods_production.store s
        LEFT JOIN master.customer c
            ON c.signup_country = s.country_name
    WHERE TRUE
      AND c.signup_country <> 'never_add_to_cart'
      AND store_label IN ('Grover - Austria online', 'Grover - Spain online',
                          'Grover - Netherlands online', 'Grover - Germany online', 'Grover - United States online')
      AND store_name NOT IN ('Grover B2B Austria', 'Grover B2B Germany', 'Grover B2B USA')
)

/*Check the first platform\device type used when generating the order_id
 We will treat fo every customer the shopping cart for each combination separetly*/
,shopping_cart_device_type AS (
    SELECT DISTINCT
        order_id,
        FIRST_VALUE(platform) OVER (PARTITION BY order_id ORDER BY collector_tstamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS platform,
        FIRST_VALUE(dvce_type) OVER (PARTITION BY order_id ORDER BY collector_tstamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dvce_type,
        FIRST_VALUE(geo_country_last) OVER (PARTITION BY order_id ORDER BY collector_tstamp DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_country_last
    FROM user_event_base
    WHERE se_action IN ('removeFromCart', 'addToCart')
)

,customer_order_country AS (
    SELECT DISTINCT
        order_id,
        customer_id::INT,
        store_country,
        created_date
    FROM master.order
    WHERE created_date >= CURRENT_DATE - 90

    UNION

    SELECT DISTINCT
        a.order_id,
        a.user_id::INT AS customer_id,
        'United States' AS store_country,
        b.collector_tstamp AS created_date
    FROM products_in_cart_us_only a
        LEFT JOIN user_event_base b USING (order_id, user_id)
)

/*Check the latest orders of the customers
 We will only consider the customers whose last order has Status = 'CART'*/
   ,shopping_cart_last_order AS (
    SELECT DISTINCT
        ord.customer_id,
        c.shipping_country,
        FIRST_VALUE(ord.order_id) OVER (PARTITION BY ord.customer_id, ord.store_country,
            COALESCE(dev.platform||dev.dvce_type,'webComputer') ORDER BY ord.created_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order_id,
        FIRST_VALUE(ord.created_date) OVER (PARTITION BY ord.customer_id,ord.store_country,
            COALESCE(dev.platform||dev.dvce_type,'webComputer') ORDER BY ord.created_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order_created_date,
        FIRST_VALUE(dev.geo_country_last) OVER (PARTITION BY c.customer_id ORDER BY ord.created_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_geo_country
    FROM customer_order_country ord
        LEFT JOIN shopping_cart_device_type dev
            ON ord.order_id = dev.order_id
        LEFT JOIN master.customer c
            ON ord.customer_id = c.customer_id
    WHERE TRUE
      AND ord.customer_id IS NOT NULL
)

   ,shopping_cart_segment AS (
    SELECT DISTINCT
        'Shopping_Cart' AS source_,
        ord.user_id::INT AS customer_id,
        COALESCE(csa.store_id, 621)::VARCHAR AS store_id,
        COALESCE(csa.store_label, 'Grover - United States online') AS store_label
/*As per request in BI-3616, if the customer has the same product with different
quantity variations, we take min quantity*/,
        CASE
            WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Germany' THEN 'DE'
            WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Spain' THEN 'ES'
            WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Austria' THEN 'AT'
            WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Netherlands' THEN 'NL'
            WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'United States' THEN 'US'
            END AS user_country_code,
        0::INT AS quantity,
        ord.product_sku,
        MAX(NULLIF(ord.variant_id,''))::VARCHAR AS variant_id,
        ord.product_name
/*As per request in BI-3616, if the customer has the same product with different
plan_duration variations, we take max plan_duration*/,
        0::INT AS plan_duration
/*Since we take max plan duration, this is associated with min price*/,
        MIN(oi.price)::DECIMAL(8,2) AS price,
        pro.slug,
        MAX(CASE WHEN sub.subscription_id IS NOT NULL
                     THEN 1 ELSE 0 END)::INT AS has_active_subscription_with_product
    FROM products_in_cart_us_only ord
        INNER JOIN shopping_cart_last_order b
                  ON ord.order_id = b.last_order_id
        LEFT JOIN customer_store_attribution csa
                  ON csa.customer_id = ord.user_id
        LEFT JOIN ods_production.order_item oi
                  ON oi.order_id = ord.order_id
                      AND oi.product_sku = ord.product_sku
                      AND oi.variant_id = ord.variant_id
        LEFT JOIN ods_production.subscription sub
                  ON ord.user_id = sub.customer_id
                      AND oi.product_sku = sub.product_sku
                      AND sub.status = 'ACTIVE'
        LEFT JOIN ods_production.variant var
                  ON oi.variant_sku = var.variant_sku
        LEFT JOIN ods_production.product pro
                  ON var.product_id = pro.product_id
        LEFT JOIN master.order o
                  ON o.order_id = ord.order_id AND o.status NOT IN ('CART', 'ADDRESS')
    WHERE TRUE
      AND o.order_id IS NULL
      AND oi.variant_id IS NOT NULL
    GROUP BY 1,2,3,4,5,7,9,12
)

   ,shopping_cart_master AS (
    SELECT DISTINCT
        'Shopping_Cart' AS source_,
        ord.customer_id::INT AS customer_id,
        ord.store_id::VARCHAR AS store_id,
        ord.store_label
/*As per BI-5301 we are introducing a country code on a customer level which follows the
rule of if we have shipping country we use the last shipping country of a customer
if we do not have shipping country we use the last browser location of the customer*/,
        CASE WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Germany' THEN 'DE'
             WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Spain' THEN 'ES'
             WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Austria' THEN 'AT'
             WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'Netherlands' THEN 'NL'
             WHEN COALESCE(b.shipping_country, b.last_geo_country) = 'United States' THEN 'US'
            END AS user_country_code,
/*As per request in BI-3616, if the customer has the same product with different
quantity variations, we take min quantity*/
        MIN(oi.quantity)::INT AS quantity,
        oi.product_sku,
        MAX(oi.variant_id)::VARCHAR AS variant_id,
        oi.product_name
/*As per request in BI-3616, if the customer has the same product with different
plan_duration variations, we take max plan_duration*/,
        MAX(oi.plan_duration)::INT AS plan_duration
/*Since we take max plan duration, this is associated with min price*/,
        MIN(oi.price)::DECIMAL(8,2) AS price,
        pro.slug,
        MAX(CASE WHEN sub.subscription_id IS NOT NULL
                     THEN 1 ELSE 0 END)::INT AS has_active_subscription_with_product
    FROM master.order ord
        INNER JOIN shopping_cart_last_order b
                  ON ord.order_id = b.last_order_id
        LEFT JOIN ods_production.order_item oi
                  ON ord.order_id = oi.order_id
        LEFT JOIN ods_production.subscription sub
                  ON ord.customer_id = sub.customer_id
                      AND oi.product_sku = sub.product_sku
                      AND sub.status = 'ACTIVE'
        LEFT JOIN ods_production.variant var
                  ON oi.variant_sku = var.variant_sku
        LEFT JOIN ods_production.product pro
                  ON var.product_id = pro.product_id
    WHERE TRUE
      AND ord.status IN ('CART', 'ADDRESS')
      AND oi.product_sku IS NOT NULL
    GROUP BY 1,2,3,4,5,7,9,12
)

   ,shopping_cart_final AS (
    SELECT *
    FROM shopping_cart_segment
    UNION
    SELECT *
    FROM shopping_cart_master
)

   ,shopping_cart_and_wishlist_union AS (
    SELECT
        *
         ,CURRENT_TIMESTAMP AS updated_at
    FROM shopping_cart_final

    UNION ALL

    SELECT DISTINCT
        wl.source_,
        wl.customer_id::INT as customer_id,
        COALESCE(csa.store_id,1)::VARCHAR AS store_id,
        COALESCE(csa.store_label, 'Grover - Germany online') AS store_label,
/*As per BI-5301 we are introducing a country code on a customer level which follows the
rule of if we have shipping country we use the last shipping country of a customer
if we do not have shipping country we use the last browser location of the customer*/
        COALESCE(CASE WHEN b.shipping_country = 'Germany' THEN 'DE'
                      WHEN b.shipping_country = 'Spain' THEN 'ES'
                      WHEN b.shipping_country = 'Austria' THEN 'AT'
                      WHEN b.shipping_country = 'Netherlands' THEN 'NL'
                      WHEN b.shipping_country = 'United States' THEN 'US'
                     END, wl.geo_country) AS user_country_code,
        wl.quantity::INT AS quantity,
        wl.product_sku,
        MAX(nullif(wl.variant_id,''))::VARCHAR AS variant_id,
        wl.product_name,
        wl.plan_duration::INT AS plan_duration,
        wl.price,
        wl.slug,
        MAX(CASE WHEN sub.subscription_id IS NOT NULL
                     THEN 1 ELSE 0 END) AS has_active_subscription_with_product,
        CURRENT_TIMESTAMP AS updated_at
    FROM wishlist wl
             LEFT JOIN ods_production.subscription sub
                       ON wl.customer_id = sub.customer_id
                           AND wl.product_sku = sub.product_sku
                           AND sub.status = 'ACTIVE'
             LEFT JOIN customer_store_attribution csa
                       ON wl.customer_id =  csa.customer_id
             LEFT JOIN shopping_cart_last_order b
                       ON b.customer_id = wl.customer_id
    WHERE variant_id IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
)

SELECT *
FROM shopping_cart_and_wishlist_union
WHERE user_country_code = CASE WHEN store_label = 'Grover - Germany online' THEN 'DE'
                               WHEN store_label = 'Grover - Netherlands online' THEN 'NL'
                               WHEN store_label = 'Grover - Austria online' THEN 'AT'
                               WHEN store_label = 'Grover - Spain online' THEN 'ES'
                               WHEN store_label = 'Grover - United States online' THEN 'US'
END
WITH NO SCHEMA BINDING;
