-- Preparing US MR
DROP TABLE IF EXISTS sorted_us_reviews;
CREATE TEMP TABLE sorted_us_reviews
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    x.customer_id,
    x.order_id,
    COALESCE(x.comments, x.reason) AS "comment",
    x.created_by,
    ROW_NUMBER () OVER (PARTITION BY x.order_id ORDER BY x.created_at DESC) AS rn
FROM stg_curated.risk_internal_us_risk_manual_review_order_v1 AS x;

DROP TABLE IF EXISTS sorted_us_reviews_first;
CREATE TEMP TABLE sorted_us_reviews_first
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    x.customer_id,
    x.order_id,
    x.comment AS lol,
    LEN(x.comment),
    CASE
	    WHEN (x.comment IS NOT NULL AND LEN(x.COMMENT) != 0) THEN x.created_by||': '||x.COMMENT
	ELSE NULL
	END AS "comment"
FROM sorted_us_reviews AS x
WHERE rn = 1;

-- SKUs market values
DROP TABLE IF EXISTS market_values;
CREATE TEMP TABLE market_values
SORTKEY(product_sku)
DISTKEY(product_sku)
AS
SELECT
    mv.product_sku,
    ROUND(COALESCE(mv.avg_market_price_new, mv.avg_market_price_agan, mv.avg_market_price_used), 2) AS market_value
FROM dwh.sku_market_valuation AS mv;

-- Order items with market_values
DROP TABLE IF EXISTS stg_order_items;
CREATE TEMP TABLE stg_order_items
SORTKEY(order_id)
DISTKEY(order_id)
AS
SELECT
    oi.order_id,
    REPLACE(oi.product_name, '"', '''') AS product_name,
    oi.variant_sku,
    oi.quantity,
    oi.total_price,
    oi.risk_label LIKE '%high_risk%' AS high_risk,
    oi.plan_duration,
    mv.market_value * oi.quantity AS market_value
FROM ods_production."order" AS o
JOIN ods_production.order_item AS oi ON o.order_id = oi.order_id
JOIN market_values AS mv ON oi.product_sku = mv.product_sku;

-- Orders market price and items dict 
DROP TABLE IF EXISTS final_order_items;
CREATE TEMP TABLE final_order_items
SORTKEY(order_id)
DISTKEY(order_id)
AS
SELECT
    o.order_id,
    SUM(o.market_value) AS market_price,
    '['||listagg(
        '{"product_name" :"'||o.product_name||
        '","sku":"'||o.variant_sku||
        '","quantity":'||o.quantity||
        ',"monthly_price":'||o.total_price||
        ',"high_risk":'||CASE WHEN o.high_risk THEN 1 ELSE 0 END||
        ',"plan_duration":'||o.plan_duration||
        ',"market_value":'||o.market_value||
        '}',
    ', ')
    ||']' AS product_details
FROM stg_order_items AS o
GROUP BY 1;

-- Subs sorted by date
DROP TABLE IF EXISTS sorted_subs;
CREATE TEMP TABLE sorted_subs
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    s.customer_id,
    REPLACE(s.order_product_name, '"', '''') AS order_product_name,
    s.status,
    s.start_date,
    (s.subscription_value*1000)::decimal as subscription_value,
    ROW_NUMBER () OVER (PARTITION BY s.customer_id ORDER BY s.start_date DESC) AS grouprownum
FROM ods_production.subscription AS s;

-- Subs dict by customers
DROP TABLE IF EXISTS subs_dict;
CREATE TEMP TABLE subs_dict
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    customer_id,
    '['||listagg(CASE
                    WHEN x.grouprownum < 15
                    THEN '{"product_name" :"'||x.order_product_name||
                        '","asv":'||ROUND(x.subscription_value/1000, 2)||
                         ',"status":"'||x.status||
                         '","creation_ts":'||extract('epoch' from x.start_date)||'}'
                    ELSE NULL
                END, ', ') WITHIN GROUP (ORDER BY x.start_date DESC)||']' AS subs_details
FROM sorted_subs AS x
GROUP BY 1;

-- Orders sorted by date
DROP TABLE IF EXISTS sorted_orders;
CREATE TEMP TABLE sorted_orders
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    o.customer_id,
    o.order_id,
    o.order_value,
    o.created_date,
    o.status,
    os.order_scoring_comments AS "comment",
    ROW_NUMBER () OVER (PARTITION BY o.customer_id ORDER BY o.created_date DESC) AS grouprownum
FROM ods_production."order" AS o
LEFT JOIN ods_production.order_scoring AS os ON o.order_id = os.order_id
WHERE o.status NOT IN ('CART', 'ADDRESS');

-- Customer previous orders dict
DROP TABLE IF EXISTS previous_orders_dict;
CREATE TEMP TABLE previous_orders_dict
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    x.customer_id,
    '['||listagg(CASE
                    WHEN x.grouprownum < 15
                    THEN '{"order_id" :"'||x.order_id||
                         '","order_value":'||x.order_value||
                         ',"status":"'||x.status||
                         '","comment":"'||COALESCE(mr_us."comment", x."comment", '')||
                         '","created_date":'||extract('epoch' from x.created_date)||
                         '}'
                    ELSE NULL
                END, ', ') WITHIN GROUP (ORDER BY x.created_date DESC)||']' AS previous_orders_details
FROM sorted_orders AS x
LEFT JOIN sorted_us_reviews_first AS mr_us ON x.order_id = mr_us.order_id
GROUP BY 1;

-- outstanding_asset_value per customer
DROP TABLE IF EXISTS outstanding_asset;
CREATE TEMP TABLE outstanding_asset
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    s.customer_id,
    COALESCE(SUM(sa.outstanding_purchase_price), 0) AS outstanding_asset_value
FROM ods_production.subscription AS s
LEFT JOIN ods_production.subscription_assets AS sa 
    ON s.subscription_id = sa.subscription_id
WHERE s.status = 'ACTIVE'
GROUP BY s.customer_id;

-- customers in DCA
DROP TABLE IF EXISTS dca_status;
CREATE TEMP TABLE dca_status
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
 d.customer_id,
 SUM(outstanding_assets)
FROM ods_production.debt_collection AS d
GROUP BY 1
HAVING SUM(outstanding_assets) > 0;

-- number of orders placed in the last month by customer
DROP TABLE IF EXISTS last_month_orders;
CREATE TEMP TABLE last_month_orders
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    customer_id,
    COUNT( distinct CASE WHEN submitted_date is not null THEN order_id END) AS last_month_submitted_orders
FROM ods_production."order"
WHERE created_date >= (CURRENT_DATE - 31)
GROUP BY customer_id;

-- Final table
DROP TABLE IF EXISTS  ods_production.order_manual_review_rules;
CREATE TABLE ods_production.order_manual_review_rules AS
SELECT
    o.order_id,
    o.submitted_date,
    o.customer_id,
    o.initial_scoring_decision,
    o.payment_method,
    CASE
        WHEN cp.email LIKE ('%gmx%') THEN 1
        WHEN p.paypal_email LIKE ('%gmx%') THEN 1
        WHEN p.shopper_email LIKE ('%gmx%') THEN 1
        ELSE 0
    END AS is_gmx_email,
    CASE
        WHEN p.payment_method NOT LIKE '%paypal%'
            THEN
                CASE
                    WHEN (((LOWER(p.cardholder_name) LIKE '%'||LOWER(cp.first_name)||'%') AND (LOWER(p.cardholder_name) LIKE '%'||LOWER(cp.last_name)||'%')))
                      OR (((LOWER(p.cardholder_name) LIKE '%'||LOWER(cp.first_name)||'%') AND (LOWER(p.cardholder_name) NOT LIKE '%'||LOWER(cp.last_name)||'%')))
                      OR (((LOWER(p.cardholder_name) NOT LIKE '%'||LOWER(cp.first_name)||'%') AND (LOWER(p.cardholder_name) LIKE '%'||LOWER(cp.last_name)||'%')))
                        THEN 0
                    ELSE 1  
                END
        ELSE
            CASE
                WHEN LOWER(COALESCE(p.paypal_email, cp.paypal_email)) = LOWER(cp.email)
                  OR SPLIT_PART(p.paypal_email, '@', 1) = SPLIT_PART(cp.email, '@', 1)
                    THEN 0
                ELSE 1
            END
    END AS mismatching_name,
    CASE
        WHEN p.payment_method NOT LIKE '%paypal%'
            THEN
                CASE
                    WHEN o.store_country = 'Germany' AND p.card_issuing_country = 'DE'
                      OR o.store_country = 'Austria' AND p.card_issuing_country = 'AT'
                      OR o.store_country = 'Netherlands' AND p.card_issuing_country = 'NL'
                      OR o.store_country = 'Spain' AND p.card_issuing_country = 'ES'
                      OR o.store_country = 'United States' AND p.card_issuing_country = 'US'
                        THEN 0
                    ELSE 1
                END 
        ELSE 0
    END AS is_foreign_card,
    CASE WHEN p.funding_source LIKE ('%PREPAID%') THEN 1 ELSE 0 END AS is_prepaid_card,               
    CASE WHEN o.shippingcity = o.billingcity THEN 0 ELSE 1 END AS mismatching_city,
    CASE WHEN (cs.fraud_type IS NOT NULL) AND (cs.fraud_type NOT LIKE '%IP_ADDRESS%') THEN 1 ELSE 0 END AS is_fraud_type,
    CASE WHEN cs.distinct_customers_with_ip > 1 THEN 1 ELSE 0 END AS is_duplicate_ip,
    CASE WHEN oi.ffp_orders_latest > 0 THEN 1 ELSE 0 END AS has_failed_first_payments,
    (is_gmx_email + mismatching_name + is_foreign_card + is_prepaid_card + mismatching_city + is_fraud_type + has_failed_first_payments) AS rules_score,
    CASE
        WHEN o.payment_method = 'paypal-gateway' THEN "atomic".levenshtein(LOWER(cp.email), LOWER(p.paypal_email))
        ELSE "atomic".levenshtein(LOWER(first_name || ' ' || last_name), LOWER(p.cardholder_name))
    END AS levenshtein_distance,
    CASE WHEN cs.is_negative_remarks IS True THEN 1 ELSE 0 END AS is_negative_remarks,
    oi.manual_review_orders,
    cp.schufa_zipcode_string = o.shippingpostalcode AS verified_zipcode,
    lmo.last_month_submitted_orders AS last_month_submitted_orders,
    pd.market_price,
    CASE WHEN ds.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_in_dca,
    oa.outstanding_asset_value,
    pd.product_details,
    COALESCE(sd.subs_details, '[]') AS subs_details,
    COALESCE(pod.previous_orders_details, '[]') AS previous_orders_details
FROM ods_production."order" AS o
LEFT JOIN ods_data_sensitive.customer_pii AS cp ON o.customer_id = cp.customer_id
LEFT JOIN ods_data_sensitive.order_payment_method AS p ON o.order_id = p.order_id
LEFT JOIN ods_production.customer_scoring AS cs ON o.customer_id = cs.customer_id
LEFT JOIN ods_production.customer_orders_details AS oi ON o.customer_id = oi.customer_id
LEFT JOIN last_month_orders AS lmo ON o.customer_id = lmo.customer_id
LEFT JOIN dca_status AS ds ON o.customer_id = ds.customer_id
LEFT JOIN outstanding_asset AS oa ON o.customer_id = oa.customer_id
LEFT JOIN final_order_items AS pd ON o.order_id = pd.order_id
LEFT JOIN subs_dict AS sd ON o.customer_id = sd.customer_id
LEFT JOIN previous_orders_dict AS pod ON o.customer_id = pod.customer_id
ORDER BY rules_score DESC
;

GRANT SELECT ON ods_production.order_manual_review_rules TO tableau;
