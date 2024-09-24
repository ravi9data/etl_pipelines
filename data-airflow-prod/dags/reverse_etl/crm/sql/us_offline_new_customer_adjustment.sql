WITH submitted_orders AS (
    SELECT 
        a.order_id,
        a.submitted_date,
        a.store_country,
        a.new_recurring,
        a.customer_type,
        SUM(b.plan_duration * b.price)::DOUBLE PRECISION AS order_value --commited_subs_value
    FROM master."order" a
        LEFT JOIN ods_production.order_item b USING (order_id)
    WHERE a."status" NOT IN ('CANCELLED', 'DECLINED', 'FAILED FIRST PAYMENT')
        AND a.submitted_date::DATE >= CURRENT_DATE - 31
        AND a.store_country = 'United States'
        AND a.new_recurring = 'NEW'
        AND b.plan_duration IS NOT NULL 
        AND b.price IS NOT NULL  
    GROUP BY 1,2,3,4,5
),

customers_per_order AS (
    SELECT a.order_id,
           a.anonymous_id,
           b.submitted_date,
           b.order_value,
           b.store_country,
           b.new_recurring,
           b.customer_type
    FROM traffic.session_order_mapping a
        INNER JOIN submitted_orders b ON a.order_id = b.order_id
            AND a.session_start BETWEEN date_add('day', -30,  b.submitted_date::DATE) AND b.submitted_date
),

get_gclid AS (
    SELECT DISTINCT anonymous_id,
           marketing_click_id AS gclid,
           page_view_start AS click_time
    FROM traffic.page_views
    WHERE page_view_start::DATE >= CURRENT_DATE - 61
    AND marketing_click_id IS NOT NULL 
),

last_touch_gclid AS (
    SELECT DISTINCT 
        gclid,
        order_id,
        click_time,
        row_number() over(partition by order_id order by click_time desc) AS rn
    FROM get_gclid a
        INNER JOIN customers_per_order b ON a.anonymous_id = b.anonymous_id AND a.click_time BETWEEN DATE_ADD('day', -30,  b.submitted_date::DATE) AND b.submitted_date
),

submitted_gclid_ids AS (
    SELECT gclid
    FROM last_touch_gclid
    WHERE rn = 1
),

non_paid_orders AS (
    SELECT 
        a.order_id,
        a.submitted_date,
        a.store_country,
        a.new_recurring,
        a.customer_type,
        SUM(b.plan_duration * b.price)::DOUBLE PRECISION AS order_value --commited_subs_value
    FROM master."order" a
        LEFT JOIN ods_production.order_item b USING (order_id)
    WHERE a."status" IN ('DECLINED', 'CANCELLED')
        AND a.submitted_date::DATE >= CURRENT_DATE - 31
        AND a.store_country = 'United States'
        AND a.new_recurring = 'NEW'
        AND b.plan_duration IS NOT NULL 
        AND b.price IS NOT NULL  
    GROUP BY 1,2,3,4,5
),

non_paid_customers_per_order AS (
    SELECT a.order_id,
           a.anonymous_id,
           b.submitted_date,
           b.order_value,
           b.store_country,
           b.new_recurring,
           b.customer_type
    FROM traffic.session_order_mapping a
        INNER JOIN non_paid_orders b ON a.order_id = b.order_id
            AND a.session_start BETWEEN date_add('day', -30,  b.submitted_date::DATE) AND b.submitted_date
),

get_non_paid_gclid AS (
    SELECT DISTINCT a.anonymous_id,
           a.marketing_click_id AS gclid,
           a.page_view_start AS click_time
    FROM traffic.page_views a
    LEFT JOIN submitted_gclid_ids b ON a.marketing_click_id = b.gclid
    WHERE a.page_view_start::DATE >= CURRENT_DATE - 61
        AND a.marketing_click_id IS NOT NULL 
        AND b.gclid IS NULL
),


get_non_paid_last_touch_gclid AS (
    SELECT DISTINCT 
        gclid AS "google click id",
        order_id,
        click_time,
        row_number() over(partition by order_id order by click_time desc) AS rn,
        CASE 
            WHEN customer_type = 'normal_customer' 
            THEN 'New customer US B2C'
            ELSE 'New customer US B2B' 
        END AS "conversion name",
        CAST(DATE_ADD('hour', 2, submitted_date::timestamp) AS VARCHAR) + ' America/Los_Angeles' AS "conversion time",
        CAST(DATE_ADD('hour', 4, submitted_date::timestamp) AS VARCHAR) + ' America/Los_Angeles' AS "adjustment time",
        'RETRACT' AS "adjustment type",
        order_value AS "adjustment value",
        'USD' AS "adjustment value currency"
    FROM get_non_paid_gclid a
        INNER JOIN non_paid_customers_per_order b ON a.anonymous_id = b.anonymous_id AND a.click_time BETWEEN DATE_ADD('day', -30,  b.submitted_date::DATE) AND b.submitted_date
)

SELECT 
    "google click id",
    "conversion name",
    "conversion time",
    "adjustment time",
    "adjustment type",
    "adjustment value",
    "adjustment value currency"
FROM get_non_paid_last_touch_gclid
WHERE rn = 1;

