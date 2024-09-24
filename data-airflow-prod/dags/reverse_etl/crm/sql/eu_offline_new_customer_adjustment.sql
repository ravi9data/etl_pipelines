WITH submitted_orders AS (
    SELECT
        a.order_id,
        a.customer_id,
        a.submitted_date,
        a.customer_type,
        CASE WHEN a.marketing_campaign like 'at_%' THEN 'Austria'
             WHEN a.marketing_campaign like 'de_%' THEN 'Germany'
             WHEN a.marketing_campaign like 'nl_%' THEN 'Netherlands'
             WHEN a.marketing_campaign like 'es_%' THEN 'Spain'
             WHEN a.marketing_campaign like 'us_%' THEN 'United States'
             ELSE a.store_country
            end as country_name,
        c.marketing_source,
        SUM(b.plan_duration * b.price)::DOUBLE PRECISION AS order_value --commited_subs_value
    FROM master."order" a
             LEFT JOIN ods_production.order_item b USING (order_id)
             LEFT JOIN ods_production.order_marketing_channel c USING(order_id)
    WHERE a.paid_date IS NOT NULL AND paid_orders = 0
      AND a.submitted_date::DATE >= CURRENT_DATE - 30
      AND b.plan_duration IS NOT NULL
      AND b.price IS NOT NULL
      AND a.new_recurring = 'NEW'
      AND a.marketing_channel in (
                                  'Display Branding',
                                  'Display Performance',
                                  'Paid Search Brand',
                                  'Paid Search Non Brand'
        )
      AND c.marketing_source not in ('bing','criteo')
    GROUP BY 1,2,3,4,5,6),

     customers_per_order AS (
         SELECT distinct
             a.order_id,
             a.session_id,
             b.submitted_date,
             b.customer_type,
             b.order_value,
             b.country_name
         FROM traffic.session_order_mapping a
                  INNER JOIN submitted_orders b ON a.order_id = b.order_id
             AND a.session_start BETWEEN date_add('day', -30,  b.submitted_date::DATE) AND b.submitted_date
     ),

     get_gclid AS (
         SELECT DISTINCT
             session_id,
             marketing_click_id AS gclid,
             page_view_start AS click_time
         FROM traffic.page_views
         WHERE page_view_start::DATE >= CURRENT_DATE - 90
           AND marketing_click_id IS NOT NULL
           AND (marketing_medium = 'display'
             OR marketing_source = 'google')
           AND marketing_source NOT IN ('bing','criteo')
     ),

     last_touch_gclid AS (
         SELECT DISTINCT
             b.gclid AS "google click id",
             a.order_id,
             b.click_time,
             CASE 
                WHEN customer_type = 'normal_customer' THEN 'New customer EU B2C'
                ELSE 'New customer EU B2B' END AS "conversion name",
             row_number() over(partition by order_id order by click_time desc) AS rn,
             CAST(DATE_ADD('hour', 2, submitted_date::timestamp) AS VARCHAR) +
             CASE
                 WHEN a.country_name = 'Spain'
                     THEN ' Europe/Madrid'
                 WHEN a.country_name = 'Germany'
                     THEN ' Europe/Berlin'
                 WHEN a.country_name = 'Netherlands'
                     THEN ' Europe/Amsterdam'
                 WHEN a.country_name = 'Austria'
                     THEN ' Europe/Vienna'
                 ELSE ' Europe/Berlin'
                 END AS "conversion time",
             CAST(DATE_ADD('hour', 2, submitted_date::timestamp) AS VARCHAR) +
             CASE
                 WHEN a.country_name = 'Spain'
                     THEN ' Europe/Madrid'
                 WHEN a.country_name = 'Germany'
                     THEN ' Europe/Berlin'
                 WHEN a.country_name = 'Netherlands'
                     THEN ' Europe/Amsterdam'
                 WHEN a.country_name = 'Austria'
                     THEN ' Europe/Vienna'
                 ELSE ' Europe/Berlin'
                 END AS "adjustment time",
             'RETRACT' AS "adjustment type",
             null AS "adjustment value",
             'EUR' AS "adjustment value currency"
         FROM customers_per_order a
                  LEFT JOIN get_gclid b ON a.session_id = b.session_id AND b.click_time BETWEEN DATE_ADD('day', -30,  a.submitted_date::DATE) AND a.submitted_date
         WHERE b.gclid is not null
     )

SELECT 
    "google click id",
    "conversion name",
    "conversion time",
    "adjustment time",
    "adjustment type",
    "adjustment value",
    "adjustment value currency"
FROM last_touch_gclid
WHERE rn = 1;
