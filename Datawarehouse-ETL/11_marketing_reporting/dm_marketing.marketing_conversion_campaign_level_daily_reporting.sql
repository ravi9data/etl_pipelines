DROP TABLE IF EXISTS tmp_marketing_conversion_campaign_level_daily_reporting;
CREATE TEMP TABLE tmp_marketing_conversion_campaign_level_daily_reporting AS
WITH marketing_channel_attributes AS (
    SELECT DISTINCT
        channel_grouping
                  ,cash_non_cash
                  ,brand_non_brand
    FROM marketing.marketing_cost_channel_mapping mccm
    UNION ALL
    SELECT
        'Paid Search Brand' AS channel_grouping,
        'Cash' AS cash_non_cash ,
        'Performance' AS brand_non_brand
    UNION ALL
    SELECT
        'Paid Search Non Brand' AS channel_grouping,
        'Cash' AS cash_non_cash ,
        'Performance' AS brand_non_brand
    UNION ALL
    SELECT
        'Paid Social Branding' AS channel_grouping,
        'Cash' AS cash_non_cash ,
        'Performance' AS brand_non_brand
    UNION ALL
    SELECT
        'Paid Social Performance' AS channel_grouping,
        'Cash' AS cash_non_cash ,
        'Performance' AS brand_non_brand
    UNION ALL
    SELECT
        'Display Branding' AS channel_grouping,
        'Cash' AS cash_non_cash ,
        'Performance' AS brand_non_brand
    UNION ALL
    SELECT
        'Display Performance' AS channel_grouping,
        'Cash' AS cash_non_cash ,
        'Performance' AS brand_non_brand
)
   ,retention_group AS (
    SELECT DISTINCT new_recurring AS retention_group
    FROM master.order
)
   ,marketing_channel_mapping AS (
    SELECT DISTINCT
        s.marketing_channel
                  ,COALESCE(s.marketing_source,'n/a') AS marketing_source
                  ,COALESCE(s.marketing_medium,'n/a') AS marketing_medium
                  ,COALESCE(um.marketing_channel, CASE
                                                      WHEN s.marketing_channel IN ('Paid Search Brand', 'Paid Search Non Brand', 'Paid Social Branding',
                                                                                   'Paid Social Performance',
                                                                                   'Display Branding',
                                                                                   'Display Performance')
                                                          THEN s.marketing_channel || ' Other'
        END,s.marketing_channel,'n/a') AS marketing_channel_detailed
    FROM traffic.sessions s
             LEFT JOIN marketing.utm_mapping um
                       ON s.marketing_channel = um.marketing_channel_grouping
                           AND s.marketing_source = um.marketing_source
    WHERE DATE(session_start) >= '2021-10-01'
    UNION
    SELECT DISTINCT
        omc.marketing_channel
                  ,COALESCE(omc.marketing_source,'n/a') AS marketing_source
                  ,COALESCE(omc.marketing_medium,'n/a') AS marketing_medium
                  ,COALESCE(um.marketing_channel, CASE
                                                      WHEN omc.marketing_channel IN ('Paid Search Brand', 'Paid Search Non Brand', 'Paid Social Branding',
                                                                                     'Paid Social Performance',
                                                                                     'Display Branding',
                                                                                     'Display Performance')
                                                          THEN omc.marketing_channel || ' Other'
        END, omc.marketing_channel,'n/a') AS marketing_channel_detailed
    FROM master.ORDER o
             LEFT JOIN ods_production.order_marketing_channel omc
                       ON omc.order_id = o.order_id
             LEFT JOIN marketing.utm_mapping um
                       ON omc.marketing_channel = um.marketing_channel_grouping
                           AND omc.marketing_source = um.marketing_source
    WHERE o.created_date >= '2021-10-01'
)
   ,user_traffic_daily AS (
    SELECT
        s.session_start::DATE AS reporting_date
         ,CASE
              WHEN st.country_name IS NULL
                  AND s.geo_country='DE'
                  THEN 'Germany'
              WHEN st.country_name IS NULL
                  AND s.geo_country = 'AT'
                  THEN 'Austria'
              WHEN st.country_name IS NULL
                  AND s.geo_country = 'NL'
                  THEN 'Netherlands'
              WHEN st.country_name IS NULL
                  AND s.geo_country = 'ES'
                  THEN 'Spain'
              WHEN st.country_name IS NULL
                  AND s.geo_country = 'US'
                  THEN 'United States'
              ELSE COALESCE(st.country_name,'Germany')
        END AS country
         ,COALESCE(s.marketing_channel, 'n/a') AS marketing_channel
         ,COALESCE(o.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
         ,COALESCE(LOWER(CASE
                             WHEN s.session_start::DATE <= '2023-03-22' AND o.marketing_channel in ('Paid Social Branding',
                                                                                                    'Paid Social Performance')
                                 THEN s.marketing_content
                             ELSE s.marketing_campaign
        END),'n/a') AS marketing_campaign
         ,LOWER(COALESCE(s.marketing_term,'n/a')) AS marketing_term
         ,LOWER(COALESCE(s.marketing_content,'n/a')) AS marketing_content
         ,CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
         ,COUNT(s.session_id) AS traffic_daily_sessions
         ,COUNT(DISTINCT s.session_id) AS traffic_daily_unique_sessions
         ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_daily_unique_users
    FROM traffic.sessions s
             LEFT JOIN ods_production.store st
                       ON st.id = s.store_id
             LEFT JOIN marketing_channel_mapping o
                       ON s.marketing_channel = o.marketing_channel
                           AND COALESCE(s.marketing_source,'n/a') = o.marketing_source
                           AND COALESCE(s.marketing_medium,'n/a') = o.marketing_medium
             LEFT JOIN traffic.snowplow_user_mapping cu
                       ON s.anonymous_id = cu.anonymous_id
                           AND s.session_id = cu.session_id
    WHERE s.session_start::DATE >= '2022-06-01'
    GROUP BY 1,2,3,4,5,6,7,8
)
   ,cost AS (
    SELECT
        co.reporting_date
         ,COALESCE(co.country,'n/a') AS country
         ,CASE WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Search' THEN
                   CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
                        WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
                        WHEN co.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
                        WHEN co.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
                        ELSE 'Paid Search Non Brand' END
               WHEN COALESCE(co.channel_grouping,'n/a') = 'Display' THEN
                   CASE WHEN co.campaign_name ILIKE '%awareness%' OR co.campaign_name ILIKE '%traffic%' THEN 'Display Branding'
                        ELSE 'Display Performance' END
               WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Social' THEN
                   CASE WHEN co.campaign_name ILIKE '%brand%' THEN 'Paid Social Branding'
                        ELSE 'Paid Social Performance' END
               ELSE COALESCE(co.channel_grouping,'n/a')
        END AS marketing_channel
         ,COALESCE(CASE WHEN marketing_channel = 'Paid Search Non Brand' then channel || ' ' || 'Non Brand'
                        WHEN marketing_channel = 'Paid Search Brand' then channel || ' ' || 'Brand'
                        WHEN marketing_channel = 'Display Branding'  then channel || ' ' || 'Brand'
                        WHEN marketing_channel = 'Display Performance' then channel || ' ' || 'Performance'
                        WHEN marketing_channel = 'Paid Social Branding' then channel || ' ' || 'Brand'
                        WHEN marketing_channel = 'Paid Social Performance' then channel || ' ' || 'Performance'
                        ELSE co.channel END, 'n/a') AS marketing_channel_detailed
         ,LOWER(COALESCE(co.campaign_name,'n/a')) AS marketing_campaign
         ,lower(COALESCE(ad_set_name,'n/a')) AS marketing_term
         ,lower(COALESCE(ad_name,'n/a')) AS marketing_content
         ,r.retention_group
         ,SUM(CASE
                  WHEN r.retention_group = 'NEW'
                      THEN co.total_spent_eur * 0.8
                  ELSE co.total_spent_eur * 0.2
        END) AS cost
         ,SUM(CASE
                  WHEN r.retention_group = 'NEW' AND co.cash_non_cash = 'Cash'
                      THEN co.total_spent_eur * 0.8
                  WHEN co.cash_non_cash = 'Cash' THEN co.total_spent_eur * 0.2
        END) AS cost_cash
         ,SUM(CASE
                  WHEN r.retention_group = 'NEW' AND co.cash_non_cash = 'Non-Cash'
                      THEN co.total_spent_eur * 0.8
                  WHEN co.cash_non_cash = 'Non-Cash' THEN co.total_spent_eur * 0.2
        END) AS cost_non_cash
    FROM retention_group r
             CROSS JOIN marketing.marketing_cost_daily_combined co
             LEFT JOIN marketing.campaigns_brand_non_brand b on co.campaign_name=b.campaign_name
    WHERE co.reporting_date >= '2022-06-01'
    GROUP BY 1,2,3,4,5,6,7,8
)
   ,subs AS (
    SELECT
        s.created_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(omc.marketing_channel,'n/a') AS marketing_channel
         ,COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
         ,COALESCE(LOWER(CASE
                             WHEN o.created_date::DATE <= '2023-03-22' AND omc.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
                                 THEN omc.marketing_content
                             ELSE  omc.marketing_campaign
        END),'n/a') AS marketing_campaign
         ,lower(COALESCE(omc.marketing_content,'n/a')) AS marketing_content
         ,lower(COALESCE(omc.marketing_term,'n/a')) AS marketing_term
         ,o.new_recurring AS retention_group
         ,SUM(s.minimum_term_months) AS minimum_term_months
         ,COUNT(DISTINCT s.subscription_id) AS subscriptions
         ,COUNT(DISTINCT s.customer_id) AS customers
         ,SUM(s.subscription_value) as subscription_value
         ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
    FROM master.order o
             LEFT JOIN master.subscription s
                       ON s.order_id = o.order_id
             LEFT JOIN ods_production.order_marketing_channel omc
                       ON s.order_id = omc.order_id
             LEFT JOIN marketing_channel_mapping orm
                       ON omc.marketing_channel = orm.marketing_channel
                           AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
                           AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
    WHERE s.created_date::DATE >= '2022-06-01'
    GROUP BY 1,2,3,4,5,6,7,8
)
   ,cart_orders AS (
    SELECT
        o.created_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(omc.marketing_channel,'n/a') AS marketing_channel
         ,COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
         ,COALESCE(LOWER(CASE
                             WHEN o.created_date::DATE <= '2023-03-22' AND omc.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
                                 THEN omc.marketing_content
                             ELSE  omc.marketing_campaign
        END),'n/a') AS marketing_campaign
         ,o.new_recurring AS retention_group
         ,lower(COALESCE(omc.marketing_content,'n/a')) AS marketing_content
         ,lower(COALESCE(omc.marketing_term,'n/a')) AS marketing_term
         ,COUNT(DISTINCT CASE
                             WHEN o.cart_orders >= 1
                                 THEN o.order_id
        END) AS carts
    FROM master.order o
             LEFT JOIN ods_production.order_marketing_channel omc
                       ON o.order_id = omc.order_id
             LEFT JOIN marketing_channel_mapping orm
                       ON omc.marketing_channel = orm.marketing_channel
                           AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
                           AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
    WHERE o.created_date::DATE >= '2022-06-01'
    GROUP BY 1,2,3,4,5,6,7,8
)
   ,submitted_orders AS (
    SELECT
        o.submitted_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(omc.marketing_channel,'n/a') AS marketing_channel
         ,COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
         ,COALESCE(LOWER(CASE
                             WHEN o.created_date::DATE <= '2023-03-22' AND omc.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
                                 THEN omc.marketing_content
                             ELSE  omc.marketing_campaign
        END),'n/a') AS marketing_campaign
         ,o.new_recurring AS retention_group
         ,LOWER(COALESCE(omc.marketing_content,'n/a')) AS marketing_content
         ,LOWER(COALESCE(omc.marketing_term,'n/a')) AS marketing_term
         ,COUNT(DISTINCT CASE
                             WHEN o.completed_orders >= 1
                                 THEN o.order_id
        END) AS submitted_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.approved_date IS NOT NULL
                        THEN o.order_id
                    END) AS approved_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.failed_first_payment_orders >= 1
                        THEN o.order_id
                    END) AS failed_first_payment_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.cancelled_orders >= 1
                        THEN o.order_id
                    END) AS cancelled_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.failed_first_payment_orders >= 1 OR o.cancelled_orders >= 1
                        THEN o.order_id
                    END) AS drop_off_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1
                        THEN o.order_id
                    END) AS paid_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1 and o.new_recurring = 'NEW'
                        THEN o.customer_id
                    END) AS new_customers
        ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1 and o.new_recurring = 'RECURRING'
                        THEN o.customer_id
                    END) AS recurring_customers
    FROM master.order o
             LEFT JOIN ods_production.order_marketing_channel omc
                       ON o.order_id = omc.order_id
             LEFT JOIN marketing_channel_mapping orm
                       ON omc.marketing_channel = orm.marketing_channel
                           AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
                           AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
    WHERE o.submitted_date::DATE >= '2022-06-01'
    GROUP BY 1,2,3,4,5,6,7,8
)
   , dates_active_subs AS (
    SELECT DISTINCT
        datum AS fact_date
    FROM public.dim_dates
    WHERE datum <= current_date
      AND datum::DATE >= '2022-06-01'
)
   , active_subs AS (
    SELECT
        s.subscription_id,
        s.store_label,
        COALESCE(omc.marketing_channel,'n/a') AS marketing_channel,
        COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
        COALESCE(LOWER(CASE
                           WHEN o.created_date::DATE <= '2023-03-22' AND omc.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
                               THEN omc.marketing_content
                           ELSE  omc.marketing_campaign END),'n/a') AS marketing_campaign,
        LOWER(COALESCE(omc.marketing_content,'n/a')) AS marketing_content,
        LOWER(COALESCE(omc.marketing_term,'n/a')) AS marketing_term,
        o.new_recurring,
        s.customer_id,
        s.subscription_value_eur AS subscription_value_euro,
        s.end_date,
        s.fact_day,
        s.country_name AS country,
        s.subscription_value_lc AS actual_subscription_value
    FROM ods_production.subscription_phase_mapping s
             LEFT JOIN master.ORDER o
                       ON o.order_id = s.order_id
             LEFT JOIN ods_production.order_marketing_channel omc
                       ON o.order_id = omc.order_id
             LEFT JOIN marketing_channel_mapping orm
                       ON omc.marketing_channel = orm.marketing_channel
                           AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
                           AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
)
   , active_subs_value AS (
    SELECT
        fact_date AS reporting_date,
        COALESCE(s.country, 'n/a') AS country,
        COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
        COALESCE(s.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
        COALESCE(s.marketing_campaign) AS marketing_campaign,
        COALESCE(s.marketing_content) AS marketing_content,
        COALESCE(s.marketing_term) AS marketing_term,
        COALESCE(s.new_recurring, 'n/a') AS retention_group,
        COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
        COUNT(DISTINCT s.customer_id) AS active_customers,
        SUM(s.subscription_value_euro) as active_subscription_value,
        SUM(s.actual_subscription_value) as actual_subscription_value
    FROM dates_active_subs d
             LEFT JOIN active_subs s
                       ON d.fact_date::date >= s.fact_Day::date
                           AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
    WHERE country IS NOT NULL
      AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
    GROUP BY 1,2,3,4,5,6,7,8
)
   , dimensions_combined AS (
    SELECT DISTINCT
        reporting_date
                  ,country
                  ,marketing_channel
                  ,marketing_channel_detailed
                  ,marketing_campaign
                  ,retention_group
                  ,marketing_content
                  ,marketing_term
    FROM user_traffic_daily
    UNION
    SELECT DISTINCT
        reporting_date
                  ,country
                  ,marketing_channel
                  ,marketing_channel_detailed
                  ,marketing_campaign
                  ,retention_group
                  ,marketing_content
                  ,marketing_term
    FROM cost
    UNION
    SELECT DISTINCT
        reporting_date
                  ,country
                  ,marketing_channel
                  ,marketing_channel_detailed
                  ,marketing_campaign
                  ,retention_group
                  ,marketing_content
                  ,marketing_term
    FROM subs
    UNION
    SELECT DISTINCT
        reporting_date
                  ,country
                  ,marketing_channel
                  ,marketing_channel_detailed
                  ,marketing_campaign
                  ,retention_group
                  ,marketing_content
                  ,marketing_term
    FROM cart_orders
    UNION
    SELECT DISTINCT
        reporting_date
                  ,country
                  ,marketing_channel
                  ,marketing_channel_detailed
                  ,marketing_campaign
                  ,retention_group
                  ,marketing_content
                  ,marketing_term
    FROM submitted_orders
    UNION
    SELECT DISTINCT
        reporting_date
                  ,country
                  ,marketing_channel
                  ,marketing_channel_detailed
                  ,marketing_campaign
                  ,retention_group
                  ,marketing_content
                  ,marketing_term
    FROM active_subs_value
)
SELECT
    dc.reporting_date
     ,dc.country
     ,dc.marketing_channel
     ,dc.marketing_channel_detailed
     ,dc.marketing_campaign
     ,dc.retention_group
     ,dc.marketing_content
     ,dc.marketing_term
     ,mca.brand_non_brand
     ,mca.cash_non_cash
     ,COALESCE(s.minimum_term_months ,0) AS minimum_term_months
     ,COALESCE(tr.traffic_daily_sessions ,0) AS traffic_daily_sessions
     ,COALESCE(tr.traffic_daily_unique_sessions ,0) AS traffic_daily_unique_sessions
     ,COALESCE(tr.traffic_daily_unique_users ,0) AS traffic_daily_unique_users
     ,COALESCE(co.carts ,0) AS cart_orders
     ,COALESCE(so.submitted_orders ,0) AS submitted_orders
     ,COALESCE(so.approved_orders ,0) AS approved_orders
     ,COALESCE(so.failed_first_payment_orders ,0) AS failed_first_payment_orders
     ,COALESCE(so.cancelled_orders ,0) AS cancelled_orders
     ,COALESCE(so.drop_off_orders ,0) AS drop_off_orders
     ,COALESCE(so.paid_orders ,0) AS paid_orders
     ,COALESCE(s.subscriptions ,0) AS acquired_subscriptions
     ,COALESCE(s.subscription_value ,0) AS acquired_subscription_value
     ,COALESCE(s.committed_subscription_value ,0) AS committed_subscription_value
     ,COALESCE(s.customers ,0) AS customers
     ,COALESCE(so.new_customers ,0) AS new_customers
     ,COALESCE(so.recurring_customers,0) as recurring_customers
     ,COALESCE(c.cost ,0) AS cost
     ,COALESCE(c.cost_non_cash ,0) AS cost_non_cash
     ,COALESCE(c.cost_cash ,0) AS cost_cash
     ,COALESCE(asv.active_subscriptions,0) AS active_subscriptions
     ,COALESCE(asv.active_customers,0) AS active_customers
     ,COALESCE(asv.active_subscription_value,0) AS active_subscription_value
     ,COALESCE(asv.actual_subscription_value,0) AS actual_subscription_value
FROM dimensions_combined dc
    LEFT JOIN user_traffic_daily tr
        ON  dc.reporting_date = tr.reporting_date
            AND dc.country = tr.country
            AND dc.marketing_channel = tr.marketing_channel
            AND dc.marketing_channel_detailed = tr.marketing_channel_detailed
            AND dc.marketing_campaign = tr.marketing_campaign
            AND dc.retention_group = tr.retention_group
            AND dc.marketing_content = tr.marketing_content
            AND dc.marketing_term = tr.marketing_term
    LEFT JOIN cart_orders co
        ON  dc.reporting_date = co.reporting_date
            AND dc.country = co.country
            AND dc.marketing_channel = co.marketing_channel
            AND dc.marketing_channel_detailed = co.marketing_channel_detailed
            AND dc.marketing_campaign = co.marketing_campaign
            AND dc.retention_group = co.retention_group
            AND dc.marketing_content = co.marketing_content
            AND dc.marketing_term = co.marketing_term
    LEFT JOIN submitted_orders so
        ON  dc.reporting_date = so.reporting_date
            AND dc.country = so.country
            AND dc.marketing_channel = so.marketing_channel
            AND dc.marketing_channel_detailed = so.marketing_channel_detailed
            AND dc.marketing_campaign = so.marketing_campaign
            AND dc.retention_group = so.retention_group
            AND dc.marketing_content = so.marketing_content
            AND dc.marketing_term = so.marketing_term
    LEFT JOIN subs s
        ON  dc.reporting_date = s.reporting_date
            AND dc.country = s.country
            AND dc.marketing_channel = s.marketing_channel
            AND dc.marketing_channel_detailed = s.marketing_channel_detailed
            AND dc.marketing_campaign = s.marketing_campaign
            AND dc.retention_group = s.retention_group
            AND dc.marketing_content = s.marketing_content
            AND dc.marketing_term = s.marketing_term
    LEFT JOIN cost c
        ON  dc.reporting_date = c.reporting_date
            AND dc.country = c.country
            AND dc.marketing_channel = c.marketing_channel
            AND dc.marketing_channel_detailed = c.marketing_channel_detailed
            AND dc.marketing_campaign = c.marketing_campaign
            AND dc.retention_group = c.retention_group
            AND dc.marketing_content = c.marketing_content
            AND dc.marketing_term = c.marketing_term
    LEFT JOIN active_subs_value asv
        ON  dc.reporting_date = asv.reporting_date
            AND dc.country = asv.country
            AND dc.marketing_channel = asv.marketing_channel
            AND dc.marketing_channel_detailed = asv.marketing_channel_detailed
            AND dc.marketing_campaign = asv.marketing_campaign
            AND dc.retention_group = asv.retention_group
            AND dc.marketing_content = asv.marketing_content
            AND dc.marketing_term = asv.marketing_term
    LEFT JOIN marketing_channel_attributes mca
        ON dc.marketing_channel = CASE WHEN mca.channel_grouping = 'Influencers'
             AND mca.cash_non_cash = 'Cash'
            THEN mca.channel_grouping
                WHEN mca.channel_grouping <> 'Influencers'
            THEN mca.channel_grouping END
    WHERE dc.reporting_date != CURRENT_DATE;

DROP TABLE IF EXISTS dm_marketing.marketing_conversion_campaign_level_daily_reporting;
CREATE TABLE dm_marketing.marketing_conversion_campaign_level_daily_reporting AS
SELECT *
FROM tmp_marketing_conversion_campaign_level_daily_reporting;

GRANT SELECT ON dm_marketing.marketing_conversion_campaign_level_daily_reporting TO tableau;
GRANT SELECT ON dm_marketing.marketing_conversion_campaign_level_daily_reporting TO redash_pricing;

