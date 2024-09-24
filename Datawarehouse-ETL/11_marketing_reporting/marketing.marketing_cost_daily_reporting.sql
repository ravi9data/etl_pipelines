
DROP TABLE IF EXISTS marketing.marketing_cost_daily_reporting;
CREATE TABLE marketing.marketing_cost_daily_reporting AS
WITH dates AS (
    SELECT DISTINCT
        datum AS reporting_date
    FROM public.dim_dates
    WHERE datum BETWEEN '2019-07-01' AND current_date - 1
)
   ,retention_group AS (
--THIS IS BAD PRACTICE TO USE A FACT TABLE AS A DIMENSION TABLE. LET'S IMPROVE BELOW QUERY LATER
    SELECT DISTINCT new_recurring AS retention_group
    FROM master."order" o
)
   ,marketing_channel AS (
--THIS IS BAD PRACTICE TO USE A FACT TABLE AS A DIMENSION TABLE. LET'S IMPROVE BELOW QUERY LATER
    SELECT DISTINCT
        marketing_channel AS channel
                  ,CASE WHEN marketing_channel IN ('Paid Search Brand','Paid Search Non Brand', 'Display Performance', 'Paid Social Performance','Paid Social Branding', 'Display Branding') THEN 'Performance'
                        ELSE COALESCE(ma.brand_non_brand, 'n/a') end AS brand_non_brand
    FROM master.ORDER o
             LEFT JOIN marketing.marketing_cost_channel_mapping ma
                       ON o.marketing_channel = ma.channel_grouping
    WHERE marketing_channel NOT IN ('Referrals')

    UNION

    SELECT DISTINCT
        channel_grouping AS channel
                  ,COALESCE(brand_non_brand, 'n/a') AS brand_non_brand
    FROM marketing.marketing_cost_channel_mapping mccm
)
   ,country AS (
    SELECT DISTINCT country
    FROM marketing.marketing_cost_channel_mapping mccm
)
   , customer_type AS (
    SELECT 'B2C'::varchar AS customer_type
    UNION
    SELECT 'n/a'::varchar AS customer_type
    UNION
    SELECT 'B2B Unknown Split'::varchar AS customer_type
    UNION
    SELECT 'B2B freelancer'::varchar AS customer_type
    UNION
    SELECT 'B2B nonfreelancer'::varchar AS customer_type
)
   ,dimensions_combined AS (
    SELECT *
    FROM dates
             CROSS JOIN retention_group
             CROSS JOIN marketing_channel
             CROSS JOIN country
             CROSS JOIN customer_type
)
   ,user_traffic_daily AS (
    SELECT
        session_start::DATE AS reporting_date
         , CASE
               WHEN s.first_page_url ILIKE '%/de-%' THEN 'Germany'
               WHEN s.first_page_url ILIKE '%/us-%' THEN 'United States'
               WHEN s.first_page_url ILIKE '%/es-%' THEN 'Spain'
               WHEN s.first_page_url ILIKE '%/nl-%' THEN 'Netherlands'
               WHEN s.first_page_url ILIKE '%/at-%' THEN 'Austria'
               WHEN s.first_page_url ILIKE '%/business_es-%' THEN 'Spain'
               WHEN s.first_page_url ILIKE '%/business-%' THEN 'Germany'
               WHEN s.first_page_url ILIKE '%/business_at-%' THEN 'Austria'
               WHEN s.first_page_url ILIKE '%/business_nl-%' THEN 'Netherlands'
               WHEN s.first_page_url ILIKE '%/business_us-%' THEN 'United States'
               WHEN st.country_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
               WHEN st.country_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
               WHEN st.country_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
               WHEN st.country_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
               WHEN st.country_name IS NULL AND s.geo_country = 'US' THEN 'United States'
               ELSE COALESCE(st.country_name,'Germany') END AS country
         ,COALESCE(s.marketing_channel,'n/a') AS channel
         ,CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
         ,CASE WHEN s.first_page_url ILIKE  '%/business%' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN s.first_page_url ILIKE  '%/business%' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN s.first_page_url ILIKE  '%/business%' THEN 'B2B Unknown Split'
               ELSE 'B2C' END AS customer_type
         ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_daily_unique_users
    FROM traffic.sessions s
             LEFT JOIN ods_production.store st
                       ON st.id = s.store_id
             LEFT JOIN traffic.snowplow_user_mapping cu
                       ON s.anonymous_id = cu.anonymous_id
                           AND s.session_id = cu.session_id
             LEFT JOIN master.customer mc
                       ON s.customer_id::varchar = mc.customer_id::varchar
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
/*IN ORDER TO HAVE WEEKLY UNIQUE USERS, WE ARE ALMOST RUNNIG THE IDENTICAL QUERY
 LIKE IN user_traffic_daily CTE WITH ONLY ONE COLUMN DIFFERENCE.
 LET'S CHECK IF WE CAN IMPROVE THIS LATER*/
   ,user_traffic_weekly AS (
    SELECT
        DATE_TRUNC('week', session_start)::DATE AS reporting_date
         , CASE
               WHEN s.first_page_url ILIKE '%/de-%' THEN 'Germany'
               WHEN s.first_page_url ILIKE '%/us-%' THEN 'United States'
               WHEN s.first_page_url ILIKE '%/es-%' THEN 'Spain'
               WHEN s.first_page_url ILIKE '%/nl-%' THEN 'Netherlands'
               WHEN s.first_page_url ILIKE '%/at-%' THEN 'Austria'
               WHEN s.first_page_url ILIKE '%/business_es-%' THEN 'Spain'
               WHEN s.first_page_url ILIKE '%/business-%' THEN 'Germany'
               WHEN s.first_page_url ILIKE '%/business_at-%' THEN 'Austria'
               WHEN s.first_page_url ILIKE '%/business_nl-%' THEN 'Netherlands'
               WHEN s.first_page_url ILIKE '%/business_us-%' THEN 'United States'
               WHEN st.country_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
               WHEN st.country_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
               WHEN st.country_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
               WHEN st.country_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
               WHEN st.country_name IS NULL AND s.geo_country = 'US' THEN 'United States'
               ELSE COALESCE(st.country_name,'Germany') END AS country
         ,COALESCE(s.marketing_channel,'n/a') AS channel
         ,CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
         ,CASE WHEN s.first_page_url ILIKE  '%/business%' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN s.first_page_url ILIKE  '%/business%' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN s.first_page_url ILIKE  '%/business%' THEN 'B2B Unknown Split'
               ELSE 'B2C' END AS customer_type
         ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_weekly_unique_users
    FROM traffic.sessions s
             LEFT JOIN ods_production.store st
                       ON st.id = s.store_id
             LEFT JOIN traffic.snowplow_user_mapping cu
                       ON s.anonymous_id = cu.anonymous_id
                           AND s.session_id = cu.session_id
             LEFT JOIN master.customer mc
                       ON s.customer_id::varchar = mc.customer_id::varchar
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
/*IN ORDER TO HAVE MONTHLY UNIQUE USERS, WE ARE ALMOST RUNNIG THE IDENTICAL QUERY
 LIKE IN user_traffic_weekly CTE WITH ONLY ONE COLUMN DIFFERENCE.
 LET'S CHECK IF WE CAN IMPROVE THIS LATER*/
   ,user_traffic_monthly AS (
    SELECT
        DATE_TRUNC('month', session_start)::DATE AS reporting_date
         , CASE
               WHEN s.first_page_url ILIKE '%/de-%' THEN 'Germany'
               WHEN s.first_page_url ILIKE '%/us-%' THEN 'United States'
               WHEN s.first_page_url ILIKE '%/es-%' THEN 'Spain'
               WHEN s.first_page_url ILIKE '%/nl-%' THEN 'Netherlands'
               WHEN s.first_page_url ILIKE '%/at-%' THEN 'Austria'
               WHEN s.first_page_url ILIKE '%/business_es-%' THEN 'Spain'
               WHEN s.first_page_url ILIKE '%/business-%' THEN 'Germany'
               WHEN s.first_page_url ILIKE '%/business_at-%' THEN 'Austria'
               WHEN s.first_page_url ILIKE '%/business_nl-%' THEN 'Netherlands'
               WHEN s.first_page_url ILIKE '%/business_us-%' THEN 'United States'
               WHEN st.country_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
               WHEN st.country_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
               WHEN st.country_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
               WHEN st.country_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
               WHEN st.country_name IS NULL AND s.geo_country = 'US' THEN 'United States'
               ELSE COALESCE(st.country_name,'Germany') END AS country
         ,COALESCE(s.marketing_channel,'n/a') AS channel
         ,CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
         ,CASE WHEN s.first_page_url ILIKE  '%/business%' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN s.first_page_url ILIKE  '%/business%' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN s.first_page_url ILIKE  '%/business%' THEN 'B2B Unknown Split'
               ELSE 'B2C' END AS customer_type
         ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_monthly_unique_users
    FROM traffic.sessions s
             LEFT JOIN ods_production.store st
                       ON st.id = s.store_id
             LEFT JOIN traffic.snowplow_user_mapping cu
                       ON s.anonymous_id = cu.anonymous_id
                           AND s.session_id = cu.session_id
             LEFT JOIN master.customer mc
                       ON s.customer_id::varchar = mc.customer_id::varchar
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
   ,cost AS (
    SELECT
        reporting_date
         ,COALESCE(country,'n/a') AS country
         ,CASE WHEN COALESCE(channel_grouping,'n/a') = 'Paid Search' THEN
                   CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
                        WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
                        WHEN a.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
                        WHEN a.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
                        ELSE 'Paid Search Non Brand' END
               WHEN COALESCE(channel_grouping,'n/a') = 'Display' THEN
                   CASE WHEN a.campaign_name ILIKE '%awareness%' OR a.campaign_name ILIKE '%traffic%' THEN 'Display Branding'
                        ELSE 'Display Performance' END
               WHEN COALESCE(channel_grouping,'n/a') = 'Paid Social' THEN
                   CASE WHEN a.campaign_name ILIKE '%brand%' THEN 'Paid Social Branding'
                        ELSE 'Paid Social Performance' END
               ELSE COALESCE(channel_grouping,'n/a') END AS channel
         ,CASE WHEN channel IN ('Paid Search Brand','Paid Search Non Brand', 'Display Performance', 'Paid Social Performance','Display Branding', 'Paid Social Branding') THEN 'Performance'
               ELSE COALESCE(a.brand_non_brand,'n/a') END AS brand_non_brand
         ,'NEW' AS retention_group
         ,CASE WHEN a.customer_type = 'B2C' THEN 'B2C'
               WHEN a.customer_type = 'B2B' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,SUM(CASE WHEN a.cash_non_cash = 'Non-Cash' THEN total_spent_eur END) * 0.8 AS cost_non_cash
         ,SUM(CASE WHEN a.cash_non_cash = 'Cash' THEN total_spent_eur END) * 0.8 AS cost_cash
         ,SUM(total_spent_eur) * 0.8 AS cost
    FROM marketing.marketing_cost_daily_combined a
             LEFT JOIN marketing.campaigns_brand_non_brand b on a.campaign_name=b.campaign_name
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
        reporting_date
         ,COALESCE(country,'n/a') AS country
         ,CASE WHEN COALESCE(channel_grouping,'n/a') = 'Paid Search' THEN
                   CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
                        WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
                        WHEN a.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
                        WHEN a.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
                        ELSE 'Paid Search Non Brand' END
               WHEN COALESCE(channel_grouping,'n/a') = 'Display' THEN
                   CASE WHEN a.campaign_name ILIKE '%awareness%' OR a.campaign_name ILIKE '%traffic%' THEN 'Display Branding'
                        ELSE 'Display Performance' END
               WHEN COALESCE(channel_grouping,'n/a') = 'Paid Social' THEN
                   CASE WHEN a.campaign_name ILIKE '%brand%' THEN 'Paid Social Branding'
                        ELSE 'Paid Social Performance' END
               ELSE COALESCE(channel_grouping,'n/a') END AS channel
         ,CASE WHEN channel IN ('Paid Search Brand','Paid Search Non Brand', 'Display Performance', 'Paid Social Performance','Display Branding', 'Paid Social Branding') THEN 'Performance'
               ELSE COALESCE(a.brand_non_brand,'n/a') END AS brand_non_brand
         ,'RECURRING' AS retention_group
         ,CASE WHEN a.customer_type = 'B2C' THEN 'B2C'
               WHEN a.customer_type = 'B2B' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,SUM(CASE WHEN a.cash_non_cash = 'Non-Cash' THEN total_spent_eur END) * 0.2 AS cost_non_cash
         ,SUM(CASE WHEN a.cash_non_cash = 'Cash' THEN total_spent_eur END) * 0.2 AS cost_cash
         ,SUM(total_spent_eur) * 0.2 AS cost
    FROM marketing.marketing_cost_daily_combined a
             LEFT JOIN marketing.campaigns_brand_non_brand b on a.campaign_name=b.campaign_name
    GROUP BY 1,2,3,4,5,6
)
   ,subs AS (
    SELECT
        s.created_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(o.marketing_channel,'n/a') AS channel
         ,o.new_recurring AS retention_group
         ,CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,COUNT(DISTINCT s.subscription_id) AS subscriptions
         ,COUNT(DISTINCT s.customer_id) AS customers
         ,SUM(s.subscription_value) as subscription_value
         ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
    FROM master."order" o
             LEFT JOIN master.subscription s
                       ON o.order_id=s.order_id
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
   ,acquired_asv_csv AS (
    SELECT
        s.start_date::DATE AS reporting_date,
        COALESCE(s.country_name, 'n/a') AS country,
        COALESCE(o.new_recurring,'n/a') AS retention_group,
        CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
             WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
             WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
             WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
             ELSE 'n/a' END AS customer_type,
        COALESCE(o.marketing_channel, 'n/a') AS channel,
        SUM(s.subscription_value_euro) AS acquired_subscription_value,
        SUM(s.committed_sub_value) AS acquired_committed_sub_value
    FROM ods_production.subscription s
             INNER JOIN master."order" o ON s.order_id = o.order_id
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
,submitted_orders AS (
    SELECT
        o.submitted_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(o.marketing_channel,'n/a') AS channel
         ,o.new_recurring AS retention_group
         ,CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,COUNT(DISTINCT CASE
                             WHEN o.completed_orders >= 1
                                 THEN o.order_id
        END) AS submitted_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1
                        THEN o.order_id
                    END) AS paid_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1 and o.new_recurring = 'NEW'
                        THEN o.order_id
                    END) AS new_customers
    FROM master."order" o
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)

   ,ftp_subs AS (
    SELECT
        s.created_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(oc.first_touchpoint,'n/a') AS channel
         ,o.new_recurring AS retention_group
         ,CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,COUNT(DISTINCT s.subscription_id) AS first_touchpoint_subscriptions
         ,COUNT(DISTINCT
                CASE
                    WHEN o.new_recurring = 'NEW' AND o.paid_orders >= 1
                        THEN s.customer_id
                    END) AS first_touchpoint_new_customers
         ,SUM(s.subscription_value) AS first_touchpoint_subscription_value
         ,SUM(s.committed_sub_value + s.additional_committed_sub_value) AS first_touchpoint_committed_subscription_value
    FROM master."order" o
             LEFT JOIN traffic.order_conversions oc
                       ON o.order_id = oc.order_id
             LEFT JOIN master.subscription s
                       ON s.order_id = o.order_id
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
   ,ftp_orders AS (
    SELECT
        o.submitted_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(oc.first_touchpoint,'n/a') AS channel
         ,o.new_recurring AS retention_group
         ,CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,COUNT(DISTINCT
                CASE
                    WHEN o.completed_orders >= 1
                        THEN o.order_id
                    END) AS first_touchpoint_submitted_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1
                        THEN o.order_id
                    END) AS first_touchpoint_paid_orders
    FROM master."order" o
             LEFT JOIN traffic.order_conversions oc
                       ON oc.order_id=o.order_id
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
   ,atp_subs AS (
    SELECT
        s.created_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(m.marketing_channel,'n/a')  AS channel
         ,o.new_recurring AS retention_group
         ,CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,COUNT(DISTINCT s.subscription_id) AS any_touchpoint_subscriptions
         ,COUNT(DISTINCT
                CASE
                    WHEN o.new_recurring = 'NEW' AND o.paid_orders >= 1
                        THEN s.customer_id
                    END) AS any_touchpoint_new_customers
         ,SUM(s.subscription_value) as any_touchpoint_subscription_value
         ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as any_touchpoint_committed_subscription_value
    FROM master."order" o
             LEFT JOIN traffic.session_order_mapping m
                       ON o.order_id = m.order_id
             LEFT JOIN master.subscription s
                       ON o.order_id = s.order_id
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
   ,atp_orders AS (
    SELECT
        o.submitted_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(m.marketing_channel,'n/a') AS channel
         ,o.new_recurring AS retention_group
         ,CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
               WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
               ELSE 'n/a' END AS customer_type
         ,COUNT(DISTINCT CASE
                             WHEN o.completed_orders >= 1
                                 THEN o.order_id
        END) AS any_touchpoint_submitted_orders
         ,COUNT(DISTINCT CASE
                             WHEN o.paid_orders >= 1
                                 THEN o.order_id
        END) AS any_touchpoint_paid_orders
    FROM master."order" o
             LEFT JOIN traffic.session_order_mapping m
                       ON o.order_id = m.order_id
             LEFT JOIN master.customer mc
                       ON o.customer_id = mc.customer_id
             LEFT JOIN dm_risk.b2b_freelancer_mapping fre
                       ON mc.company_type_name = fre.company_type_name
    GROUP BY 1,2,3,4,5
)
SELECT DISTINCT
    dc.country
              ,dc.channel
              ,dc.retention_group
              ,dc.brand_non_brand
              ,dc.reporting_date
              ,dc.customer_type
              ,COALESCE(SUM(utd.traffic_daily_unique_users), 0) AS traffic_daily_unique_users
              ,COALESCE(SUM(utw.traffic_weekly_unique_users), 0) AS traffic_weekly_unique_users
              ,COALESCE(SUM(utm.traffic_monthly_unique_users), 0) AS traffic_monthly_unique_users
              ,COALESCE(SUM(o.submitted_orders), 0) AS submitted_orders
              ,COALESCE(SUM(o.paid_orders), 0) AS paid_orders
              ,COALESCE(SUM(ftpo.first_touchpoint_submitted_orders) ,0) AS first_touchpoint_submitted_orders
              ,COALESCE(SUM(atpo.any_touchpoint_submitted_orders) ,0) AS any_touchpoint_submitted_orders
              ,COALESCE(SUM(s.subscriptions), 0) AS subscriptions
              ,COALESCE(SUM(s.subscription_value), 0) AS subscription_value
              ,COALESCE(SUM(s.committed_subscription_value), 0) AS committed_subscription_value
              ,COALESCE(SUM(ftps.first_touchpoint_subscription_value ),0) as first_touchpoint_subscription_value
              ,COALESCE(SUM(ftps.first_touchpoint_committed_subscription_value ),0) as first_touchpoint_committed_subscription_value
              ,COALESCE(SUM(atps.any_touchpoint_subscription_value ),0) as any_touchpoint_subscription_value
              ,COALESCE(SUM(atps.any_touchpoint_committed_subscription_value ),0) as any_touchpoint_committed_subscription_value
              ,COALESCE(SUM(ftps.first_touchpoint_subscriptions), 0) AS first_touchpoint_subscriptions
              ,COALESCE(SUM(atps.any_touchpoint_subscriptions), 0) AS any_touchpoint_subscriptions
              ,COALESCE(SUM(s.customers), 0) AS customers
              ,COALESCE(SUM(o.new_customers), 0) AS new_customers
              ,COALESCE(SUM(ftps.first_touchpoint_new_customers), 0) AS first_touchpoint_new_customers
              ,COALESCE(SUM(atps.any_touchpoint_new_customers), 0) AS any_touchpoint_new_customers
              ,COALESCE(SUM(c.cost_non_cash), 0) AS cost_non_cash
              ,COALESCE(SUM(c.cost_cash), 0) AS cost_cash
              ,COALESCE(SUM(c.cost), 0) AS cost
              ,COALESCE(SUM(aac.acquired_subscription_value), 0) AS acquired_subscription_value
              ,COALESCE(SUM(aac.acquired_committed_sub_value), 0) AS acquired_committed_sub_value
FROM dimensions_combined dc
         LEFT JOIN user_traffic_daily utd
                   ON dc.reporting_date = utd.reporting_date
                       AND dc.channel = utd.channel
                       AND dc.country = utd.country
                       AND dc.retention_group = utd.retention_group
                       AND dc.customer_type = utd.customer_type
         LEFT JOIN user_traffic_weekly utw
                   ON dc.reporting_date = utw.reporting_date
                       AND dc.channel = utw.channel
                       AND dc.country = utw.country
                       AND dc.retention_group = utw.retention_group
                       AND dc.customer_type = utw.customer_type
         LEFT JOIN user_traffic_monthly utm
                   ON dc.reporting_date = utm.reporting_date
                       AND dc.channel = utm.channel
                       AND dc.country = utm.country
                       AND dc.retention_group = utm.retention_group
                       AND dc.customer_type = utm.customer_type
         LEFT JOIN submitted_orders o
                   ON dc.reporting_date = o.reporting_date
                       AND dc.channel = o.channel
                       AND dc.country = o.country
                       AND dc.retention_group = o.retention_group
                       AND dc.customer_type = o.customer_type
         LEFT JOIN subs s
                   ON  dc.reporting_date = s.reporting_date
                       AND dc.channel = s.channel
                       AND dc.country = s.country
                       AND dc.retention_group = s.retention_group
                       AND dc.customer_type = s.customer_type
         LEFT JOIN ftp_subs ftps
                   ON  dc.reporting_date = ftps.reporting_date
                       AND dc.country = ftps.country
                       AND dc.retention_group = ftps.retention_group
                       AND dc.channel = ftps.channel
                       AND dc.customer_type = ftps.customer_type
         LEFT JOIN ftp_orders ftpo
                   ON  dc.reporting_date = ftpo.reporting_date
                       AND dc.country = ftpo.country
                       AND dc.retention_group = ftpo.retention_group
                       AND dc.channel = ftpo.channel
                       AND dc.customer_type = ftpo.customer_type
         LEFT JOIN atp_subs atps
                   ON  dc.reporting_date = atps.reporting_date
                       AND dc.country = atps.country
                       AND dc.retention_group = atps.retention_group
                       AND dc.channel = atps.channel
                       AND dc.customer_type = atps.customer_type
         LEFT JOIN atp_orders atpo
                   ON  dc.reporting_date = atpo.reporting_date
                       AND dc.country = atpo.country
                       AND dc.retention_group = atpo.retention_group
                       AND dc.channel = atpo.channel
                       AND dc.customer_type = atpo.customer_type
         LEFT JOIN cost c
                   ON  dc.reporting_date = c.reporting_date
                       AND dc.channel = c.channel
                       AND dc.country = c.country
                       AND dc.retention_group = c.retention_group
                       AND dc.customer_type = c.customer_type
         LEFT JOIN acquired_asv_csv aac
                   ON  dc.reporting_date = aac.reporting_date
                       AND dc.channel = aac.channel
                       AND dc.country = aac.country
                       AND dc.retention_group = aac.retention_group
                       AND dc.customer_type = aac.customer_type
GROUP BY 1,2,3,4,5,6
;

GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;

GRANT SELECT ON marketing.marketing_cost_daily_reporting TO redash_growth;

GRANT SELECT ON marketing.marketing_cost_daily_reporting TO tableau;
