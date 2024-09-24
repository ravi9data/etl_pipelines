 CREATE OR REPLACE VIEW dm_marketing.v_topline_marketing_metrics AS 
   WITH retention_group AS (
    SELECT DISTINCT
        new_recurring
    FROM master.ORDER
)
   , cost AS (
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
         ,r.new_recurring
         ,CASE WHEN r.new_recurring = 'NEW' THEN SUM(co.total_spent_eur) * 0.8
               WHEN r.new_recurring = 'RECURRING' THEN SUM(co.total_spent_eur) * 0.2
        END AS cost
         ,CASE WHEN r.new_recurring = 'NEW' THEN SUM(CASE WHEN co.cash_non_cash = 'Cash' THEN co.total_spent_eur END) * 0.8
               WHEN r.new_recurring = 'RECURRING' THEN SUM(CASE WHEN co.cash_non_cash = 'Cash' THEN co.total_spent_eur END) * 0.2
        END AS cost_cash
         ,CASE WHEN r.new_recurring = 'NEW' THEN SUM(CASE WHEN co.cash_non_cash = 'Non-Cash' THEN co.total_spent_eur END) * 0.8
               WHEN r.new_recurring = 'RECURRING' THEN SUM(CASE WHEN co.cash_non_cash = 'Non-Cash' THEN co.total_spent_eur END) * 0.2
        END AS cost_non_cash
    FROM marketing.marketing_cost_daily_combined co
             LEFT JOIN marketing.campaigns_brand_non_brand b
                       ON co.campaign_name=b.campaign_name
             CROSS JOIN retention_group r
    WHERE DATE_TRUNC('year',co.reporting_date) >= DATE_TRUNC('year',DATEADD('year',-2,current_date))
    GROUP BY 1,2,3,4
)
   , submitted_orders AS (
    SELECT
        COALESCE(o.store_country, 'n/a') AS country,
        COALESCE(o.marketing_channel, 'n/a') AS marketing_channel,
        o.submitted_date::date AS reporting_date,
        COALESCE(o.new_recurring, 'n/a') AS new_recurring,
        COUNT(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END) AS submitted_orders,
        COUNT(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END) AS paid_orders,
        COUNT(DISTINCT CASE WHEN o.paid_orders >= 1 and o.new_recurring = 'NEW' THEN o.customer_id END) AS new_customers
    FROM master.ORDER o
    WHERE DATE_TRUNC('year',o.submitted_date::date) >= DATE_TRUNC('year',DATEADD('year',-2,current_date))
    GROUP BY 1,2,3,4
)

   ,subs AS (
    SELECT
        s.created_date::DATE AS reporting_date
         ,COALESCE(o.store_country,'n/a') AS country
         ,COALESCE(o.marketing_channel,'n/a') AS marketing_channel
         ,COALESCE(o.new_recurring, 'n/a') AS new_recurring
         ,COUNT(DISTINCT s.subscription_id) AS subscriptions
         ,COUNT(DISTINCT s.customer_id) AS customers
         ,SUM(s.subscription_value_euro) as acquired_subscription_value
         ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
    FROM master.order o
             LEFT JOIN master.subscription s
                       ON s.order_id = o.order_id
    WHERE DATE_TRUNC('year',s.created_date::DATE) >= DATE_TRUNC('year',DATEADD('year',-2,current_date))
    GROUP BY 1,2,3,4
)
   , traffic AS (
    SELECT
        s.session_start::date AS reporting_date,
        CASE
            WHEN s.first_page_url ILIKE '/de-%' THEN 'Germany'
            WHEN s.first_page_url ILIKE '/us-%' THEN 'United States'
            WHEN s.first_page_url ILIKE '/es-%' THEN 'Spain'
            WHEN s.first_page_url ILIKE '/nl-%' THEN 'Netherlands'
            WHEN s.first_page_url ILIKE '/at-%' THEN 'Austria'
            WHEN s.first_page_url ILIKE '/business_es-%' THEN 'Spain'
            WHEN s.first_page_url ILIKE '/business-%' THEN 'Germany'
            WHEN s.first_page_url ILIKE '/business_at-%' THEN 'Austria'
            WHEN s.first_page_url ILIKE '/business_nl-%' THEN 'Netherlands'
            WHEN s.first_page_url ILIKE '/business_us-%' THEN 'United States'
            WHEN s.store_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
            WHEN s.store_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
            WHEN s.store_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
            WHEN s.store_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
            WHEN s.store_name IS NULL AND s.geo_country = 'US' THEN 'United States'
            WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
            ELSE 'Germany' END
                              AS country,
        s.marketing_channel,
        CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS new_recurring,
        COUNT(distinct s.session_id) AS traffic_daily_unique_sessions,
        COUNT(DISTINCT COALESCE(cu.anonymous_id_new,s.anonymous_id)) AS traffic_daily_unique_users
    FROM traffic.sessions s
             LEFT JOIN traffic.snowplow_user_mapping cu
                       ON s.anonymous_id = cu.anonymous_id
                           AND s.session_id = cu.session_id
    WHERE DATE_TRUNC('year',s.session_start) >= DATE_TRUNC('year',DATEADD('year',-2,current_date))
    GROUP BY 1,2,3,4
)
   ,is_paid AS (
    SELECT DISTINCT marketing_channel, is_paid
    FROM traffic.sessions
    WHERE session_start >= '2023-05-01'
)
   , dimensions AS (
    SELECT DISTINCT
        reporting_date,
        country,
        marketing_channel,
        new_recurring
    FROM cost
    UNION
    SELECT DISTINCT
        reporting_date,
        country,
        marketing_channel,
        new_recurring
    FROM subs
    UNION
    SELECT DISTINCT
        reporting_date,
        country,
        marketing_channel,
        new_recurring
    FROM submitted_orders
    UNION
    SELECT DISTINCT
        reporting_date,
        country,
        marketing_channel,
        new_recurring
    FROM traffic
)

SELECT
    d.reporting_date,
    d.country,
    d.marketing_channel,
    ip.is_paid,
    d.new_recurring,
    s.subscriptions,
    s.customers,
    s.acquired_subscription_value,
    s.committed_subscription_value,
    o.new_customers,
    o.paid_orders,
    o.submitted_orders,
    c.cost,
    c.cost_cash,
    c.cost_non_cash,
    t.traffic_daily_unique_sessions,
    t.traffic_daily_unique_users
FROM dimensions d
  LEFT JOIN subs s
    ON d.reporting_date = s.reporting_date
        AND d.country = s.country
        AND d.marketing_channel = s.marketing_channel
        AND d.new_recurring = s.new_recurring
  LEFT JOIN submitted_orders o
    ON d.reporting_date = o.reporting_date
        AND d.country = o.country
        AND d.marketing_channel = o.marketing_channel
        AND d.new_recurring = o.new_recurring
  LEFT JOIN cost c
    ON d.reporting_date = c.reporting_date
        AND d.country = c.country
        AND d.marketing_channel = c.marketing_channel
        AND d.new_recurring = c.new_recurring
  LEFT JOIN traffic t
    ON d.reporting_date = t.reporting_date
        AND d.country = t.country
        AND d.marketing_channel = t.marketing_channel
        AND d.new_recurring = t.new_recurring
  LEFT JOIN is_paid ip
            ON d.marketing_channel = ip.marketing_channel
  WHERE d.reporting_date != CURRENT_DATE
WITH NO SCHEMA BINDING;
