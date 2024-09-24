DROP TABLE IF EXISTS tmp_marketing_conversion_daily_reporting;
CREATE TEMP TABLE tmp_marketing_conversion_daily_reporting AS
WITH dates AS (
SELECT DISTINCT 
  datum AS reporting_date,
  LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS month_eom
FROM public.dim_dates
WHERE datum BETWEEN '2021-10-01' AND CURRENT_DATE - 1
)
,retention_group AS (
--THIS IS BAD PRACTICE TO USE A FACT TABLE AS A DIMENSION TABLE. LET'S IMPROVE BELOW QUERY LATER 
SELECT DISTINCT new_recurring AS retention_group 
FROM master."order" o
UNION
SELECT 'n/a' AS retention_group  
)
,marketing_channel_detailed_order_mapping AS (
--THIS IS BAD PRACTICE TO USE A FACT TABLE AS A DIMENSION TABLE. LET'S IMPROVE BELOW QUERY LATER 
SELECT DISTINCT 
  omc.marketing_channel 
  ,omc.marketing_source
  ,omc.marketing_medium
  ,COALESCE(CASE
     WHEN omc.marketing_channel = 'Display Branding' AND marketing_source = 'google'
      THEN 'Google Display Brand'
     WHEN omc.marketing_channel = 'Display Branding' AND marketing_source = 'google_deman_gen'
      THEN 'Google Demand Gen Brand'
     WHEN omc.marketing_channel = 'Display Branding' AND marketing_source = 'google_discovery'
      THEN 'Google Discovery Brand'
     WHEN omc.marketing_channel = 'Display Branding' AND marketing_source = 'outbrain'
      THEN 'Outbrain Brand'
     WHEN omc.marketing_channel = 'Display Branding' AND LOWER(marketing_source) = 'youtube'
      THEN 'Google Youtube Brand'
     WHEN omc.marketing_channel = 'Display Branding' AND LOWER(marketing_source) = 'google_perf_max'
      THEN 'Google Performance Max Brand'
     WHEN omc.marketing_channel = 'Display Branding'
      THEN 'Display Other Brand'
    WHEN omc.marketing_channel = 'Display Performance' AND marketing_source = 'google_deman_gen'
      THEN 'Google Demand Gen Performance'
    WHEN omc.marketing_channel = 'Display Performance' AND marketing_source = 'google'
      THEN 'Google Display Performance'
     WHEN omc.marketing_channel = 'Display Performance' AND marketing_source = 'google_discovery'
      THEN 'Google Discovery Performance'
     WHEN omc.marketing_channel = 'Display Performance' AND marketing_source = 'outbrain'
      THEN 'Outbrain Performance'
     WHEN omc.marketing_channel = 'Display Performance' AND LOWER(marketing_source) = 'youtube'
      THEN 'Google Youtube Performance'
     WHEN omc.marketing_channel = 'Display Performance' AND LOWER(marketing_source) = 'google_perf_max'
      THEN 'Google Performance Max Performance'
     WHEN omc.marketing_channel = 'Display Performance'
      THEN 'Display Other Performance'
     WHEN omc.marketing_channel = 'Paid Search Brand' AND marketing_source = 'bing'
      THEN 'Bing Search Brand'
     WHEN omc.marketing_channel = 'Paid Search Brand' AND marketing_source = 'google'
      THEN 'Google Search Brand'
     WHEN omc.marketing_channel = 'Paid Search Brand'
      THEN 'Paid Search Brand Other'
     WHEN omc.marketing_channel = 'Paid Search Non Brand' AND marketing_source = 'bing'
      THEN 'Bing Search Non Brand'
     WHEN omc.marketing_channel = 'Paid Search Non Brand' AND marketing_source = 'google'
      THEN 'Google Search Non Brand'
     WHEN omc.marketing_channel = 'Paid Search Non Brand'
      THEN 'Paid Search Non Brand Other'
     WHEN omc.marketing_channel = 'Paid Social Branding' AND LOWER(marketing_source) = 'facebook'
      THEN 'Facebook Brand'
     WHEN omc.marketing_channel = 'Paid Social Branding' AND marketing_source = 'linkedin'
      THEN 'Linkedin Brand'
     WHEN omc.marketing_channel = 'Paid Social Branding' AND marketing_source = 'snapchat'
      THEN 'Snapchat Brand'
     WHEN omc.marketing_channel = 'Paid Social Branding' AND marketing_source = 'tiktok'
      THEN 'TikTok Brand'
     WHEN omc.marketing_channel = 'Paid Social Branding'
      THEN 'Paid Social Other Brand'
    WHEN omc.marketing_channel = 'Paid Social Performance' AND LOWER(marketing_source) = 'facebook'
      THEN 'Facebook Performance'
     WHEN omc.marketing_channel = 'Paid Social Performance' AND marketing_source = 'linkedin'
      THEN 'Linkedin Performance'
     WHEN omc.marketing_channel = 'Paid Social Performance' AND marketing_source = 'snapchat'
      THEN 'Snapchat Performance'
     WHEN omc.marketing_channel = 'Paid Social Performance' AND marketing_source = 'tiktok'
      THEN 'TikTok Performance'
     WHEN omc.marketing_channel = 'Paid Social Performance'
      THEN 'Paid Social Other Performance'
     WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'google'
      THEN 'Google Shopping'
     WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'bing'
      THEN 'Bing Shopping'
     WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'ebay_kleinanzeigen'
      THEN 'eBay Kleinanzeigen Shopping'
     WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'marktplaats'
      THEN 'Marktplaats Shopping'
   ELSE omc.marketing_channel
  END,'n/a') AS marketing_channel_detailed
FROM master.ORDER o
  LEFT JOIN ods_production.order_marketing_channel omc  
    ON omc.order_id = o.order_id  
WHERE o.created_date >= '2021-10-01'
)
,marketing_channel AS (
SELECT DISTINCT 
  o.marketing_channel
  ,o.marketing_channel_detailed
FROM marketing_channel_detailed_order_mapping o
  LEFT JOIN marketing.marketing_cost_channel_mapping ma
    ON o.marketing_channel_detailed = COALESCE(CASE 
                                                WHEN channel_detailed LIKE '%Performance%'
                                                  THEN 'Google Performance Max'
                                                ELSE channel_detailed
                                               END, ma.channel)  
UNION 
SELECT DISTINCT 
  channel_grouping AS marketing_channel
  ,COALESCE(CASE 
              WHEN channel_detailed LIKE '%Performance%'
                THEN 'Google Performance Max'
              ELSE channel_detailed
            END, channel, 'n/a') AS marketing_channel_detailed
FROM marketing.marketing_cost_channel_mapping mccm 
UNION 
SELECT DISTINCT 
	COALESCE(omc.marketing_channel,'n/a') AS marketing_channel,
	COALESCE(CASE 
	    WHEN omc.marketing_source = 'google' AND omc.marketing_medium = 'cpc' AND omc.referer_url ILIKE ('%youtube%')
	     THEN 'Google Youtube' 
	    ELSE orm.marketing_channel_detailed  
	   END, 'n/a') AS marketing_channel_detailed
FROM master."order" o
  LEFT JOIN ods_production.order_marketing_channel omc
    ON o.order_id = omc.order_id 
  LEFT JOIN marketing_channel_detailed_order_mapping orm
    ON omc.marketing_channel = orm.marketing_channel
    AND omc.marketing_source = orm.marketing_source 
    AND omc.marketing_medium = orm.marketing_medium                                              
)
,country AS (
SELECT DISTINCT country 
FROM marketing.marketing_cost_channel_mapping mccm 
)
,dimensions_combined AS (
SELECT DISTINCT
	d.reporting_date,
    d.month_eom,
	r.retention_group,
	m.marketing_channel,
  	m.marketing_channel_detailed,
	c.country,
	CASE WHEN m.marketing_channel IN ('Paid Search Brand','Paid Search Non Brand')
        THEN 'Performance'
        ELSE COALESCE(ma.brand_non_brand, 'n/a') end AS brand_non_brand ,
  	CASE WHEN m.marketing_channel IN ('Paid Search Brand','Paid Search Non Brand')
        THEN 'Cash'
        ELSE COALESCE(ma.cash_non_cash, 'n/a') END AS cash_non_cash
FROM 
dates d
  CROSS JOIN retention_group r
  CROSS JOIN 
  marketing_channel m
  CROSS JOIN country c
	LEFT JOIN marketing.marketing_cost_channel_mapping ma
		ON COALESCE(ma.channel_grouping, 'n/a') = m.marketing_channel
		AND m.marketing_channel_detailed = COALESCE(CASE WHEN ma.channel_detailed LIKE '%Performance%'
                								THEN 'Google Performance Max' ELSE ma.channel_detailed END, ma.channel, 'n/a')
       WHERE CASE WHEN  m.marketing_channel = 'Influencers' AND ma.cash_non_cash IN ('Non Cash' , 'Non-Cash') 
			THEN 'NOK' ELSE 'OK' END = 'OK'							
)
/*IN ORDER TO HAVE WEEKLY UNIQUE USERS, WE ARE ALMOST RUNNIG THE IDENTICAL QUERY 
 LIKE IN user_traffic_daily CTE WITH ONLY ONE COLUMN DIFFERENCE. 
 LET'S CHECK IF WE CAN IMPROVE THIS LATER*/
,user_traffic_daily AS (
SELECT 
 session_start::DATE AS reporting_date
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
  ,COALESCE(CASE 
    WHEN s.marketing_source = 'google' AND s.marketing_medium = 'cpc' AND s.referer_url ILIKE ('%youtube%')
     THEN case when s.marketing_channel = 'Display Branding' then 'Google Youtube Brand' 
     else 'Google Youtube Performance' end  
    ELSE o.marketing_channel_detailed  
   END, s.marketing_channel, 'n/a') AS marketing_channel_detailed
  , CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
  ,COUNT(s.session_id) AS traffic_daily_sessions
  ,COUNT(DISTINCT s.session_id) AS traffic_daily_unique_sessions
  ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_daily_unique_users
FROM traffic.sessions s 
  LEFT JOIN ods_production.store st 
    ON st.id = s.store_id
  LEFT JOIN marketing_channel_detailed_order_mapping o
    ON s.marketing_channel = o.marketing_channel
    AND s.marketing_source = o.marketing_source 
    AND s.marketing_medium = o.marketing_medium 
  LEFT JOIN traffic.snowplow_user_mapping cu
    ON s.anonymous_id = cu.anonymous_id 
    AND s.session_id = cu.session_id
GROUP BY 1,2,3,4,5
)
,user_traffic_weekly AS (
SELECT 
 DATE_TRUNC('week', session_start)::DATE AS reporting_date
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
  ,COALESCE(CASE 
    WHEN s.marketing_source = 'google' AND s.marketing_medium = 'cpc' AND s.referer_url ILIKE ('%youtube%')
     THEN case when s.marketing_channel = 'Display Branding' then 'Google Youtube Brand' 
     else 'Google Youtube Performance' end  
    ELSE o.marketing_channel_detailed  
   END, s.marketing_channel, 'n/a') AS marketing_channel_detailed
  , CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
  ,COUNT(s.session_id) AS traffic_weekly_sessions
  ,COUNT(DISTINCT s.session_id) AS traffic_weekly_unique_sessions
  ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_weekly_unique_users
FROM traffic.sessions s 
  LEFT JOIN ods_production.store st 
    ON st.id = s.store_id
  LEFT JOIN marketing_channel_detailed_order_mapping o
    ON s.marketing_channel = o.marketing_channel
    AND s.marketing_source = o.marketing_source 
    AND s.marketing_medium = o.marketing_medium 
  LEFT JOIN traffic.snowplow_user_mapping cu
    ON s.anonymous_id = cu.anonymous_id 
    AND s.session_id = cu.session_id
GROUP BY 1,2,3,4,5
)
/*IN ORDER TO HAVE MONTHLY UNIQUE USERS, WE ARE ALMOST RUNNIG THE IDENTICAL QUERY 
 LIKE IN user_traffic_weekly CTE WITH ONLY ONE COLUMN DIFFERENCE. 
 LET'S CHECK IF WE CAN IMPROVE THIS LATER*/
,user_traffic_monthly AS (
SELECT 
 DATE_TRUNC('month', session_start)::DATE AS reporting_date
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
  ,COALESCE(s.marketing_channel,'n/a') AS marketing_channel
  ,COALESCE(CASE 
    WHEN s.marketing_source = 'google' AND s.marketing_medium = 'cpc' AND s.referer_url ILIKE ('%youtube%')
     THEN case when s.marketing_channel = 'Display Branding' then 'Google Youtube Brand' 
     else 'Google Youtube Performance' end  
    ELSE o.marketing_channel_detailed  
   END, s.marketing_channel, 'n/a') AS marketing_channel_detailed
  , CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
  ,COUNT(s.session_id) AS traffic_monthly_sessions
  ,COUNT(DISTINCT s.session_id) AS traffic_monthly_unique_sessions
  ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_monthly_unique_users
FROM traffic.sessions s 
  LEFT JOIN ods_production.store st 
    ON st.id = s.store_id
  LEFT JOIN marketing_channel_detailed_order_mapping o
    ON s.marketing_channel = o.marketing_channel
    AND s.marketing_source = o.marketing_source 
    AND s.marketing_medium = o.marketing_medium 
  LEFT JOIN traffic.snowplow_user_mapping cu
    ON s.anonymous_id = cu.anonymous_id
    AND s.session_id = cu.session_id
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
    ELSE COALESCE(channel_grouping,'n/a')
  END AS marketing_channel
-- ,COALESCE(channel, 'n/a') AS marketing_channel_detailed
 ,COALESCE(CASE WHEN marketing_channel = 'Paid Search Non Brand' then channel || ' ' || 'Non Brand'
                WHEN marketing_channel = 'Paid Search Brand' then channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Display Branding'  then channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Display Performance' then channel || ' ' || 'Performance'
                WHEN marketing_channel = 'Paid Social Branding' then channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Paid Social Performance' then channel || ' ' || 'Performance'
    ELSE channel END, 'n/a') AS marketing_channel_detailed
 ,SUM(total_spent_eur) AS cost
 ,SUM(CASE WHEN a.cash_non_cash = 'Non-Cash' THEN total_spent_eur END) AS cost_non_cash
 ,SUM(CASE WHEN a.cash_non_cash = 'Cash' THEN total_spent_eur END) AS cost_cash
FROM marketing.marketing_cost_daily_combined a
LEFT JOIN marketing.campaigns_brand_non_brand b on a.campaign_name=b.campaign_name
GROUP BY 1,2,3,4
)
,customer_min_created_date AS (
 SELECT order_id,
        customer_id,
        MIN(created_date::DATE) AS reporting_date
 FROM master.subscription
 GROUP BY 1,2
)
,subs AS (
SELECT 
 s.created_date::DATE AS reporting_date 
 ,COALESCE(o.store_country,'n/a') AS country  
 ,COALESCE(omc.marketing_channel,'n/a') AS marketing_channel
 ,COALESCE(CASE 
    WHEN omc.marketing_source = 'google' AND omc.marketing_medium = 'cpc' AND omc.referer_url ILIKE ('%youtube%')
    THEN case when omc.marketing_channel = 'Display Branding' then 'Google Youtube Brand' 
     else 'Google Youtube Performance' end
    ELSE orm.marketing_channel_detailed  
   END, 'n/a') AS marketing_channel_detailed
 ,o.new_recurring AS retention_group
 ,SUM(s.minimum_term_months) AS minimum_term_months
 ,COUNT(DISTINCT s.subscription_id) AS subscriptions
 ,COUNT(DISTINCT s.customer_id) AS customers
 ,COUNT(DISTINCT CASE
          WHEN o.new_recurring = 'NEW' AND o.paid_orders >= 1
              THEN cm.customer_id
          END) AS new_customers
 ,SUM(s.subscription_value) as subscription_value
 ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
FROM master."order" o
  LEFT JOIN master.subscription s 
    ON o.order_id=s.order_id
  LEFT JOIN customer_min_created_date cm
            ON s.customer_id=cm.customer_id 
            AND s.created_date::DATE=cm.reporting_date
            AND s.order_id=cm.order_id
  LEFT JOIN ods_production.order_marketing_channel omc
    ON o.order_id = omc.order_id
  LEFT JOIN marketing_channel_detailed_order_mapping orm
    ON omc.marketing_channel = orm.marketing_channel
    AND omc.marketing_source = orm.marketing_source 
    AND omc.marketing_medium = orm.marketing_medium 
GROUP BY 1,2,3,4,5
)
,cart_orders AS (
SELECT 
  o.created_date::DATE AS reporting_date
 ,COALESCE(o.store_country,'n/a') AS country  
 ,COALESCE(omc.marketing_channel,'n/a') AS marketing_channel
 ,COALESCE(CASE 
    WHEN omc.marketing_source = 'google' AND omc.marketing_medium = 'cpc' AND omc.referer_url ILIKE ('%youtube%')
    THEN case when omc.marketing_channel = 'Display Branding' then 'Google Youtube Brand' 
     else 'Google Youtube Performance' end
    ELSE orm.marketing_channel_detailed  
   END, 'n/a') AS marketing_channel_detailed
 ,o.new_recurring AS retention_group
 ,COUNT(DISTINCT CASE 
   WHEN o.cart_orders >= 1 
   THEN o.order_id 
  END) AS carts
FROM master."order" o
  LEFT JOIN ods_production.order_marketing_channel omc
    ON o.order_id = omc.order_id 
  LEFT JOIN marketing_channel_detailed_order_mapping orm
    ON omc.marketing_channel = orm.marketing_channel
    AND omc.marketing_source = orm.marketing_source 
    AND omc.marketing_medium = orm.marketing_medium 
GROUP BY 1,2,3,4,5
)
,submitted_orders AS (
SELECT 
  o.submitted_date::DATE AS reporting_date
 ,COALESCE(o.store_country,'n/a') AS country  
 ,COALESCE(omc.marketing_channel,'n/a') AS marketing_channel
 ,COALESCE(CASE 
    WHEN omc.marketing_source = 'google' AND omc.marketing_medium = 'cpc' AND omc.referer_url ILIKE ('%youtube%')
    THEN case when omc.marketing_channel = 'Display Branding' then 'Google Youtube Brand' 
     else 'Google Youtube Performance' end
    ELSE orm.marketing_channel_detailed  
   END, 'n/a') AS marketing_channel_detailed
 ,o.new_recurring AS retention_group
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
FROM master."order" o
  LEFT JOIN ods_production.order_marketing_channel omc
    ON o.order_id = omc.order_id 
  LEFT JOIN marketing_channel_detailed_order_mapping orm
    ON omc.marketing_channel = orm.marketing_channel
    AND omc.marketing_source = orm.marketing_source 
    AND omc.marketing_medium = orm.marketing_medium 
GROUP BY 1,2,3,4,5
)
, dates_active_subs AS (
	SELECT DISTINCT 
		 datum AS fact_date
	FROM public.dim_dates
	WHERE datum <= current_date
)
, s AS (
	SELECT 
		s.subscription_id,
		s.store_label, 
		s.store_commercial, 
		orm.marketing_channel,
		orm.marketing_source,
		o.new_recurring,
		s.customer_id, 
		c2.company_type_name, 
		s.subscription_value_eur AS subscription_value_euro,
		s.end_date, 
		s.fact_day, 
		s.cancellation_date, 
		c.cancellation_reason_churn, 
		cf.last_valid_payment_category, 
		cf.default_date+31 AS default_date,
		s.country_name AS country,
		s.subscription_value_lc AS actual_subscription_value,
		COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
 	FROM ods_production.subscription_phase_mapping s 
	LEFT JOIN ods_production.subscription_cancellation_reason c 
		ON s.subscription_id=c.subscription_id
	LEFT JOIN ods_production.subscription_cashflow cf 
		ON s.subscription_id=cf.subscription_id
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN master.ORDER o
		ON o.order_id = s.order_id
	LEFT JOIN (SELECT DISTINCT 
				order_id, marketing_channel, marketing_source, marketing_medium, referer_url
			FROM ods_production.order_marketing_channel) omc
		ON o.order_id = omc.order_id 
	LEFT JOIN marketing_channel_detailed_order_mapping orm
		ON omc.marketing_channel = orm.marketing_channel
		AND omc.marketing_source = orm.marketing_source 
		AND omc.marketing_medium = orm.marketing_medium 
)
, active_subs_value AS (
	SELECT 
		fact_date AS reporting_date,
		COALESCE(s.country, 'n/a') AS country,
		COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(s.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
		COALESCE(s.new_recurring, 'n/a') AS retention_group,
		COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
		COUNT(DISTINCT s.customer_id) AS active_customers,
		SUM(s.subscription_value_euro) as active_subscription_value,
		SUM(s.actual_subscription_value) as actual_subscription_value
	FROM dates_active_subs d
	LEFT JOIN s 
		ON d.fact_date::date >= s.fact_Day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	WHERE country IS NOT NULL
		AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3,4,5	
)
SELECT DISTINCT
  dc.reporting_date
 ,dc.month_eom
 ,dc.country
 ,dc.marketing_channel
 ,dc.marketing_channel_detailed
 ,dc.retention_group
 ,dc.brand_non_brand
 ,dc.cash_non_cash 
 ,COALESCE(SUM(s.minimum_term_months), 0) AS minimum_term_months
 ,COALESCE(ROUND(SUM(utd.traffic_daily_sessions)), 0) AS traffic_daily_sessions
 ,COALESCE(ROUND(SUM(utd.traffic_daily_unique_sessions)), 0) AS traffic_daily_unique_sessions  
 ,COALESCE(ROUND(SUM(utw.traffic_weekly_sessions)), 0) AS traffic_weekly_sessions
 ,COALESCE(ROUND(SUM(utw.traffic_weekly_unique_sessions)), 0) AS traffic_weekly_unique_sessions
 ,COALESCE(ROUND(SUM(utm.traffic_monthly_sessions)), 0) AS traffic_monthly_sessions
 ,COALESCE(ROUND(SUM(utm.traffic_monthly_unique_sessions)), 0) AS traffic_monthly_unique_sessions
 ,COALESCE(ROUND(SUM(utd.traffic_daily_unique_users)), 0) AS traffic_daily_unique_users
 ,COALESCE(ROUND(SUM(utw.traffic_weekly_unique_users)), 0) AS traffic_weekly_unique_users
 ,COALESCE(ROUND(SUM(utm.traffic_monthly_unique_users)), 0) AS traffic_monthly_unique_users 
 ,COALESCE(SUM(co.carts), 0) AS cart_orders
 ,COALESCE(SUM(so.submitted_orders), 0) AS submitted_orders
 ,COALESCE(SUM(so.approved_orders), 0) AS approved_orders
 ,COALESCE(SUM(so.drop_off_orders), 0) AS drop_off_orders
 ,COALESCE(SUM(so.paid_orders), 0) AS paid_orders
 ,COALESCE(SUM(so.failed_first_payment_orders), 0) AS failed_first_payment_orders
 ,COALESCE(SUM(so.cancelled_orders), 0) AS cancelled_orders
-- ,COALESCE(MEDIAN(o.time_to_submit_order), 0) AS time_to_submit_order
 ,COALESCE(SUM(s.subscriptions), 0) AS subscriptions
 ,COALESCE(SUM(s.subscription_value), 0) AS subscription_value
 ,COALESCE(SUM(s.committed_subscription_value), 0) AS committed_subscription_value
 ,COALESCE(SUM(s.customers), 0) AS customers
 ,COALESCE(SUM(s.new_customers), 0) AS new_customers
 ,COALESCE(SUM(CASE 
   WHEN dc.retention_group = 'NEW'
     THEN c.cost * 0.8
   WHEN dc.retention_group = 'n/a'
     THEN 0
   ELSE c.cost * 0.2
   END), 0) AS cost
  ,COALESCE(SUM(CASE 
   WHEN dc.retention_group = 'NEW'
     THEN c.cost_cash * 0.8
   WHEN dc.retention_group = 'n/a'
     THEN 0
   ELSE c.cost_cash * 0.2
   END), 0) AS cost_cash
  ,COALESCE(SUM(CASE 
   WHEN dc.retention_group = 'NEW'
     THEN c.cost_non_cash * 0.8
   WHEN dc.retention_group = 'n/a'
     THEN 0
   ELSE c.cost_non_cash * 0.2
   END), 0) AS cost_non_cash
  ,COALESCE(SUM(asv.active_subscription_value),0) AS active_subscription_value
  ,COALESCE(SUM(asv.active_subscriptions),0) AS active_subscriptions
  ,COALESCE(SUM(asv.active_customers),0) AS active_customers
FROM dimensions_combined dc
  LEFT JOIN user_traffic_daily utd
    ON dc.reporting_date = utd.reporting_date
    AND dc.marketing_channel = utd.marketing_channel
    AND dc.marketing_channel_detailed = utd.marketing_channel_detailed 
    AND dc.country = utd.country
    AND dc.retention_group = utd.retention_group
  LEFT JOIN user_traffic_weekly utw
    ON dc.reporting_date = utw.reporting_date
    AND dc.marketing_channel = utw.marketing_channel
    AND dc.marketing_channel_detailed = utw.marketing_channel_detailed 
    AND dc.country = utw.country
    AND dc.retention_group = utw.retention_group
   LEFT JOIN user_traffic_monthly utm
    ON dc.reporting_date = utm.reporting_date
    AND dc.marketing_channel = utm.marketing_channel 
    AND dc.marketing_channel_detailed = utm.marketing_channel_detailed
    AND dc.country = utm.country
    AND dc.retention_group = utm.retention_group
  LEFT JOIN cart_orders co
    ON dc.reporting_date = co.reporting_date
    AND dc.marketing_channel = co.marketing_channel
    AND dc.marketing_channel_detailed = co.marketing_channel_detailed 
    AND dc.country = co.country
    AND dc.retention_group = co.retention_group
  LEFT JOIN submitted_orders so
    ON dc.reporting_date = so.reporting_date
    AND dc.marketing_channel = so.marketing_channel
    AND dc.marketing_channel_detailed = so.marketing_channel_detailed 
    AND dc.country = so.country
    AND dc.retention_group = so.retention_group 
  LEFT JOIN subs s
    ON  dc.reporting_date = s.reporting_date
    AND dc.marketing_channel = s.marketing_channel
    AND dc.marketing_channel_detailed = s.marketing_channel_detailed 
    AND dc.country = s.country 
    AND dc.retention_group = s.retention_group
  LEFT JOIN cost c
    ON  dc.reporting_date = c.reporting_date
    AND dc.marketing_channel = c.marketing_channel
    AND dc.marketing_channel_detailed = c.marketing_channel_detailed 
    AND dc.country = c.country 
  LEFT JOIN active_subs_value asv
  	ON dc.reporting_date = asv.reporting_date
    AND dc.marketing_channel = asv.marketing_channel
    AND dc.marketing_channel_detailed = asv.marketing_channel_detailed 
    AND dc.country = asv.country
    AND dc.retention_group = asv.retention_group
GROUP BY 1,2,3,4,5,6,7,8

;

DROP TABLE IF EXISTS dm_marketing.marketing_conversion_daily_reporting;
CREATE TABLE dm_marketing.marketing_conversion_daily_reporting AS
SELECT * 
FROM tmp_marketing_conversion_daily_reporting;

GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;

GRANT SELECT ON dm_marketing.marketing_conversion_daily_reporting TO tableau;
