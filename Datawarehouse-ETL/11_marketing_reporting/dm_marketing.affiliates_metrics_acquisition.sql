
DROP TABLE IF EXISTS tmp_affiliates_metrics_acquisition;
CREATE TEMP TABLE tmp_affiliates_metrics_acquisition AS 
WITH dates AS (
	SELECT DISTINCT 
	  datum AS reporting_date 
	FROM public.dim_dates
	WHERE DATE_TRUNC('year',datum)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND datum <= current_date - 1 
)
,retention_group AS (
	SELECT DISTINCT new_recurring AS retention_group 
	FROM master."order" o 
)
, marketing_channel AS (
	SELECT 
		'Affiliates' AS marketing_channel
	UNION
	SELECT 
		'Others' AS marketing_channel
)
, is_affiliate_voucher AS (
	SELECT 
		1 AS is_affiliate_voucher
	UNION
	SELECT 
		0 AS is_affiliate_voucher
)
, customer_type AS (
	SELECT 
		'normal_customer' AS customer_type
	UNION
	SELECT
		'business_customer' AS customer_type
	UNION
	SELECT 
		'n/a' AS customer_type
)
, marketing_sources AS (
	SELECT
		marketing_source,
		count(*) AS sessions
	FROM traffic.sessions 
	WHERE marketing_channel = 'Affiliates'
		AND marketing_source IS NOT NULL
	GROUP BY 1
	HAVING sessions > 10000 --ONLY revelants mkt SOURCE
)
, cost AS (
	SELECT
	  "date"::date AS reporting_date
	 ,COALESCE(country,'n/a') AS country
	 ,'NEW' AS retention_group
	 , channel AS marketing_channel
	 , 0 AS is_affiliate_voucher
	 , lower(campaign_name) AS marketing_campaign
	 , 'n/a' AS marketing_source
	 , COALESCE(CASE WHEN customer_type = 'B2C' THEN 'normal_customer'
	 				 WHEN customer_type = 'B2B' THEN 'business_customer' END,'n/a') AS customer_type
	 ,SUM(total_spent_local_currency) * 0.8 AS cost
	FROM marketing.marketing_cost_daily_base_data a
	WHERE DATE_TRUNC('year',"date")::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND "date" <= current_date - 1
		AND channel = 'Affiliates'
	GROUP BY 1,2,3,4,5,6,7,8
	--
	UNION ALL 
	--
	SELECT
	  "date"::date AS reporting_date
	 ,COALESCE(country,'n/a') AS country
	 ,'RECURRING' AS retention_group
	 , channel AS marketing_channel
	 , 0 AS is_affiliate_voucher
	 , lower(campaign_name) AS marketing_campaign
	 , 'n/a' AS marketing_source
	 , COALESCE(CASE WHEN customer_type = 'B2C' THEN 'normal_customer'
	 				 WHEN customer_type = 'B2B' THEN 'business_customer' END,'n/a') AS customer_type
	 ,SUM(total_spent_local_currency) * 0.2 AS cost
	FROM marketing.marketing_cost_daily_base_data a
	WHERE DATE_TRUNC('year',"date")::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND "date" <= current_date - 1
		AND channel = 'Affiliates'
	GROUP BY 1,2,3,4,5,6,7,8
)
, summing_costs_per_week AS (
	SELECT
	  DATE_TRUNC('week',reporting_date) AS reporting_date_week
	 ,country
	 ,marketing_campaign
	 ,marketing_channel
	 ,is_affiliate_voucher
	 ,SUM(cost) AS cost
	FROM cost
	GROUP BY 1,2,3,4,5
)
, ranking_publishers_per_week_and_country_by_cost AS (
	SELECT 
		reporting_date_week::date,
		country,
		marketing_campaign,
		marketing_channel,
	 	is_affiliate_voucher,
		ROW_NUMBER() OVER (PARTITION BY country, reporting_date_week ORDER BY cost DESC) AS row_num
	FROM summing_costs_per_week
)
, user_traffic_weekly AS (
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
	  ,lower(s.marketing_campaign) AS marketing_campaign
	  ,lower(COALESCE(ms.marketing_source, 'Others')) AS marketing_source
	  , 0 AS is_affiliate_voucher
	  , s.marketing_channel 
	  , CASE WHEN s.first_page_url ILIKE '%business%' THEN 'business_customer' ELSE 'normal_customer' END AS customer_type
	  , CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
	  , COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_weekly_unique_users
	FROM traffic.sessions s 
	  LEFT JOIN ods_production.store st 
	    ON st.id = s.store_id
	LEFT JOIN traffic.snowplow_user_mapping cu
	    ON s.anonymous_id = cu.anonymous_id 
	    AND s.session_id = cu.session_id
	LEFT JOIN marketing_sources ms 
		ON s.marketing_source = ms.marketing_source
	WHERE s.marketing_channel = 'Affiliates'
		AND DATE_TRUNC('year',session_start)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND session_start <= current_date - 1
	GROUP BY 1,2,3,4,5,6,7,8
)
, ranking_publishers_per_week_and_country_by_traffic AS (
	SELECT 
		reporting_date::date,
		country,
		marketing_campaign,
		marketing_source,
		marketing_channel,
		is_affiliate_voucher,
		ROW_NUMBER() OVER (PARTITION BY country, reporting_date ORDER BY traffic_weekly_unique_users DESC) AS row_num
	FROM user_traffic_weekly
)
, ranking_publishers_per_week_and_country AS (
	SELECT 
		reporting_date_week::date AS reporting_date,
		country,
		marketing_campaign,
		marketing_channel,
		'n/a' AS marketing_source,
		is_affiliate_voucher
	FROM ranking_publishers_per_week_and_country_by_cost
	WHERE row_num <= 20
		AND marketing_campaign <> 'n/a'
	UNION 
	SELECT 
		reporting_date::date AS reporting_date,
		country,
		marketing_campaign,
		marketing_channel,
		marketing_source,
		is_affiliate_voucher
	FROM ranking_publishers_per_week_and_country_by_traffic
	WHERE row_num <= 20
		AND marketing_campaign <> 'n/a'
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
	  ,COALESCE(lower(c.marketing_campaign), 'Others') AS marketing_campaign
	  ,lower(COALESCE(ms.marketing_source, 'Others')) AS marketing_source
	  , 0 AS is_affiliate_voucher
	  , CASE WHEN s.marketing_channel = 'Affiliates' THEN 'Affiliates' ELSE 'Others' END AS marketing_channel
	  , CASE WHEN s.first_page_url ILIKE '%business%' THEN 'business_customer' ELSE 'normal_customer' END AS customer_type
	  , CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group
	  ,COUNT(DISTINCT COALESCE(cu.anonymous_id_new, s.anonymous_id)) AS traffic_daily_unique_users
	FROM traffic.sessions s 
	  LEFT JOIN ods_production.store st 
	    ON st.id = s.store_id
	  LEFT JOIN marketing_sources ms 
		ON s.marketing_source = ms.marketing_source
	  LEFT JOIN ranking_publishers_per_week_and_country c
	  	ON lower(s.marketing_campaign) = c.marketing_campaign
	  	AND 
	  	(CASE WHEN st.country_name IS NULL AND s.geo_country='DE' THEN 'Germany'
	   		  WHEN st.country_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
	   		  WHEN st.country_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
	  		  WHEN st.country_name IS NULL AND s.geo_country = 'ES' THEN 'Spain' 
	 		  WHEN st.country_name IS NULL AND s.geo_country = 'US' THEN 'United States'   
	  		  ELSE COALESCE(st.country_name,'Germany') END) = c.country
	  	AND date_trunc('week',s.session_start) = c.reporting_date
	  	AND lower(COALESCE(ms.marketing_source, 'Others')) = c.marketing_source
	  LEFT JOIN traffic.snowplow_user_mapping cu
	    ON s.anonymous_id = cu.anonymous_id 
	    AND s.session_id = cu.session_id
	WHERE s.marketing_channel = 'Affiliates'
		AND DATE_TRUNC('year',session_start)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND session_start <= current_date - 1
	GROUP BY 1,2,3,4,5,6,7,8
)
,subs AS (
	SELECT 
	 s.start_date::DATE AS reporting_date 
	 ,COALESCE(o.store_country,'n/a') AS country  
	 ,o.new_recurring AS retention_group	 
	 ,CASE WHEN o.marketing_channel = 'Affiliates' THEN 'Affiliates' ELSE 'Others' END AS marketing_channel
	 ,COALESCE(c.marketing_campaign, 'Others') AS marketing_campaign
	 ,lower(COALESCE(ms.marketing_source, 'Others')) AS marketing_source
	 ,COALESCE(o.customer_type,'n/a') AS customer_type
	 ,CASE WHEN av.publisher IS NOT NULL THEN 1 ELSE 0 END AS is_affiliate_voucher
	 ,COUNT(DISTINCT s.subscription_id) AS subscriptions
	 ,COUNT(DISTINCT s.customer_id) AS customers
	 ,SUM(s.subscription_value) as subscription_value
	 ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
	FROM master."order" o
	  LEFT JOIN master.subscription s 
	    ON o.order_id=s.order_id
	  LEFT JOIN ods_production.order_marketing_channel omc 
	  	ON o.order_id = omc.order_id
	  LEFT JOIN marketing_sources ms 
		ON omc.marketing_source = ms.marketing_source
	  LEFT JOIN ranking_publishers_per_week_and_country c
	  	ON lower(o.marketing_campaign) = c.marketing_campaign
	  	AND o.store_country = c.country
	  	AND date_trunc('week',s.start_date) = c.reporting_date
	  	AND lower(COALESCE(ms.marketing_source, 'Others')) = c.marketing_source
	 LEFT JOIN staging.affiliates_vouchers av 
		ON o.voucher_code ILIKE av.voucher_prefix_code + '%'		
	WHERE 
		(o.marketing_channel = 'Affiliates'
			OR av.publisher IS NOT NULL)
		AND DATE_TRUNC('year',s.start_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND s.start_date <= current_date - 1
	GROUP BY 1,2,3,4,5,6,7,8
)
,orders AS (
	SELECT 
	  o.submitted_date::DATE AS reporting_date 
	 ,COALESCE(o.store_country,'n/a') AS country  
	 ,o.new_recurring AS retention_group 
	 ,CASE WHEN o.marketing_channel = 'Affiliates' THEN 'Affiliates' ELSE 'Others' END AS marketing_channel
	 ,COALESCE(c.marketing_campaign, 'Others') AS marketing_campaign
	 ,lower(COALESCE(ms.marketing_source, 'Others')) AS marketing_source
	 ,COALESCE(o.customer_type,'n/a') AS customer_type
	 ,CASE WHEN av.publisher IS NOT NULL THEN 1 ELSE 0 END AS is_affiliate_voucher
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
	   WHEN o.new_recurring = 'NEW' AND o.paid_orders >= 1 
	    THEN o.customer_id
	  END) AS new_customers
	FROM master."order" o
	  LEFT JOIN ods_production.order_marketing_channel omc 
	  	ON o.order_id = omc.order_id
	  LEFT JOIN marketing_sources ms 
		ON omc.marketing_source = ms.marketing_source
	LEFT JOIN ranking_publishers_per_week_and_country c
	  	ON lower(o.marketing_campaign) = c.marketing_campaign
	  	AND o.store_country = c.country
	  	AND date_trunc('week',o.submitted_date) = c.reporting_date
	  	AND lower(COALESCE(ms.marketing_source, 'Others')) = c.marketing_source
	LEFT JOIN staging.affiliates_vouchers av 
		ON o.voucher_code ILIKE av.voucher_prefix_code + '%'
	WHERE (o.marketing_channel = 'Affiliates'
			OR av.publisher IS NOT NULL)
		AND DATE_TRUNC('year',o.submitted_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
			AND o.submitted_date <= current_date - 1
	GROUP BY 1,2,3,4,5,6,7,8
)
, cost_with_others AS (
	SELECT 
	  co.reporting_date
	 ,co.country
	 ,co.retention_group
	 ,co.customer_type
	 ,COALESCE(c.marketing_campaign, 'Others') AS marketing_campaign
	 ,COALESCE(co.marketing_source, 'n/a') AS marketing_source
	 ,COALESCE(co.marketing_channel, 'Others') AS marketing_channel
	 ,COALESCE(c.is_affiliate_voucher, 0) AS is_affiliate_voucher	 
	 ,SUM(co.cost) AS cost
	FROM cost co
	LEFT JOIN ranking_publishers_per_week_and_country c
	  	ON lower(co.marketing_campaign) = c.marketing_campaign
	  	AND co.country = c.country
	  	AND date_trunc('week',co.reporting_date) = c.reporting_date
	  	AND co.marketing_source = c.marketing_source
	 GROUP BY 1,2,3,4,5,6,7,8
)  
, user_traffic_weekly_with_others AS (
	SELECT 
	  co.reporting_date
	 ,co.country
	 ,COALESCE(c.marketing_campaign, 'Others') AS marketing_campaign
	 ,COALESCE(c.marketing_source, 'Others') AS marketing_source
	 ,COALESCE(co.marketing_channel, 'Others') AS marketing_channel
	 ,COALESCE(co.retention_group, 'Others') AS retention_group
	 ,COALESCE(co.customer_type, 'Others') AS customer_type
	 ,COALESCE(c.is_affiliate_voucher, 0) AS is_affiliate_voucher
	 ,SUM(co.traffic_weekly_unique_users) AS traffic_weekly_unique_users
	FROM user_traffic_weekly co
	LEFT JOIN ranking_publishers_per_week_and_country c
	  	ON lower(co.marketing_campaign) = c.marketing_campaign
	  	AND co.country = c.country
	  	AND date_trunc('week',co.reporting_date) = c.reporting_date
	  	AND co.marketing_source = c.marketing_source
	 GROUP BY 1,2,3,4,5,6,7,8
)
, marketing_source_with_orders AS (
	SELECT marketing_source FROM marketing_sources
	UNION 
	SELECT 'Others' AS marketing_source
	UNION 
	SELECT 'n/a' AS marketing_source
)
,dimensions_combined AS (
	SELECT DISTINCT
		d.reporting_date,
		r.country,
		r.marketing_campaign AS mkt_campaign,
		rg.retention_group,
		mc.marketing_channel,
		ms.marketing_source,
		av.is_affiliate_voucher,
		ct.customer_type
	FROM dates d
	LEFT JOIN ranking_publishers_per_week_and_country r
		ON DATE_TRUNC('week', d.reporting_date) = r.reporting_date
	CROSS JOIN retention_group rg
	CROSS JOIN marketing_channel mc
	CROSS JOIN is_affiliate_voucher av
	CROSS JOIN customer_type ct
	CROSS JOIN marketing_source_with_orders ms
	UNION
	SELECT DISTINCT
		d.reporting_date,
		r.country,
		'Others' AS mkt_campaign,
		rg.retention_group,
		mc.marketing_channel,
		ms.marketing_source,
		av.is_affiliate_voucher,
		ct.customer_type
	FROM dates d
	LEFT JOIN ranking_publishers_per_week_and_country r
		ON DATE_TRUNC('week', d.reporting_date) = r.reporting_date
	CROSS JOIN retention_group rg	
	CROSS JOIN marketing_channel mc
	CROSS JOIN is_affiliate_voucher av
	CROSS JOIN customer_type ct
	CROSS JOIN marketing_source_with_orders ms
)
,final_table AS (
	SELECT DISTINCT
	  dc.country
	 ,dc.marketing_channel AS channel
	 ,dc.is_affiliate_voucher
	 ,dc.retention_group
	 ,dc.reporting_date
	 ,dc.mkt_campaign
	 ,dc.marketing_source
	 ,dc.customer_type
	 ,COALESCE(SUM(utd.traffic_daily_unique_users), 0) AS traffic_daily_unique_users
	 ,COALESCE(SUM(utw.traffic_weekly_unique_users), 0) AS traffic_weekly_unique_users 
	 ,COALESCE(SUM(o.submitted_orders), 0) AS submitted_orders 
	 ,COALESCE(SUM(o.paid_orders), 0) AS paid_orders 
	 ,COALESCE(SUM(s.subscriptions), 0) AS subscriptions 
	 ,COALESCE(SUM(s.committed_subscription_value), 0) AS committed_subscription_value 
	 ,COALESCE(SUM(o.new_customers), 0) AS new_customers 
	 ,COALESCE(SUM(c.cost), 0) AS cost 
	FROM dimensions_combined dc
	  LEFT JOIN user_traffic_daily utd
	    ON dc.reporting_date = utd.reporting_date
	    AND dc.country = utd.country
	    AND dc.mkt_campaign = utd.marketing_campaign
	    AND dc.marketing_channel = utd.marketing_channel
	    AND dc.is_affiliate_voucher = utd.is_affiliate_voucher
	    AND dc.retention_group = utd.retention_group
	    AND dc.customer_type = utd.customer_type
	    AND dc.marketing_source = utd.marketing_source
	  LEFT JOIN user_traffic_weekly_with_others utw
	    ON dc.reporting_date = utw.reporting_date
	    AND dc.country = utw.country
	    AND dc.mkt_campaign = utw.marketing_campaign
	    AND dc.marketing_channel = utw.marketing_channel
	    AND dc.is_affiliate_voucher = utw.is_affiliate_voucher
	    AND dc.retention_group = utw.retention_group
	    AND dc.customer_type = utw.customer_type
	    AND dc.marketing_source = utw.marketing_source
	  LEFT JOIN orders o
	    ON dc.reporting_date = o.reporting_date
	    AND dc.country = o.country
	    AND dc.retention_group = o.retention_group 
	    AND dc.mkt_campaign = o.marketing_campaign
	    AND dc.marketing_channel = o.marketing_channel
	    AND dc.is_affiliate_voucher = o.is_affiliate_voucher
	    AND dc.customer_type = o.customer_type
	    AND dc.marketing_source = o.marketing_source
	  LEFT JOIN subs s
	    ON  dc.reporting_date = s.reporting_date
	    AND dc.country = s.country 
	    AND dc.retention_group = s.retention_group
	    AND dc.mkt_campaign = s.marketing_campaign
	    AND dc.marketing_channel = s.marketing_channel
	    AND dc.is_affiliate_voucher = s.is_affiliate_voucher
	    AND dc.customer_type = s.customer_type
	    AND dc.marketing_source = s.marketing_source
	  LEFT JOIN cost_with_others c
	    ON  dc.reporting_date = c.reporting_date
	    AND dc.country = c.country 
	    AND dc.retention_group = c.retention_group
	    AND dc.mkt_campaign = c.marketing_campaign
	    AND dc.marketing_channel = c.marketing_channel
	    AND dc.is_affiliate_voucher = c.is_affiliate_voucher
	    AND dc.customer_type = c.customer_type
	    AND dc.marketing_source = c.marketing_source
	GROUP BY 1,2,3,4,5,6,7,8
	ORDER BY dc.reporting_date DESC 	
)
SELECT *
FROM final_table
WHERE traffic_daily_unique_users <> 0
OR traffic_weekly_unique_users <> 0
OR submitted_orders <> 0
OR paid_orders <> 0
OR subscriptions <> 0
OR committed_subscription_value <> 0
OR new_customers <> 0
OR cost <> 0
;



DROP TABLE IF EXISTS dm_marketing.affiliates_metrics_acquisition;
CREATE TABLE dm_marketing.affiliates_metrics_acquisition AS
SELECT * 
FROM tmp_affiliates_metrics_acquisition;

GRANT SELECT ON dm_marketing.affiliates_metrics_acquisition TO tableau;
GRANT SELECT ON dm_marketing.affiliates_metrics_acquisition TO GROUP BI;