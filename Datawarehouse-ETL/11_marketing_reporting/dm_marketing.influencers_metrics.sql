DROP TABLE IF EXISTS tmp_influencers_utm;
CREATE TEMP TABLE tmp_influencers_utm
SORTKEY(utm_medium, utm_source, utm_campaign, influencer_name, social, vendor,collaboration )
DISTKEY(influencer_name)
AS
	SELECT DISTINCT 
		lower(utm_medium) AS utm_medium,
		lower(utm_source) AS utm_source,
		lower(utm_campaign) AS utm_campaign,
		lower(max(influencer_name)) AS influencer_name,
		lower(max(social)) AS social,
		lower(max(vendor)) AS vendor,
		lower(max(collaboration)) AS collaboration
	FROM staging.influencers_fixed_costs
	WHERE utm_medium IS NOT NULL 
		OR utm_source IS NOT NULL 
		OR utm_campaign IS NOT NULL 
	GROUP BY 1,2,3;


DROP TABLE IF EXISTS tmp_orders_info_prep;
CREATE TEMP TABLE tmp_orders_info_prep
SORTKEY(order_id)
DISTKEY(order_id)
AS
WITH tmp_orders_asset_lending AS (
	SELECT DISTINCT 
		lending_order_id, 
		lower(max(influencer_name)) AS influencer_name,
		lower(max(social)) AS social ,
		lower(max(vendor)) AS vendor,
		lower(max(collaboration)) AS collaboration
	FROM staging.influencers_fixed_costs
	WHERE lending_order_id IS NOT NULL 
	GROUP BY 1
)
,  tmp_vouchers AS (
	SELECT DISTINCT 
		voucher, 
		lower(max(influencer_name)) AS influencer_name,
		lower(max(social)) AS social ,
		lower(max(vendor)) AS vendor,
		lower(max(collaboration)) AS collaboration
	FROM staging.influencers_fixed_costs 
	GROUP BY 1
)
	SELECT DISTINCT 
		o.order_id,
		o.created_date::date AS reporting_date ,
		CASE WHEN o.marketing_channel = 'Influencers' THEN 1 ELSE 0 END AS is_from_influencer_channel,
		CASE WHEN v.voucher IS NOT NULL THEN 1 ELSE 0 END AS is_from_voucher,
		CASE WHEN lo.lending_order_id IS NOT NULL THEN 1 ELSE 0 END AS is_lending_order,
		lower(COALESCE(o.marketing_channel, 'n/a')) AS marketing_channel,
		lower(COALESCE(o.marketing_campaign, 'n/a')) AS marketing_campaign,
		lower(COALESCE(m.marketing_source, 'n/a')) AS marketing_source,
		COALESCE(o.voucher_code, 'n/a') AS voucher_code,
		COALESCE(o.new_recurring, 'n/a') AS new_recurring,
		COALESCE(o.store_country, 'n/a') AS store_country,
		COALESCE(o.customer_type, 'n/a') AS customer_type,
		o.order_value,
		o.completed_orders,
		o.status,
		CASE WHEN o.status = 'PAID' THEN o.voucher_discount END AS voucher_discount,
		o.paid_orders,
		o.customer_id,
		max(CASE WHEN utm.utm_source IS NOT NULL
				OR utm.utm_campaign IS NOT NULL THEN 1 ELSE 0 END) AS is_from_utm,
		COALESCE(max(coalesce(v.influencer_name,utm.influencer_name, lo.influencer_name )),'n/a') AS influencer_name,
		COALESCE(max(coalesce(v.social,utm.social, lo.social )),'n/a') AS social,
		COALESCE(max(coalesce(v.vendor,utm.vendor, lo.vendor )),'n/a') AS vendor,
		COALESCE(max(coalesce(v.collaboration,utm.collaboration, lo.collaboration )),'n/a') AS collaboration
	FROM master.ORDER o 
	LEFT JOIN ods_production.order_marketing_channel m
		ON o.order_id = m.order_id
	LEFT JOIN tmp_influencers_utm utm
		ON CASE WHEN utm.utm_medium IS NULL THEN 'a' ELSE m.marketing_medium END = coalesce(lower(utm.utm_medium),'a')
		AND CASE WHEN utm.utm_source IS NULL THEN 'a' ELSE m.marketing_source END = coalesce(lower(utm.utm_source),'a')
		AND CASE WHEN utm.utm_campaign IS NULL THEN 'a' ELSE m.marketing_campaign END = coalesce(lower(utm.utm_campaign),'a')
	LEFT JOIN tmp_vouchers v
		ON lower(o.voucher_code) = lower(v.voucher)
	LEFT JOIN tmp_orders_asset_lending lo 
		ON o.order_id = lo.lending_order_id
	WHERE DATE_TRUNC('year',o.created_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
	HAVING ( is_from_influencer_channel = 1 
			OR is_from_voucher = 1 
			OR is_lending_order = 1 
			OR is_from_utm = 1 )
	
;


DROP TABLE IF EXISTS tmp_influencers_category;
CREATE TEMP TABLE tmp_influencers_category
SORTKEY(order_id )
DISTKEY(order_id)
AS
WITH products_orders AS ( 
	SELECT 
		o.order_id,
		s.category_name,
		s.subcategory_name, 
		s.product_sku,
		s.product_name,
		SUM(s.quantity) AS total_products,
		SUM(s.price) AS total_price
	FROM tmp_orders_info_prep o 
	INNER JOIN ods_production.order_item s 
	  ON o.order_id = s.order_id
	WHERE o.is_lending_order <> 1 --deleting lending orders	
	GROUP BY 1,2,3,4,5
)
, products_orders_category AS (
	SELECT 
		o.order_id,
		o.category_name,
		sum(o.total_products) AS total_products,
		sum(o.total_price) AS total_price
	FROM products_orders o
	GROUP BY 1,2
)
, row_order_category AS (  
	SELECT 
		o.order_id, 
		o.category_name, 
		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_price DESC, o.total_products DESC, category_name) AS rowno
	FROM products_orders_category o
)
, row_order_product AS (
 	SELECT 
 		o.order_id,
 		o.subcategory_name,
 		o.category_name,
 		o.product_sku,
 		o.product_name,
 		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_price DESC, o.total_products DESC, o.product_name) AS rowno_product
 	FROM products_orders o 
 	INNER JOIN row_order_category c
 		ON o.order_id = c.order_id 
 		AND o.category_name = c.category_name
 		AND c.rowno = 1
 )
 SELECT 
 	o.order_id,
	o.subcategory_name,
	o.category_name,
	o.product_sku,
	o.product_name
 FROM row_order_product o
 WHERE rowno_product = 1
 ;


DROP TABLE IF EXISTS tmp_user_traffic_daily;
CREATE TEMP TABLE tmp_user_traffic_daily
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country)
DISTKEY(reporting_date)
AS
	SELECT 
		s.session_start::DATE AS reporting_date,
		CASE WHEN 
					utm.utm_source IS NOT NULL
					OR utm.utm_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_from_utm,
		CASE WHEN s.marketing_medium = 'influencers' THEN 1 ELSE 0 END AS is_from_influencer_channel,
		0 AS is_from_voucher,
		0 AS is_lending_order,
		COALESCE(max(utm.influencer_name), 'n/a') AS influencer_name,
		COALESCE(max(utm.social), 'n/a') AS social,
		lower(COALESCE(s.marketing_medium, 'n/a')) AS marketing_channel,
		lower(COALESCE(s.marketing_campaign, 'n/a')) AS marketing_campaign,
		lower(COALESCE(s.marketing_source, 'n/a')) AS marketing_source,
		'n/a' AS voucher_code,
		CASE WHEN s.customer_id IS NOT NULL THEN 'RECURRING' ELSE 'NEW' END AS new_recurring,
		CASE 
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
		  END AS store_country,
		'n/a' AS customer_type,
		COUNT(DISTINCT s.anonymous_id) AS traffic_daily_unique_users,
		COUNT(DISTINCT s.session_id) AS traffic_daily_unique_sessions,
		COALESCE(max(utm.vendor), 'n/a') AS vendor,
		COALESCE(max(utm.collaboration), 'n/a') AS collaboration
	FROM traffic.sessions s 
	LEFT JOIN ods_production.store st 
		ON st.id = s.store_id
	LEFT JOIN tmp_influencers_utm utm
		ON CASE WHEN utm.utm_medium IS NULL THEN 'a' ELSE s.marketing_medium END = coalesce(lower(utm.utm_medium),'a')
		AND CASE WHEN utm.utm_source IS NULL THEN 'a' ELSE s.marketing_source END = coalesce(lower(utm.utm_source),'a')
		AND CASE WHEN utm.utm_campaign IS NULL THEN 'a' ELSE s.marketing_campaign END = coalesce(lower(utm.utm_campaign),'a')
	WHERE (is_from_utm = 1 -- filtering orders that has influencers utm
				OR is_from_influencer_channel = 1) -- filtering orders that channel IS influencer
			AND DATE_TRUNC('year',s.session_start)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
	GROUP BY 1,2,3,4,5,8,9,10,11,12,13,14	
;




DROP TABLE IF EXISTS tmp_subs;
CREATE TEMP TABLE tmp_subs
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country)
DISTKEY(reporting_date)
AS
WITH subs_prep AS (
	SELECT 
		o.reporting_date,
		o.is_from_utm,
		o.is_from_influencer_channel,
		o.is_from_voucher,
		o.is_lending_order,
		o.influencer_name,
		o.social,
		o.vendor,
		ss.category_name,
		o.collaboration,
		o.marketing_channel,
		o.marketing_campaign,
		o.marketing_source,
		o.voucher_code,
		o.new_recurring,
		o.store_country,
		o.customer_type,
		s.subscription_id,
		s.subscription_value_eur AS subscription_value,
		(ss.committed_sub_value + ss.additional_committed_sub_value) as committed_sub_value,
		ss.status,
		ss.rental_period,
		ROW_NUMBER() OVER (PARTITION BY s.subscription_id ORDER BY s.fact_day) AS row_num
	FROM tmp_orders_info_prep o 
	LEFT JOIN ods_production.subscription_phase_mapping s 
		ON o.order_id = s.order_id 
	LEFT JOIN master.subscription ss 
		ON s.subscription_id = ss.subscription_id  
	WHERE s.subscription_id IS NOT NULL 
			AND o.is_lending_order <> 1
)		
	SELECT 
		o.reporting_date,
		o.is_from_utm,
		o.is_from_influencer_channel,
		o.is_from_voucher,
		o.is_lending_order,
		o.influencer_name,
		o.social,
		o.vendor,
		o.category_name,
		o.collaboration,
		o.marketing_channel,
		o.marketing_campaign,
		o.marketing_source,
		o.voucher_code,
		o.new_recurring,
		o.store_country,
		o.customer_type,
		count(DISTINCT o.subscription_id) AS number_subscriptions,
		sum(o.subscription_value) AS subscription_value,
		SUM(o.committed_sub_value) as committed_subscription_value,
		SUM(CASE WHEN o.status = 'ACTIVE' THEN o.subscription_value END) AS active_subscription_value,
		COUNT(DISTINCT CASE WHEN o.status = 'ACTIVE' THEN o.subscription_id END) AS active_subscriptions,
		sum(o.rental_period) AS rental_period
	FROM subs_prep o
	WHERE row_num = 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;



DROP TABLE IF EXISTS tmp_fixed_costs;
CREATE TEMP TABLE tmp_fixed_costs
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country, vendor, collaboration)
DISTKEY(reporting_date)
AS
	SELECT 
		c.reporting_date,
		CASE WHEN  
				c.utm_source IS NOT NULL
				OR c.utm_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_from_utm,
		CASE WHEN c.utm_medium = 'influencers' THEN 1 ELSE 0 END AS is_from_influencer_channel,
		CASE WHEN c.voucher IS NOT NULL THEN 1 ELSE 0 END AS is_from_voucher,
		0 AS is_lending_order,
		lower(COALESCE(c.influencer_name,'n/a')) AS influencer_name,
		lower(COALESCE(c.social,'n/a')) AS social,
		lower(COALESCE(c.vendor,'n/a')) AS vendor,
		lower(COALESCE(c.collaboration,'n/a')) AS collaboration,
		lower(COALESCE(c.utm_medium,'n/a')) AS marketing_channel,
		lower(COALESCE(c.utm_campaign,'n/a')) AS marketing_campaign,
		lower(COALESCE(c.utm_source,'n/a')) AS marketing_source,
		COALESCE(c.voucher,'n/a') AS voucher_code,
		'n/a' AS new_recurring,
		COALESCE(c.country,'n/a') AS store_country,
		'n/a' AS customer_type,
		SUM(COALESCE(replace(c.total_spent_local_currency, ',','')::float,0) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))::float AS total_spent_eur,
		SUM(COALESCE(replace(c.agency_fee, ',','')::float,0) * COALESCE(exc_2.exchange_rate_eur, exc_last_2.exchange_rate_eur, 1))::float AS agency_fee_eur,
		SUM(COALESCE(replace(c.views, ',','')::float,0)) AS views,
		SUM(COALESCE(replace(c.impressions, ',','')::float,0)) AS impressions,
		SUM(COALESCE(replace(c.estimated_views, ',','')::float,0)) AS estimated_views
	FROM staging.influencers_fixed_costs c 
	LEFT JOIN  trans_dev.daily_exchange_rate exc
		ON c.reporting_date = exc.date_
		AND c.currency = exc.currency
		AND c.reporting_date::date BETWEEN '2021-10-18' AND '2023-01-14'
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
		ON c.currency = exc_last.currency
	LEFT JOIN  trans_dev.daily_exchange_rate exc_2
		ON c.reporting_date = exc_2.date_
		AND c.currency_agency_fee = exc_2.currency
		AND c.reporting_date::date BETWEEN '2021-10-18' AND '2023-01-14'
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last_2
		ON c.currency_agency_fee = exc_last_2.currency
	WHERE (replace(c.total_spent_local_currency, ',','')::float <> 0
		OR replace(c.views, ',','')::float <> 0
		OR replace(c.impressions, ',','')::float <> 0 
		OR replace(c.estimated_views, ',','')::float <> 0 
		)
		AND DATE_TRUNC('year',c.reporting_date::date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))::date
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;


DROP TABLE IF EXISTS tmp_variable_costs;
CREATE TEMP TABLE tmp_variable_costs
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country, vendor, collaboration)
DISTKEY(reporting_date)
AS
	SELECT 
		o.reporting_date,
		o.is_from_utm,
		o.is_from_influencer_channel,
		o.is_from_voucher,
		o.is_lending_order,
		o.influencer_name,
		o.social,
		o.vendor,
		o.collaboration,
		c.category_name,
		'affiliates'::text AS marketing_channel,
		COALESCE(lower(COALESCE(c1.affiliate,c2.affiliate)),'n/a') AS marketing_campaign,
		COALESCE(lower(COALESCE(c1.affiliate_network,c2.affiliate_network)),'n/a') AS marketing_source,
		o.voucher_code,
		o.new_recurring,
		o.store_country,
		o.customer_type,
		SUM(COALESCE(c1.total_spent_local_currency,0) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)
	FROM tmp_orders_info_prep o
	LEFT JOIN marketing.marketing_cost_daily_affiliate_order c1 
		ON o.order_id = c1.order_id
		AND c1.date::date BETWEEN '2021-10-18' AND '2023-01-14'
	LEFT JOIN  trans_dev.daily_exchange_rate exc
		ON c1.date::date = exc.date_
		AND c1.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
		ON c1.currency = exc_last.currency
	LEFT JOIN marketing.affiliate_validated_orders c2 
		ON o.order_id = c2.order_id
		AND c2.commission_approval = 'APPROVED'
		AND c2.submitted_date::date >= '2023-01-15'
	LEFT JOIN tmp_influencers_category c 
		ON o.order_id = c.order_id
	WHERE (c1.order_id IS NOT NULL 
		OR c2.order_id IS NOT NULL)
		AND o.is_lending_order <> 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;

DROP TABLE IF EXISTS tmp_orders_info;
CREATE TEMP TABLE tmp_orders_info
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country, vendor, collaboration)
DISTKEY(reporting_date)
AS
	SELECT 
		o.reporting_date,
		o.is_from_utm,
		o.is_from_influencer_channel,
		o.is_from_voucher,
		o.is_lending_order,
		o.influencer_name,
		o.social,
		o.vendor, 
		o.collaboration,
		o.marketing_channel,
		o.marketing_campaign,
		o.marketing_source,
		o.voucher_code,
		o.new_recurring,
		o.store_country,
		o.customer_type,
		c.category_name,
		sum(o.voucher_discount) AS voucher_discount,
		sum(o.order_value) AS order_value,
		count(DISTINCT o.order_id) AS created_orders,
		count(DISTINCT CASE WHEN o.completed_orders > 0 THEN o.order_id END) AS submitted_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' THEN o.order_id END) AS paid_orders,
		count(DISTINCT CASE WHEN o.new_recurring = 'NEW' AND o.paid_orders > 0 THEN customer_id END) AS new_customers,
		count(DISTINCT CASE WHEN o.paid_orders > 0 THEN o.customer_id END) AS total_customers
	FROM tmp_orders_info_prep o
	LEFT JOIN tmp_influencers_category c 
		ON o.order_id = c.order_id
	WHERE o.is_lending_order <> 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;


DROP TABLE IF EXISTS tmp_asset_lending_costs;
CREATE TEMP TABLE tmp_asset_lending_costs
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country, vendor, collaboration)
DISTKEY(reporting_date)
AS
	SELECT 
		sp.paid_date::date AS reporting_date,
		op.is_from_utm,
		op.is_from_influencer_channel,
		op.is_from_voucher,
		op.is_lending_order,
		op.influencer_name,
		op.social,
		op.vendor, 
		op.collaboration,
		op.marketing_channel,
		op.marketing_campaign,
		op.marketing_source,
		op.voucher_code,
		op.new_recurring,
		op.store_country,
		op.customer_type,
		c.category_name,
		SUM(COALESCE(sp.amount_subscription,0) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS asset_lending_costs,
		SUM(COALESCE(sp.amount_paid,0) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS amount_paid_asset_lending
	FROM tmp_orders_info_prep op
	LEFT JOIN master.subscription_payment sp 
		ON op.order_id = sp.order_id
	LEFT JOIN  trans_dev.daily_exchange_rate exc
		ON sp.paid_date::date = exc.date_
		AND sp.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
		ON sp.currency = exc_last.currency
	LEFT JOIN tmp_influencers_category c 
		ON op.order_id = c.order_id
	WHERE is_lending_order = 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
	ORDER BY 2	
;	
	


DROP TABLE IF EXISTS tmp_dimensions_combined;
CREATE TEMP TABLE tmp_dimensions_combined
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country, vendor, collaboration)
DISTKEY(reporting_date)
AS
SELECT DISTINCT 
		reporting_date::Date,
		is_from_utm,
		is_from_influencer_channel,
		is_from_voucher,
		is_lending_order,
		influencer_name,
		social,
		vendor, 
		collaboration,
		marketing_channel,
		marketing_campaign,
		marketing_source,
		voucher_code,
		new_recurring,
		store_country,
		customer_type,
		'n/a' AS category_name
	FROM tmp_user_traffic_daily
	UNION 
	SELECT DISTINCT 
		reporting_date::date,
		is_from_utm,
		is_from_influencer_channel,
		is_from_voucher,
		is_lending_order,
		influencer_name,
		social,
		vendor, 
		collaboration,
		marketing_channel,
		marketing_campaign,
		marketing_source,
		voucher_code,
		new_recurring,
		store_country,
		customer_type,
		category_name
	FROM tmp_subs
	WHERE DATE_TRUNC('year',reporting_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
	UNION
	SELECT DISTINCT 
		reporting_date::date,
		is_from_utm,
		is_from_influencer_channel,
		is_from_voucher,
		is_lending_order,
		influencer_name,
		social,
		vendor, 
		collaboration,
		marketing_channel,
		marketing_campaign,
		marketing_source,
		voucher_code,
		new_recurring,
		store_country,
		customer_type,
		'n/a' AS category_name
	FROM tmp_fixed_costs
	WHERE DATE_TRUNC('year',reporting_date::date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
	UNION
	SELECT DISTINCT 
		reporting_date::date,
		is_from_utm,
		is_from_influencer_channel,
		is_from_voucher,
		is_lending_order,
		influencer_name,
		social,
		vendor, 
		collaboration,
		marketing_channel,
		marketing_campaign,
		marketing_source,
		voucher_code,
		new_recurring,
		store_country,
		customer_type,
		category_name
	FROM tmp_variable_costs
	WHERE DATE_TRUNC('year',reporting_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
	UNION
	SELECT DISTINCT 
		reporting_date::date,
		is_from_utm,
		is_from_influencer_channel,
		is_from_voucher,
		is_lending_order,
		influencer_name,
		social,
		vendor, 
		collaboration,
		marketing_channel,
		marketing_campaign,
		marketing_source,
		voucher_code,
		new_recurring,
		store_country,
		customer_type,
		category_name
	FROM tmp_orders_info
	WHERE DATE_TRUNC('year',reporting_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
	UNION
	SELECT DISTINCT 
		reporting_date::date,
		is_from_utm,
		is_from_influencer_channel,
		is_from_voucher,
		is_lending_order,
		influencer_name,
		social,
		vendor, 
		collaboration,
		marketing_channel,
		marketing_campaign,
		marketing_source,
		voucher_code,
		new_recurring,
		store_country,
		customer_type,
		category_name
	FROM tmp_asset_lending_costs	
	WHERE DATE_TRUNC('year',reporting_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
;


DROP TABLE IF EXISTS tmp_influencers_metrics;
CREATE TEMP TABLE tmp_influencers_metrics
SORTKEY(reporting_date, influencer_name, social, marketing_channel, marketing_campaign, marketing_source, new_recurring, store_country, voucher_code, customer_type, vendor, collaboration)
DISTKEY(reporting_date)
AS	
SELECT 
	dc.reporting_date,
	dc.is_from_utm,
	dc.is_from_influencer_channel,
	dc.is_from_voucher,
	dc.is_lending_order,
	dc.influencer_name,
	dc.social,
	dc.vendor, 
	dc.collaboration,
	dc.marketing_channel,
	dc.marketing_campaign,
	dc.marketing_source,
	dc.voucher_code,
	dc.new_recurring,
	dc.store_country,
	dc.customer_type,
	COALESCE(dc.category_name, 'n/a') AS category_name,
	COALESCE(sum(ut.traffic_daily_unique_users),0) AS traffic_daily_unique_users,
	COALESCE(sum(ut.traffic_daily_unique_sessions),0) AS traffic_daily_unique_sessions,
	COALESCE(sum(su.number_subscriptions),0) AS number_subscriptions,
	COALESCE(sum(su.subscription_value),0) AS subscription_value,
	COALESCE(SUM(su.committed_subscription_value),0) as committed_subscription_value,
	COALESCE(SUM(su.active_subscription_value),0) as active_subscription_value,
	COALESCE(SUM(su.active_subscriptions),0) as active_subscriptions,
	COALESCE(SUM(su.rental_period),0) as rental_period,
	COALESCE(SUM(fc.total_spent_eur),0) AS total_spent_eur_fix_cost,
	COALESCE(SUM(fc.agency_fee_eur),0) AS agency_fee_eur_fix_cost,
	COALESCE(SUM(vc.total_spent_eur),0) AS total_spent_eur_variable_cost,
	COALESCE(sum(oi.voucher_discount),0) AS voucher_discount,
	COALESCE(sum(oi.order_value),0) AS order_value,
	COALESCE(sum(oi.created_orders),0) AS created_orders,
	COALESCE(sum(oi.submitted_orders),0) AS submitted_orders,
	COALESCE(sum(oi.paid_orders),0) AS paid_orders,
	COALESCE(sum(oi.new_customers),0) AS new_customers,
	COALESCE(sum(oi.total_customers),0) AS total_customers,
	COALESCE(sum(lo.asset_lending_costs),0) AS asset_lending_costs,
	COALESCE(sum(lo.amount_paid_asset_lending),0) AS amount_paid_asset_lending,
	COALESCE(sum(fc.views),0) AS views,
	COALESCE(sum(fc.impressions),0) AS impressions,
	COALESCE(sum(fc.estimated_views),0) AS estimated_views
FROM tmp_dimensions_combined dc
LEFT JOIN tmp_user_traffic_daily ut
	ON 	dc.reporting_date = ut.reporting_date
	AND dc.is_from_utm = ut.is_from_utm
	AND dc.is_from_influencer_channel = ut.is_from_influencer_channel
	AND dc.is_from_voucher = ut.is_from_voucher
	AND dc.is_lending_order = ut.is_lending_order
	AND dc.influencer_name = ut.influencer_name
	AND dc.social = ut.social
	AND dc.marketing_channel = ut.marketing_channel
	AND dc.marketing_campaign = ut.marketing_campaign
	AND dc.marketing_source = ut.marketing_source
	AND dc.voucher_code = ut.voucher_code
	AND dc.new_recurring = ut.new_recurring
	AND dc.store_country = ut.store_country
	AND dc.customer_type = ut.customer_type
	AND dc.vendor = ut.vendor
	AND dc.collaboration = ut.collaboration
	AND dc.category_name = 'n/a'
LEFT JOIN tmp_subs su
	ON 	dc.reporting_date = su.reporting_date
	AND dc.is_from_utm = su.is_from_utm
	AND dc.is_from_influencer_channel = su.is_from_influencer_channel
	AND dc.is_from_voucher = su.is_from_voucher
	AND dc.is_lending_order = su.is_lending_order
	AND dc.influencer_name = su.influencer_name
	AND dc.social = su.social
	AND dc.marketing_channel = su.marketing_channel
	AND dc.marketing_campaign = su.marketing_campaign
	AND dc.marketing_source = su.marketing_source
	AND dc.voucher_code = su.voucher_code
	AND dc.new_recurring = su.new_recurring
	AND dc.store_country = su.store_country
	AND dc.customer_type = su.customer_type
	AND dc.vendor = su.vendor
	AND dc.collaboration = su.collaboration
	AND dc.category_name = su.category_name
LEFT JOIN tmp_fixed_costs fc
	ON 	dc.reporting_date = fc.reporting_date
	AND dc.is_from_utm = fc.is_from_utm
	AND dc.is_from_influencer_channel = fc.is_from_influencer_channel
	AND dc.is_from_voucher = fc.is_from_voucher
	AND dc.is_lending_order = fc.is_lending_order
	AND dc.influencer_name = fc.influencer_name
	AND dc.social = fc.social
	AND dc.marketing_channel = fc.marketing_channel
	AND dc.marketing_campaign = fc.marketing_campaign
	AND dc.marketing_source = fc.marketing_source
	AND dc.voucher_code = fc.voucher_code
	AND dc.new_recurring = fc.new_recurring
	AND dc.store_country = fc.store_country
	AND dc.customer_type = fc.customer_type
	AND dc.vendor = fc.vendor
	AND dc.collaboration = fc.collaboration
	AND dc.category_name = 'n/a'
LEFT JOIN tmp_variable_costs vc
	ON 	dc.reporting_date = vc.reporting_date
	AND dc.is_from_utm = vc.is_from_utm
	AND dc.is_from_influencer_channel = vc.is_from_influencer_channel
	AND dc.is_from_voucher = vc.is_from_voucher
	AND dc.is_lending_order = vc.is_lending_order
	AND dc.influencer_name = vc.influencer_name
	AND dc.social = vc.social
	AND dc.marketing_channel = vc.marketing_channel
	AND dc.marketing_campaign = vc.marketing_campaign
	AND dc.marketing_source = vc.marketing_source
	AND dc.voucher_code = vc.voucher_code
	AND dc.new_recurring = vc.new_recurring
	AND dc.store_country = vc.store_country
	AND dc.customer_type = vc.customer_type
	AND dc.vendor = vc.vendor
	AND dc.collaboration = vc.collaboration
	AND dc.category_name = vc.category_name
LEFT JOIN tmp_orders_info oi
	ON 	dc.reporting_date = oi.reporting_date
	AND dc.is_from_utm = oi.is_from_utm
	AND dc.is_from_influencer_channel = oi.is_from_influencer_channel
	AND dc.is_from_voucher = oi.is_from_voucher
	AND dc.is_lending_order = oi.is_lending_order
	AND dc.influencer_name = oi.influencer_name
	AND dc.social = oi.social
	AND dc.marketing_channel = oi.marketing_channel
	AND dc.marketing_campaign = oi.marketing_campaign
	AND dc.marketing_source = oi.marketing_source
	AND dc.voucher_code = oi.voucher_code
	AND dc.new_recurring = oi.new_recurring
	AND dc.store_country = oi.store_country
	AND dc.customer_type = oi.customer_type
	AND dc.vendor = oi.vendor
	AND dc.collaboration = oi.collaboration
	AND dc.category_name = oi.category_name
LEFT JOIN tmp_asset_lending_costs lo
	ON 	dc.reporting_date = lo.reporting_date
	AND dc.is_from_utm = lo.is_from_utm
	AND dc.is_from_influencer_channel = lo.is_from_influencer_channel
	AND dc.is_from_voucher = lo.is_from_voucher
	AND dc.is_lending_order = lo.is_lending_order
	AND dc.influencer_name = lo.influencer_name
	AND dc.social = lo.social
	AND dc.marketing_channel = lo.marketing_channel
	AND dc.marketing_campaign = lo.marketing_campaign
	AND dc.marketing_source = lo.marketing_source
	AND dc.voucher_code = lo.voucher_code
	AND dc.new_recurring = lo.new_recurring
	AND dc.store_country = lo.store_country
	AND dc.customer_type = lo.customer_type
	AND dc.vendor = lo.vendor
	AND dc.collaboration = lo.collaboration
	AND dc.category_name = lo.category_name
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;


BEGIN TRANSACTION;

DROP TABLE IF EXISTS dm_marketing.influencers_metrics;
CREATE TABLE dm_marketing.influencers_metrics AS
SELECT *
FROM tmp_influencers_metrics
;

END TRANSACTION;





DROP TABLE IF EXISTS tmp_influencers_products;
CREATE TEMP TABLE tmp_influencers_products
SORTKEY(reporting_date , is_from_utm, is_from_influencer_channel, is_from_voucher, influencer_name, social, marketing_channel,
	marketing_campaign, marketing_source, voucher_code, new_recurring, store_country, customer_type, subcategory_name, product_sku, product_name, vendor, collaboration)
DISTKEY(reporting_date)
AS
SELECT DISTINCT 
	o.reporting_date ,
	o.is_from_utm,
	o.is_from_influencer_channel,
	o.is_from_voucher,
	o.influencer_name,
	o.social,
	o.vendor,
	o.collaboration,
	o.marketing_channel,
	o.marketing_campaign,
	o.marketing_source,
	o.voucher_code,
	o.new_recurring,
	o.store_country,
	o.customer_type,
	COALESCE(ro.subcategory_name,'n/a') AS subcategory_name,
	COALESCE(ro.category_name, 'n/a') AS category_name,
	COALESCE(ro.product_sku,'n/a') AS product_sku,
	COALESCE(ro.product_name,'n/a') AS product_name,
	count(DISTINCT o.order_id) AS paid_orders
FROM tmp_orders_info_prep o 
LEFT JOIN tmp_influencers_category ro
	ON o.order_id = ro.order_id
WHERE o.is_lending_order <> 1 --deleting lending orders	
	AND o.paid_orders >= 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
;



BEGIN TRANSACTION;

DROP TABLE IF EXISTS dm_marketing.influencers_products;
CREATE TABLE dm_marketing.influencers_products AS
SELECT *
FROM tmp_influencers_products;

END TRANSACTION;

GRANT SELECT ON dm_marketing.influencers_products TO tableau;
GRANT SELECT ON dm_marketing.influencers_metrics TO tableau;



