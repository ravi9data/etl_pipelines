DROP TABLE IF EXISTS dwh.acquisition_weekly_report;
CREATE TABLE dwh.acquisition_weekly_report AS 
WITH pv AS (
	SELECT 
		session_id,
		count(DISTINCT CASE WHEN page_type='pdp' THEN anonymous_id END) AS pdp_users
	FROM traffic.page_views pv
	GROUP BY 1
)
,a AS (
	SELECT 
		DATE_TRUNC('week', s.session_start)::date AS fact_date,
		coalesce(st.store_short,'Grover') AS store,
		st.store_name,
		c.customer_type,	
		CASE WHEN COALESCE(c.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
		     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
		     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
		     WHEN s.first_page_url ILIKE  '%/business%' THEN 'B2B Unknown Split' 
		     ELSE 'B2C' END AS customer_type_freelancer_split,
		CASE WHEN s.session_start>c.start_date_of_first_subscription THEN 'RECURRING'
		 	 WHEN c.customer_id IS NOT NULL THEN 'NEW'
		 	 ELSE 'Logged-out'
		  END AS new_recurring,
		CASE WHEN browser='Apple WebKit' THEN 'App' WHEN device_type='Mobile' THEN 'Mobile'
		  	 ELSE 'Desktop & Others'
		  END AS device,
		marketing_channel,
		is_paid,
		MAX(CASE WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	      		WHEN s.geo_country = 'DE' THEN 'Germany'
	      		WHEN s.geo_country = 'ES' THEN 'Spain'
	      		WHEN s.geo_country = 'AT' THEN 'Austria'
	      		WHEN s.geo_country = 'NL' THEN 'Netherlands'
	      		WHEN s.geo_country = 'US' THEN 'United States' 
	      	END) AS country, 
		COUNT(DISTINCT anonymous_id) AS traffic,
		COUNT(DISTINCT CASE WHEN pv.pdp_users>=1 THEN anonymous_id END) AS pdp_traffic,
		COUNT(DISTINCT CASE WHEN session_index<=1 THEN anonymous_id END) AS new_traffic,
		COUNT(DISTINCT CASE WHEN session_index>1 THEN anonymous_id END) AS recurring_traffic
	FROM traffic.sessions s
	LEFT JOIN pv 
		ON pv.session_id=s.session_id
	LEFT JOIN master.customer c 
		ON s.customer_id=c.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
		ON c.company_type_name = fre.company_type_name
	LEFT JOIN ods_production.store st 
	 	ON st.id=s.store_id
	GROUP BY  1,2,3,4,5,6,7,8,9
	ORDER BY 1 DESC 
)
,  is_paid_info_prep AS (
	SELECT DISTINCT 
	 	COALESCE(marketing_channel,'Others') AS channel, 
	 	is_paid,
	 	count(DISTINCT session_id) AS session_id 
	FROM traffic.sessions s
	GROUP BY 1,2
)
, is_paid_info AS (
	SELECT DISTINCT 
	 	channel, 
	 	is_paid,
	 	ROW_NUMBER() OVER (PARTITION BY channel ORDER BY session_id DESC) AS row_num
	FROM is_paid_info_prep
)
,b AS (
	SELECT  
		DATE_TRUNC('week',o.created_date)::date AS fact_Date,
		COALESCE(o.store_short,'Grover') AS store,
		o.customer_type ,
		o.store_name,
		o.new_recurring,
		CASE 
			WHEN browser='Apple WebKit' THEN 'App'
			WHEN os in ('Android','iOS')THEN 'Mobile'
			WHEN oc.order_id is null THEN 'n/a'
			ELSE 'Desktop & Others'
			END AS device,
		REPLACE(oc.last_touchpoint_excl_direct,'only Direct','Direct') AS marketing_channel,
		p.is_paid,
		o.store_country AS country, 
		CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
		     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
		     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
		  ELSE 'n/a' END AS customer_type_freelancer_split,
		SUM(o.cart_orders) AS carts,
		SUM(o.cart_logged_in_orders) AS cart_logged_in_orders,
		SUM(o.address_orders) AS address_orders,
		SUM(o.payment_orders) AS payment_orders,
		SUM(o.summary_orders) AS summary_orders,
		SUM(o.completed_orders) AS submitted_orders, 
		SUM(o.declined_orders) AS declined_orders,
		SUM(o.failed_first_payment_orders) AS failed_first_payment_orders,
		SUM(o.cancelled_orders) AS cancelled_orders,
		SUM(o.paid_orders) AS paid_orders,
		SUM(CASE WHEN o.status='PAID' THEN o.voucher_discount END) AS acquisition_voucher_amount
	FROM master."order" o
	LEFT JOIN master.customer mc 
	  	ON o.customer_id = mc.customer_id 
  	LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
		ON mc.company_type_name = fre.company_type_name
	LEFT JOIN traffic.order_conversions oc
	 	ON o.order_id=oc.order_id
	LEFT JOIN is_paid_info p 
	 	ON p.channel=replace(oc.last_touchpoint_excl_direct,'only Direct','Direct')
	 	AND p.row_num = 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT 
	COALESCE(a.fact_date,b.fact_Date) AS fact_Date,
	COALESCE(a.store,b.store) AS store,
	COALESCE(a.new_recurring,b.new_recurring) AS new_recurring,
	COALESCE(a.device,b.device) AS device,
	COALESCE(a.store_name,b.store_name) AS store_name,
	COALESCE(a.marketing_channel,b.marketing_channel) AS marketing_channel,
	COALESCE(a.is_paid,b.is_paid) AS is_paid,
	COALESCE(a.customer_type_freelancer_split, b.customer_type_freelancer_split) AS customer_type_freelancer_split,
	a.traffic,
	a.customer_type,
	a.pdp_traffic,
	a.new_traffic,
	a.recurring_traffic,
	carts,
	cart_logged_in_orders,
	address_orders,
	payment_orders,
	summary_orders,
	submitted_orders, 
	declined_orders,
	failed_first_payment_orders,
	cancelled_orders,
	paid_orders,
	COALESCE(b.country, a.country) AS country 
FROM b  
FULL OUTER JOIN  a
	 ON  a.fact_Date=b.fact_Date
	 AND a.store=b.store
	 AND a.new_recurring=b.new_recurring
	 AND a.device=b.device
	 AND a.marketing_channel=b.marketing_channel
	 AND a.is_paid=b.is_paid
	 AND a.customer_type=b.customer_type
	 AND a.store_name = b.store_name
	 AND a.country = b.country 
	 AND a.customer_type_freelancer_split = b.customer_type_freelancer_split
	 ;


GRANT SELECT ON dwh.acquisition_weekly_report TO tableau;
