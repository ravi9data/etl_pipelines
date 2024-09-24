CREATE OR REPLACE VIEW dm_marketing.v_rfm_segmentation_metrics AS 
--monthly level 
WITH countries_sessions AS (
    SELECT
        s.customer_id,
        CASE WHEN store_name IN ('Austria', 'Spain', 'Germany', 'Netherlands',
                                 'United States') THEN store_name
             WHEN store_name = 'Grover B2B Germany' THEN 'Germany'
             WHEN store_name = 'Grover B2B Spain' THEN 'Spain'
             WHEN store_name = 'Grover B2B Netherlands' THEN 'Netherlands'
             WHEN store_name = 'Grover B2B Austria' THEN 'Austria'
             WHEN geo_country = 'AT' THEN 'Austria'
             WHEN geo_country = 'ES' THEN 'Spain'
             WHEN geo_country = 'DE' THEN 'Germany'
             WHEN geo_country = 'NL' THEN 'Netherlands'
             WHEN geo_country = 'US' THEN 'United States' 
        END AS country_name,
        ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY session_start) AS row_sessions
    FROM traffic.sessions s
    LEFT JOIN master.customer c 
		ON s.customer_id = c.customer_id
	WHERE country_name IS NOT NULL
		AND s.customer_id IS NOT NULL
		AND c.signup_country = 'never_add_to_cart'
		AND s.session_start::date >= dateadd('month',-13,date_trunc('month',current_date))::date
)
,  potential_churners_month AS (
    SELECT DISTINCT
        s.customer_id,
        s."date"
    FROM master.subscription_historical s
    WHERE status = 'ACTIVE'
    	AND minimum_cancellation_date::date BETWEEN DATE_TRUNC('month',  "date") AND LAST_DAY( "date")
    	AND "date"::date >= dateadd('month', -13, date_trunc('month',  current_date))::date
    	AND LAST_DAY( "date") = "date"
)
, previous_month_status AS (
	SELECT 
		ch.date,
		ch.customer_id,
		CASE WHEN crm_label = 'Lapsed' THEN 'Inactive' ELSE crm_label END AS crm_label_final,
		lag(crm_label_final) OVER (PARTITION BY ch.customer_id ORDER BY ch.date) AS previous_month_status
	FROM master.customer_historical ch
	WHERE ch.date = dateadd('day',-1,dateadd('month',1,date_trunc('month',ch.date)))
		AND ch.date >= dateadd('month', -13, date_trunc('month', current_date))::date
)
, orders AS (
	SELECT
		dateadd('day',-1,dateadd('month',1,date_trunc('month',o.paid_date::date)))::date AS paid_date,
		o.customer_id,
		count(DISTINCT o.order_id) AS paid_orders
 	FROM master."order" o 
	WHERE o.paid_orders >= 1
		AND paid_date::date >= dateadd('month', -13, date_trunc('month', current_date))::date
	GROUP BY 1,2
)
, subscriptions AS (
	SELECT 
		dateadd('day',-1,dateadd('month',1,date_trunc('month',s.start_date::date)))::date AS start_date, 
		s.customer_id,
		count(DISTINCT s.subscription_id) AS acquired_subscriptions,
		sum(s.subscription_value_euro) AS acquired_subscription_value
	FROM master.subscription s
	WHERE s.start_date::date >= dateadd('month', -13, date_trunc('month', current_date))::date
	GROUP BY 1,2
)
, customers_rfm_base AS (
	SELECT
		dateadd('day',-1,(date_trunc('month',rfm.date)))::date AS fact_date, ---END OF the month
		rfm.customer_id,
		rfm.recency,
		rfm.frequency,
		rfm.monetary,
		rfm_segmentation,
		CASE 
			WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country 
			ELSE s.country_name
		END AS country_name,
		c.customer_type,
		CASE
			WHEN date_trunc('month',c.customer_acquisition_cohort)::date = date_trunc('month', dateadd('day',-1,rfm.date))::date THEN 'New Users' --clarify WITH Leisha
			WHEN c.crm_label = 'Active' AND ps.previous_month_status = 'Inactive' THEN 'Reactivated Customers'
            WHEN c.crm_label = 'Active' AND pc.customer_id IS NULL THEN 'Active Customers'
            WHEN c.crm_label = 'Active' AND pc.customer_id IS NOT NULL THEN 'Active Customers - Potential Churners'
            WHEN c.crm_label = 'Inactive' AND ps.previous_month_status = 'Inactive' THEN 'Inactive Customers'
            WHEN c.crm_label = 'Inactive' AND ps.previous_month_status = 'Active' THEN 'Churn Customers'
            --WHEN c.crm_label = 'Lapsed' THEN 'Inactive'
            --ELSE c.crm_label
        END AS customer_label,
        o.paid_orders,
        su.acquired_subscriptions,
        su.acquired_subscription_value
	FROM dm_marketing.customer_rfm_segmentation_historical rfm
	LEFT JOIN countries_sessions s 
		ON s.customer_id = rfm.customer_id
		AND s.row_sessions = 1
	LEFT JOIN master.customer_historical c
		ON c.customer_id = rfm.customer_id 
			AND c.date = dateadd('day',-1,(date_trunc('month',rfm.date)))::date
	LEFT JOIN potential_churners_month pc          --NOT joining on date because we only look at the current_month
		ON rfm.customer_id = pc.customer_id
		AND pc.date = dateadd('day',-1,(date_trunc('month',rfm.date)))::date
	LEFT JOIN previous_month_status ps
		ON ps.customer_id = rfm.customer_id
			AND ps.date = dateadd('day',-1,(date_trunc('month',rfm.date)))::date
	LEFT JOIN orders o 
		ON o.customer_id = rfm.customer_id
			AND o.paid_date = dateadd('day',-1,(date_trunc('month',rfm.date)))::date
	LEFT JOIN subscriptions su
		ON su.customer_id = rfm.customer_id 
			AND su.start_date = dateadd('day',-1,(date_trunc('month',rfm.date)))::date
	LEFT JOIN master.customer ct 
		ON ct.customer_id = rfm.customer_id 
	WHERE rfm.date = date_trunc('month',rfm.date)::date ---FIRST DAY OF the month
		AND rfm.date >= dateadd('month', -13, date_trunc('month', current_date))::date
		AND ct.created_at::date <= DATEADD('day',-1,date_trunc('month',rfm.date))::date
)
--weekly level
,  potential_churners_week AS ( 
    SELECT DISTINCT 
        customer_id,
        s.date
    FROM master.subscription_historical s
    WHERE status = 'ACTIVE'
    	AND minimum_cancellation_date::date BETWEEN DATE_TRUNC('week', s.date) AND dateadd('day',-1,dateadd('week',1,date_trunc('week',s.date)))
    	AND start_date::date >= dateadd('month', -13, date_trunc('month', current_date))::date
    	AND s.date = dateadd('day',-1,dateadd('week',1,date_trunc('week',s.date)))
)
, previous_week_status AS (
	SELECT 
		ch.date,
		ch.customer_id,
		CASE WHEN crm_label = 'Lapsed' THEN 'Inactive' ELSE crm_label END AS crm_label_final,
		lag(crm_label_final) OVER (PARTITION BY ch.customer_id ORDER BY ch.date) AS previous_week_status
	FROM master.customer_historical ch
	WHERE ch.date = dateadd('day',-1,dateadd('week',1,date_trunc('week',ch.date)))
		AND ch.date >= dateadd('month', -13, date_trunc('month', current_date))::date
)
, orders_week AS (
	SELECT
		dateadd('day',-1,dateadd('week',1,date_trunc('week',o.paid_date::date)))::date AS paid_date, 
		o.customer_id,
		count(DISTINCT o.order_id) AS paid_orders
 	FROM master."order" o 
	WHERE o.paid_orders >= 1 
		AND paid_date::date >= dateadd('month', -13, date_trunc('month', current_date))::date
	GROUP BY 1,2
)
, subscriptions_week AS (
	SELECT 
		dateadd('day',-1,dateadd('week',1,date_trunc('week',s.start_date)))::date AS start_date, 
		s.customer_id,
		count(DISTINCT s.subscription_id) AS acquired_subscriptions,
		sum(s.subscription_value_euro) AS acquired_subscription_value
	FROM master.subscription s
	WHERE s.start_date::date >= dateadd('month', -13, date_trunc('month', current_date))::date
	GROUP BY 1,2
)
, customers_rfm_base_week AS (
	SELECT
		dateadd('day',-1,(date_trunc('week',rfm.date)))::date AS fact_date,
		rfm.customer_id,
		rfm.recency,
		rfm.frequency,
		rfm.monetary,
		rfm_segmentation,
		CASE 
			WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country 
			ELSE s.country_name
		END AS country_name,
		c.customer_type,
		CASE
			WHEN date_trunc('week',c.customer_acquisition_cohort)::date = date_trunc('week', dateadd('day',-1,rfm.date))::date THEN 'New Users'
			WHEN c.crm_label = 'Active' AND ps.previous_week_status = 'Inactive' THEN 'Reactivated Customers'
            WHEN c.crm_label = 'Active' AND pc.customer_id IS NULL THEN 'Active Customers'
            WHEN c.crm_label = 'Active' AND pc.customer_id IS NOT NULL THEN 'Active Customers - Potential Churners'
            WHEN c.crm_label = 'Inactive' AND ps.previous_week_status = 'Inactive' THEN 'Inactive Customers'
            WHEN c.crm_label = 'Inactive' AND ps.previous_week_status = 'Active' THEN 'Churn Customers'
           -- WHEN c.crm_label = 'Lapsed' THEN 'Inactive'
           -- ELSE c.crm_label
        END AS customer_label,
        o.paid_orders,
        su.acquired_subscriptions,
        su.acquired_subscription_value
	FROM dm_marketing.customer_rfm_segmentation_historical rfm
	LEFT JOIN countries_sessions s 
		ON s.customer_id = rfm.customer_id
		AND s.row_sessions = 1
	LEFT JOIN master.customer_historical c
		ON c.customer_id = rfm.customer_id 
			AND c.date = dateadd('day',-1,(date_trunc('week',rfm.date)))::date 
	LEFT JOIN potential_churners_week pc          --NOT joining on date because we only look at the current_week
		ON pc.customer_id = rfm.customer_id
			AND pc.date = dateadd('day',-1,(date_trunc('week',rfm.date)))::date 
	LEFT JOIN previous_week_status ps
		ON ps.customer_id = rfm.customer_id
			AND ps.date = dateadd('day',-1,(date_trunc('week',rfm.date)))::date
	LEFT JOIN orders_week o 
		ON o.customer_id = rfm.customer_id
			AND o.paid_date = dateadd('day',-1,(date_trunc('week',rfm.date)))::date
	LEFT JOIN subscriptions_week su
		ON su.customer_id = rfm.customer_id 
			AND su.start_date = dateadd('day',-1,(date_trunc('week',rfm.date)))::date
	LEFT JOIN master.customer ct
		ON ct.customer_id = rfm.customer_id 
	WHERE rfm.date = date_trunc('week',rfm.date)::date 
		AND rfm.date >= dateadd('month', -13, date_trunc('month', current_date))::date
		AND ct.created_at::date <= dateadd('day',-1,date_trunc('week',rfm.date))::date
)
SELECT 
	'Monthly' AS date_dimension,
	rfm.fact_date,
	rfm.rfm_segmentation,
	rfm.customer_type,
	rfm.country_name,
	rfm.customer_label,
	sum(recency) AS recency, -- will divide by number of customers IN tableau
	sum(frequency) AS frequency,
	sum(monetary) AS monetary,
	count(DISTINCT rfm.customer_id) AS number_of_customers,
	sum(paid_orders) AS paid_orders,
	sum(acquired_subscriptions) AS acquired_subscriptions,
	sum(acquired_subscription_value) AS acquired_subscription_value
FROM customers_rfm_base rfm
GROUP BY 1,2,3,4,5,6
UNION 
SELECT 
	'Weekly' AS date_dimension,
	r.fact_date,
	r.rfm_segmentation,
	r.customer_type,
	r.country_name,
	r.customer_label,
	sum(recency) AS recency,
 	sum(frequency) AS frequency,
	sum(monetary) AS monetary,
	count(DISTINCT r.customer_id) AS number_of_customers,
	sum(paid_orders) AS paid_orders,
	sum(acquired_subscriptions) AS acquired_subscriptions,
	sum(acquired_subscription_value) AS acquired_subscription_value
FROM customers_rfm_base_week r
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA binding
;
​
GRANT SELECT ON dm_marketing.v_rfm_segmentation_metrics TO tableau;
