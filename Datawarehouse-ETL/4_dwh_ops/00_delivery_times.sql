DROP TABLE IF EXISTS dm_operations.delivery_times;
CREATE TABLE dm_operations.delivery_times AS
WITH allocation_shipment AS (
	SELECT DISTINCT
		sa.tracking_number, 
		sa.region,
		COALESCE(sa.shipping_country,sa.receiver_country) AS country,
		sa.warehouse,
		CASE
			WHEN sa.customer_type = 'normal_customer' 
				THEN 'Normal Customer'
			ELSE 'Business Customer'
		END AS customer_type,
		shipment_service,
		CASE
			WHEN shipment_service = 'Hermes 2MH Standard'
				THEN '2 Man Handling (Bulky)'
			ELSE 'Small Parcel'
		END AS shipment_service_grouping,
		sa.order_completed_at, 
		COALESCE(sa.first_delivery_attempt , sa.delivered_at) AS first_delivery_attempt, --to be fact date
		CASE 
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
				    	count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  ) 
			<= 4)
			AND country = 'Germany'
			AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
        				count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  ) 
			<= 3)
			AND country = 'Netherlands'
			AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
						count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  ) 
			<= 4)
			AND country = 'Austria'
			AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
						count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  )
			<= 5)
			AND country = 'Spain'
			AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
        				count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  )
			<= 10)
			AND country = 'Germany'
			AND shipment_service = 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
						count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  )
			<= 12)
			AND country = 'Netherlands'
			AND shipment_service = 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day',sa.order_completed_at,sa.first_delivery_attempt) 
        		- (
        			SELECT
						count(1)
					FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)
				  ) 
			<= 10)
			AND country = 'Austria'
			AND shipment_service = 'Hermes 2MH Standard'
				THEN TRUE
			--No bulky/2 man handling for Spain
			ELSE FALSE
		END AS is_promised_delivery_time_fullfillment,
		round(transit_time / 24::float, 2) AS transit_time,
		net_ops_cycle
FROM ods_operations.allocation_shipment sa
	LEFT JOIN master."subscription" b ON sa.subscription_id = b.subscription_id
WHERE COALESCE(first_delivery_attempt , sa.delivered_at) > dateadd('month',-2,date_trunc('month', current_date))
	AND sa.customer_type IS NOT NULL
	AND b.store_short NOT IN ('Partners Offline', 'Partners Online')
)
,
--create a temp table to define which metric in which column (section) in Tableau
--we do not want to list all shipment service in the report, so this is also kind of a filter 
shipment_service_config AS (
SELECT
	'Germany' sc,
	1 n,
	'DHL Named person only' ss
UNION
SELECT
	'Germany',
	1,
	'UPS Standard Direct Delivery Only'
UNION
SELECT
	'Germany',
	2,
	'DHL Ident Check'
UNION
SELECT
	'Germany',
	2,
	'Hermes 2MH Standard'
UNION
SELECT
	'Austria',
	1,
	'UPS Standard Direct Delivery Only'
UNION
SELECT
	'Austria',
	2,
	'DHL Ident Check'
UNION
SELECT
	'Austria',
	2,
	'Hermes 2MH Standard'
UNION
SELECT
	'Netherlands',
	1,
	'UPS Standard Direct Delivery Only'
UNION
SELECT
	'Netherlands',
	2,
	'DHL Ident Check'
UNION
SELECT
	'Netherlands',
	2,
	'Hermes 2MH Standard'
UNION
SELECT
	'Spain',
	1,
	'UPS Standard Direct Delivery Only'
UNION
SELECT
	'Spain',
	2,
	'DHL Ident Check'
)
SELECT 
	a.first_delivery_attempt AS fact_date, 
	a.tracking_number, 
	a.region, 
	a.country AS shipping_country, 
	a.warehouse,
	a.customer_type, 
	a.shipment_service,
	a.shipment_service_grouping, 
	a.order_completed_at,
	a.first_delivery_attempt, 
	a.is_promised_delivery_time_fullfillment,
	a.transit_time, 
	a.net_ops_cycle,
	s.n AS section_number
FROM allocation_shipment a
INNER JOIN shipment_service_config s 
	ON a.country = s.sc
	AND a.shipment_service = s.ss 
;

GRANT SELECT ON dm_operations.delivery_times TO tableau;
