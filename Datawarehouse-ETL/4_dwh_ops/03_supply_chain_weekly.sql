DROP TABLE IF EXISTS dm_operations.supply_chain_weekly;

CREATE TABLE dm_operations.supply_chain_weekly AS 
WITH dates AS (
	SELECT
		dateadd(
			'week',
			- ordinal,
			date_trunc('week', current_date)
		) reporting_date
	FROM public.numbers
	WHERE
		reporting_date >= date_trunc('year', current_date)
),
------------
------------ DELIVERY MANAGEMENT
------------
dm_klarfalle_prep AS (
	SELECT 
		'KLÄRFÄLLE DAMAGED ASSETS' AS kpi_name,
		reporting_date	
	FROM dates
	UNION
	SELECT 
		'KLÄRFÄLLE MISSING QUANTITIES' AS kpi_name,
		reporting_date	
	FROM dates
	UNION
	SELECT 
		'KLÄRFÄLLE NO/WRONG PPW' AS kpi_name,
		reporting_date	
	FROM dates
),	
dm_klarfalle AS (
	SELECT
		'DELIVERY MANAGEMENT' AS section_name,
		'EU' AS region,
		d.kpi_name,
		d.reporting_date,
		COALESCE(s.kpi_value, 0) AS kpi_value
	FROM dm_klarfalle_prep d
	LEFT JOIN dm_operations.supply_chain_daily s
		ON d.kpi_name = s.kpi_name
		AND d.reporting_date = s.reporting_date
),
--stakeholder wants to see null values as 0
--due to formatting and % diff calculations in Tableau it seems not possible
--first create a temp table of last three weeks crossed by metric names and regions
dates_pending_allocation AS (
	SELECT
		reporting_date,
		kpi_name,
		region
	FROM dates,
		(
			SELECT 'PENDING ALLO. (PARTNERS)' kpi_name
			UNION
			SELECT 'PENDING ALLO. (GROVER)'
		),
		(
			SELECT 'EU' region
			UNION
			SELECT 'US'
		)
	WHERE
		reporting_date > dateadd ('week', -4, date_trunc('week', current_date))
),
--calculate the pending allocations
pre_pending_allocation AS (
	SELECT
		region,
		'PENDING ALLO. ' || CASE
			WHEN store_short IN ('Partners Online', 'Partners Offline')
				THEN '(PARTNERS)'
			ELSE '(GROVER)'
		END AS kpi_name,
		datum AS reporting_date,
		SUM(pending_allocations) AS kpi_value
	FROM ods_production.inventory_reservation_pending_historical
	WHERE
		(
			hours = 13 --let's take middle of the day, 
			--orders should be reviewed and allocated until 13
			AND date_part(dow, datum) = 1
			AND datum < current_date
		) --old mondays
		OR (
			datum = current_date
			AND date_part(dow, current_date) = 1 --if we are on last monday, then take latest hour before 13
			AND hours = (
				SELECT
					LEAST(MAX(hours), 13)
				FROM ods_production.inventory_reservation_pending_historical
				WHERE
					datum = current_date
			)
		)
	GROUP BY 1, 2, 3
),
pending_allocation AS (
	SELECT
		'DELIVERY MANAGEMENT' AS section_name,
		d.region,
		d.kpi_name,
		d.reporting_date,
		COALESCE (p.kpi_value, 0) as kpi_value
	FROM dates_pending_allocation d
	LEFT JOIN pre_pending_allocation p
		ON d.reporting_date = p.reporting_date
		AND d.kpi_name = p.kpi_name
		AND d.region = p.region
),
delivery_schedule AS (
	SELECT
		'DELIVERY MANAGEMENT' AS section_name,
		'EU' AS region,
		'DELIVERY SCHEDULE' AS kpi_name,
		date_trunc('week', reporting_date) AS reporting_date,
		SUM(weekly_quantity) AS kpi_value
	FROM stg_external_apis.sc_eu_inbound_schedule
	WHERE
		warehouse = 'Roermond'
		AND (
			--get the latest number
			reporting_date = (
				SELECT
					MAX(reporting_date)
				FROM stg_external_apis.sc_eu_inbound_schedule
			) --and friday value of previous weeks as most up-to-date value
			OR (
				date_part(dow, reporting_date) = 5
				AND date_trunc('week', reporting_date) < date_trunc('week', current_date)
			)
		)
	GROUP BY 1, 2, 3, 4
),
rejected_deliveries AS (
	SELECT
		'DELIVERY MANAGEMENT' AS section_name,
		'EU' AS region,
		'SHIPMENTS REJECTED BY WH' AS kpi_name,
		date_trunc('week', "reported date") AS reporting_date,
		COUNT(1) AS kpi_value
	FROM stg_external_apis_dl.sc_ib_rejected_deliveries
	GROUP BY 1, 2, 3, 4
),
------------
------------ INBOUND
------------
assets_booked AS (
	SELECT
		'INBOUND' AS section_name,
		CASE
			WHEN warehouse IN ('ups_softeon_eu_nlrng')
				THEN 'EU'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN 'US'
		END AS region,
		CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN 'ASSETS BOOKED'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN 'ASSETS BOOKED'
		END AS kpi_name,
		date_trunc('week', created_at :: TIMESTAMP) AS reporting_date,
		SUM(booked_asset) AS kpi_value
	FROM dm_operations.assets_booked
	WHERE
		warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse'
		)
	GROUP BY 1, 2, 3, 4
), inb_klarfalle AS (
	SELECT
		'INBOUND' AS section_name,
		'EU' AS region,
		kpi_name,
		reporting_date,
		kpi_value
	FROM dm_operations.supply_chain_daily
	WHERE
		date_part(dow, reporting_date) = 1
		AND kpi_name IN (
			'TOTAL KLÄRFÄLLE',
			'PENDING KLÄRFÄLLE UPS',
			'PENDING VALUE KLÄRFÄLLE (QTY)',
			'UNRESOLVED KLÄRFÄLLE VALUE'
		)
),
------------
------------ INVENTORY
------------
inventory_overview AS (
	SELECT
		'INVENTORY' AS section_name,
		region,
		CASE
			WHEN kpi_name = 'IN STOCK (SF)'
				THEN 'IN STOCK'
			WHEN kpi_name = 'STOCK ON HAND'
				THEN kpi_name
			WHEN kpi_name = 'REPLENISHMENT (SF)'
				THEN 'REPLENISHMENT'
			WHEN kpi_name IN ('IN STOCK (UPS)', 'IN STOCK (SYN)')
				THEN 'STOCK IN ' || UPPER(warehouse)
		END AS kpi_name,
		reporting_date,
		SUM(stock_level) AS kpi_value
	FROM dm_operations.inventory_overview
	WHERE
		kpi_name IN (
			'IN STOCK (SF)',
			'STOCK ON HAND',
			'REPLENISHMENT (SF)',
			'IN STOCK (UPS)',
			'IN STOCK (SYN)'
		)
		AND date_part(dow, reporting_date) = 1 --monday
	GROUP BY 1, 2, 3, 4
),
stock_reconcilation_us AS (
	SELECT
		'INVENTORY' AS section_name,
		'US' AS region,
		'RECONCILATION' AS kpi_name,
		report_date AS reporting_date,
		COUNT(1) AS kpi_value
	FROM ods_external.us_inventory
	WHERE
		asset_id IS NULL
		AND date_part(dow, report_date) = 1 --monday
	GROUP BY 1, 2, 3, 4
),
------------
------------ QUALITY ASSURANCE
------------
reconcilation_ups AS (
	SELECT
		'QUALITY ASSURANCE' AS section_name,
		'EU' AS region,
		'RECONCILATION (UPS)' AS kpi_name,
		report_date AS reporting_date,
		COUNT(1) AS kpi_value
	FROM dm_operations.ups_eu_reconciliation
	WHERE
		asset_id IS NULL
		AND date_part(dow, report_date) = 1 --monday
	GROUP BY 1, 2, 3, 4
),
lost_assets AS (
	SELECT
		'QUALITY ASSURANCE' AS section_name,
		'EU' AS region,
		'LOST ASSETS' AS kpi_name,
		date_trunc('week', lost_date :: TIMESTAMP) AS reporting_date,
		COUNT(
			CASE
				WHEN asset_status_original = 'LOST'
					THEN serial_number
			END
		) AS kpi_value
	FROM dm_operations.lost_assets_and_compensation
	GROUP BY 1, 2, 3, 4
),
lost_assets_rate AS
(
	SELECT
		'QUALITY ASSURANCE' :: text AS section_name,
		'EU' :: text AS region,
		'LOST ASSET RATE' :: text AS kpi_name,
		d.reporting_date,
		ROUND(
			SUM(la.kpi_value) OVER (
				ORDER BY d.reporting_date ROWS unbounded preceding
			) :: FLOAT * 100 / SUM(a.assets_shipped) OVER (
				ORDER BY d.reporting_date ROWS unbounded preceding
			) :: FLOAT,
			2
		) AS kpi_value
	FROM dates d
	LEFT JOIN lost_assets la
		ON d.reporting_date = la.reporting_date
	LEFT JOIN (
			SELECT
				fact_date,
				assets_shipped
			FROM dwh.shipment_reporting
			WHERE
				store_short = 'EU'
				AND is_recirculated = 'Total'
		) a
		ON d.reporting_date = a.fact_date
),
return_damaged AS (
	SELECT
		date_trunc('week', return_delivery_date :: TIMESTAMP) AS reporting_date,
		COUNT(1) AS kpi_value
	FROM ods_production.allocation
	WHERE
		return_delivery_date :: TIMESTAMP IS NOT NULL
		AND issue_reason = 'Asset Damage'
	GROUP BY 1
),
return_delivered AS (
	SELECT
		date_trunc(
			'week',
			COALESCE (
				return_delivery_date :: TIMESTAMP,
				return_delivery_date_old :: TIMESTAMP
			)
		) reporting_date,
		COUNT(1) return_delivered
	FROM ods_operations.allocation_shipment
	WHERE
		shipping_country NOT IN ('United States')
	GROUP BY 1
),
damaged_assets_rate AS
(
	SELECT
		'QUALITY ASSURANCE' :: text AS section_name,
		'EU' :: text AS region,
		'DAMAGED ASSET RATE' :: text AS kpi_name,
		d.reporting_date,
		ROUND(
			SUM(rd.kpi_value) OVER (
				ORDER BY d.reporting_date ROWS unbounded preceding
			) :: FLOAT * 100 / SUM(r.return_delivered) OVER (
				ORDER BY d.reporting_date ROWS unbounded preceding
			) :: FLOAT,
			2
		) AS kpi_value
	FROM dates d
	LEFT JOIN return_damaged rd
		ON d.reporting_date = rd.reporting_date
	LEFT JOIN return_delivered r
		ON d.reporting_date = r.reporting_date
),
------------
------------ OUTBOUND
------------
outbound_historical AS (
	SELECT
		a."date",
		a.allocation_id,
		oa.order_id,
		oa.region,
		CASE
			WHEN oa.warehouse = 'synerlogis_de'
				THEN ' (SYN)'
			WHEN oa.warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN oa.warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN oa.warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS warehouse,
		a.store_short,
		a.allocation_status_original,
		a.shipment_label_created_at :: TIMESTAMP,
		a.ready_to_ship_at :: TIMESTAMP,
		a.push_to_wh_at :: TIMESTAMP
	FROM MASTER.allocation_historical a
	LEFT JOIN ods_operations.allocation_shipment oa
		ON oa.allocation_id = a.allocation_id
	WHERE
		a."date" >= dateadd ('day', -365, current_date)
		AND a.asset_id IS NOT NULL
		AND a.store_short NOT IN ('Partners Online', 'Partners Offline')
		AND oa.warehouse IN (
			'synerlogis_de',
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
),
outbound_pre AS (
	SELECT
		ob_shipment_unique AS shipment_unique,
		s.carrier,
		s.shipment_service,
		s.shipping_country,
		s.region,
		s.customer_type,
		s.warehouse,
		s.warehouse_detail,
		s.order_id,
		s.asset_id,
		s.allocation_id,
		s.allocation_status_original,
		MIN(s.allocated_at) AS allocated_at,
		MIN(s.shipment_label_created_at) AS shipment_label_created_at,
		MIN(s.shipment_at) AS shipment_at,
		MIN(s.delivered_at) AS delivered_at,
		MIN(s.ready_to_ship_at) AS ready_to_ship_at
	FROM ods_operations.allocation_shipment AS s
	WHERE
		s.outbound_bucket NOT IN (
			'Partners Offline',
			'Cancelled Before Shipment'
		)
		AND s.warehouse IN (
			'synerlogis_de',
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),
outbound_ods AS (
	SELECT
		order_id,
		allocation_id,
		warehouse,
		shipping_country,
		allocated_at,
		asset_id,
		warehouse_detail,
		allocation_status_original,
		ready_to_ship_at,
		shipment_label_created_at,
		shipment_at,
		delivered_at,
		region,
		customer_type
	FROM outbound_pre
	WHERE
		asset_id IS NOT NULL
),
ready_to_ship AS (
	SELECT
		'OUTBOUND' AS section_name,
		region,
		'ORDERS PUSHED' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS kpi_name,
		date_trunc(
			'week',
			convert_timezone('Europe/Berlin', ready_to_ship_at :: TIMESTAMP)
		) AS reporting_date,
		COUNT(DISTINCT order_id) AS kpi_value
	FROM outbound_ods
	WHERE
		ready_to_ship_at :: TIMESTAMP IS NOT NULL
		AND warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4
),
aged_orders AS (
	SELECT
		'OUTBOUND' AS section_name,
		region,
		'AGING ORDERS >2 DAYS' || warehouse AS kpi_name,
		--we take sunday, almost the snapshot early monday
		--so add 1 day to shift week number
		date_trunc('week', dateadd('day', 1, DATE)) AS reporting_date,
		COUNT(DISTINCT order_id) AS kpi_value
	FROM outbound_historical
	WHERE
		date_part(dayofweek, "date") = 0 --take sunday, since it
		AND allocation_status_original = 'READY TO SHIP'
		AND shipment_label_created_at :: TIMESTAMP IS NULL
		AND datediff(
			DAY,
			convert_timezone('Europe/Berlin', ready_to_ship_at :: TIMESTAMP),
			DATE
		) > 2
	GROUP BY 1, 2, 3, 4
),
assets_shipped_all AS (
	SELECT
		'OUTBOUND' AS section_name,
		region,
		'SHIPPED ASSETS' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS kpi_name,
		date_trunc(
			'week',
			convert_timezone(
				'Europe/Berlin',
				shipment_label_created_at :: TIMESTAMP
			)
		) AS reporting_date,
		COUNT(allocation_id) AS kpi_value
	FROM outbound_ods
	WHERE
		shipment_label_created_at :: TIMESTAMP IS NOT NULL
		AND warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4
),
assets_shipped_all_b2b_b2c AS (
	SELECT
		'SUMMARY' AS section_name,
		region,
		'SHIPPED ASSETS' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
			END || 
				CASE WHEN customer_type = 'normal_customer' THEN ' - B2C'
			 		 WHEN customer_type = 'business_customer' THEN ' - B2B'
					 END
			AS kpi_name,
		date_trunc(
			'week',
			convert_timezone(
				'Europe/Berlin',
				shipment_label_created_at :: TIMESTAMP
			)
		) AS reporting_date,
		COUNT(allocation_id) AS kpi_value
	FROM outbound_ods
	WHERE
		shipment_label_created_at :: TIMESTAMP IS NOT NULL
		AND warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4
),
assets_shipped_countries AS (
	SELECT
		'OUTBOUND' AS section_name,
		region,
		'SHIPPED TO ' || CASE
			WHEN shipping_country = 'Germany'
				THEN 'DE'
			WHEN shipping_country = 'Austria'
				THEN 'AT'
			WHEN shipping_country = 'Netherlands'
				THEN 'NL'
			WHEN shipping_country = 'Spain'
				THEN 'ES'
			ELSE ''
		END AS kpi_name,
		date_trunc(
			'week',
			convert_timezone(
				'Europe/Berlin',
				shipment_label_created_at :: TIMESTAMP
			)
		) AS reporting_date,
		COUNT(allocation_id) AS kpi_value
	FROM outbound_ods
	WHERE
		shipment_label_created_at :: TIMESTAMP IS NOT NULL
		AND shipping_country IN ('Germany', 'Netherlands', 'Austria', 'Spain')
	GROUP BY 1, 2, 3, 4
),
warehouse_lt AS (
	SELECT
		'OUTBOUND' AS section_name,
		region,
		'WAREHOUSE LT' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS kpi_name,
		-- Days to Process WH @ Tableau
		date_trunc('week', delivered_at :: TIMESTAMP) AS reporting_date,
		ROUND(AVG(warehouse_lt :: FLOAT), 2) AS kpi_value
	FROM ods_operations.allocation_shipment
	WHERE
		warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
		AND delivered_at IS NOT NULL
		AND shipment_at IS NOT NULL
		AND store <> 'Partners Offline'
	GROUP BY 1, 2, 3, 4
),
------------
------------ TRANSPORTATION
------------
delivery_times AS (
	SELECT
		'TRANSPORTATION' AS section_name,
		CASE
			WHEN store_short = 'US'
				THEN 'US'
			ELSE 'EU'
		END AS region,
		CASE
			WHEN store_short IN ('EU', 'US')
				THEN 'NET OPS CYCLE'
			WHEN store_short IN ('Grover', 'Austria', 'Netherlands', 'Spain')
				THEN 'NET OPS CYCLE ' || CASE
				WHEN store_short = 'Grover'
					THEN '(DE)'
				WHEN store_short = 'Netherlands'
					THEN '(NL)'
				WHEN store_short = 'Spain'
					THEN '(ES)'
				WHEN store_short = 'Austria'
					THEN '(AT)'
			END
		END AS kpi_name,
		fact_date AS reporting_date,
		total_delivery_time_excl_weekend AS kpi_value
	FROM dwh.shipment_reporting
	WHERE
		is_recirculated = 'Total'
		AND store_short IN (
			'Grover',
			'Austria',
			'Netherlands',
			'Spain',
			'EU',
			'US'
		)
	UNION ALL
	SELECT
		'TRANSPORTATION' AS section_name,
		CASE
			WHEN store_short = 'US'
				THEN 'US'
			ELSE 'EU'
		END AS region,
		CASE
			WHEN store_short IN ('EU', 'US')
				THEN 'TIME TO DELIVER'
			WHEN store_short IN ('Grover', 'Austria', 'Netherlands', 'Spain')
				THEN 'TIME TO DELIVER ' || CASE
				WHEN store_short = 'Grover'
					THEN '(DE)'
				WHEN store_short = 'Netherlands'
					THEN '(NL)'
				WHEN store_short = 'Spain'
					THEN '(ES)'
				WHEN store_short = 'Austria'
					THEN '(AT)'
			END
		END AS kpi_name,
		fact_date AS reporting_date,
		--round to half decimals 0.5 , 1 , 1.5 , and so on
		ROUND(time_to_deliver_excl_weekend :: FLOAT / 24 * 2) / 2 AS kpi_value
	FROM dwh.shipment_reporting
	WHERE
		is_recirculated = 'Total'
		AND store_short IN (
			'Grover',
			'Austria',
			'Netherlands',
			'Spain',
			'EU',
			'US'
		)
),
reverse_delivery_time AS (
	SELECT 
		'TRANSPORTATION' AS section_name,
		CASE WHEN ash.shipping_country = 'United States' THEN 'US' ELSE 'EU' END AS region,
		'REVERSE DELIVERY TIME' AS kpi_name,
		DATE_TRUNC('week',a.return_delivery_date)::date AS reporting_date,
		round(AVG(greatest (datediff('hour', a.return_shipment_at, a.return_delivery_date ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= a.return_shipment_at and d.datum < a.return_delivery_date
					  and d.week_day_number in (6, 7)) * 24
			, 0)::float / 24),1) as kpi_value
	FROM ods_production.allocation a
	LEFT JOIN ods_operations.allocation_shipment ash 
		ON a.allocation_id = ash.allocation_id
	WHERE a.return_shipment_at IS NOT NULL 
		AND a.return_shipment_at <> a.return_delivery_date --DELETE the cases WHERE we ONLY have 1 date, so BOTH would be the same.
		AND DATE_TRUNC('week',a.return_delivery_date) > dateadd('week',-4,date_trunc('week', current_date))
		AND ash.customer_type IS NOT NULL
		AND ash.store <> 'Partners Offline'
		AND ash.shipping_country IS NOT NULL
	GROUP BY 1,2,3,4
),
failed_deliveries AS (
	SELECT
		'TRANSPORTATION' AS section_name,
		CASE
			WHEN shipping_country = 'United States'
				THEN 'US'
			ELSE 'EU'
		END AS region,
		'FAILED DELIVERY RATE' kpi_name,
		date_trunc('week', fact_date) AS reporting_date,
		CASE
			WHEN SUM(total_shipments) = 0
				THEN 0
			ELSE ROUND(
				SUM(total_failed_deliveries) :: FLOAT * 100 / SUM(total_shipments) :: FLOAT,
				2
			)
		END AS kpi_value
	FROM dm_operations.failed_deliveries
	WHERE COALESCE(failed_reason, ' ') <> 'Customer Refused'
	GROUP BY 1, 2, 3, 4
),
shipping_costs AS (
	SELECT
		'TRANSPORTATION' AS section_name,
		'EU' AS region,
		CASE
			WHEN shipment_type = 'inbound'
				THEN ' (IB)'
			ELSE ' (OB)'
		END kpi_name,
		fact_date AS reporting_date,
		ROUND(SUM(total_price_paid) / SUM(total_best_price), 2) AS pricing_efficiency,
		ROUND(
			SUM(total_price_paid) / SUM(total_allocations),
			2
		) AS avg_price
	FROM dm_operations.shipping_costs_comparison
	GROUP BY 1, 2, 3, 4
),
nps_after_receive_return AS (
	SELECT
		'TRANSPORTATION' AS section_name,
		CASE
			WHEN shipping_country = 'United States'
				THEN 'US'
			ELSE 'EU'
		END AS region,
		'NPS ' || poll_bucket || ', ' || carrier AS kpi_name,
		fact_date AS reporting_date,
		ROUND(
			(
				SUM(
					CASE
						WHEN vote_slug IN ('8', '9', '10')
							THEN total_votes
					END
				) :: FLOAT - SUM(
					CASE
						WHEN vote_slug IN ('0', '1', '2', '3', '4', '5', '6')
							THEN total_votes
					END
				) :: FLOAT
			) / SUM(total_votes) :: FLOAT * 100,
			1
		) AS kpi_value
	FROM dm_operations.hats_after_return_receive
	WHERE
		poll_slug IN (
			'likelihood-recommend',
			'after-return-likelihood-recommend'
		)
		AND vote_slug NOT IN ('7', '8')
		AND carrier IN ('DHL', 'UPS', 'HERMES')
	GROUP BY 1, 2, 3, 4
),
------------
------------ REVERSE
------------
ingram_inventory AS (
	SELECT
		'REVERSE' AS section_name,
		'EU' AS region,
		UPPER(main_bucket) AS kpi_name,
		reporting_date,
		SUM(total_assets) AS kpi_value
	FROM dm_operations.ingram_inventory_buckets
	WHERE
		reporting_date > current_date - 35
		AND date_part(dow, reporting_date) = 1 --monday
	GROUP BY 1, 2, 3, 4
),
reverse_net_ops_cycle_prep AS (
	SELECT DISTINCT 
		'REVERSE' AS section_name,
		CASE WHEN ash.shipping_country = 'United States' THEN 'US' ELSE 'EU' END AS region,
		a.allocation_id,
		CASE WHEN ah.asset_id IS NOT NULL THEN 1 ELSE 0 END is_asset_in_repair,		
        		- (select count(1) from public.dim_dates d
					  and d.week_day_number in (6, 7)) * 24
			, 0)::float / 24 as reverse_net_ops_cycle,			
		row_number() OVER (PARTITION BY i.order_number ORDER BY a.return_shipment_at DESC) AS rn,
	FROM ods_production.allocation a
	LEFT JOIN ods_operations.ingram_micro_orders i
		ON a.serial_number = i.serial_number
	LEFT JOIN master.asset_historical ah 
		ON a.asset_id = ah.asset_id
		AND ah.asset_status_original IN ('LOCKED DEVICE','IN REPAIR') --checking assets that were/ARE IN repair
	LEFT JOIN ods_operations.allocation_shipment ash 
		ON a.allocation_id = ash.allocation_id
	WHERE a.return_shipment_at IS NOT NULL 	
			-- since our cohort IS WHEN the customer DROP the asset AND IF the asset will be IN repair
			--it will be for many months, so, for the reverse, it makes sense to have it longer, otherwise, we will exclude many cases,
			-- since we only show the cases where assets are already available to be rented again
		AND ash.customer_type IS NOT NULL
		AND ash.store <> 'Partners Offline'
		AND ash.shipping_country IS NOT NULL
)
, reverse_net_ops_cycle AS (
	SELECT 
		section_name,
		region,
		'REVERSE NET OPS CYCLE' AS kpi_name,
		reporting_date,
		round(AVG(reverse_net_ops_cycle),1) AS kpi_value
	FROM reverse_net_ops_cycle_prep
	WHERE rn = 1 AND rn_2 = 1
	GROUP BY 1,2,3,4
	UNION ALL 
	SELECT 
		section_name,
		region,
		'REVERSE HAPPY PATH' AS kpi_name,
		reporting_date,
		round(AVG(reverse_net_ops_cycle),1) AS kpi_value
	FROM reverse_net_ops_cycle_prep
	WHERE rn = 1 AND rn_2 = 1
		AND is_asset_in_repair = 0
	GROUP BY 1,2,3,4
),
------------
------------ SUMMARY
------------
----- Returned not scanned kpi
al_histo_returned_not_scanned AS (
    SELECT
        DATE,
        allocation_id,
        asset_id,
        allocation_status_original,
        return_delivery_date,
        is_last_allocation_per_asset,
        shipping_country
    FROM MASTER.allocation_historical
    WHERE
        DATE >= dateadd('week',-4,date_trunc('week', current_date))
        AND date_part('weekday', DATE) = 1 -- ONLY monday
),
as_histo_returned_not_scanned  AS (
    SELECT
        DATE,
        asset_id,
        serial_number,
        asset_status_original
    FROM MASTER.asset_historical
    WHERE
        DATE >= dateadd('week',-4,date_trunc('week', current_date))
        AND date_part('weekday', DATE) = 1 -- ONLY monday
),
im_events_returned_not_scanned AS (
    SELECT
        serial_number,
        source_timestamp AS first_event
    FROM recommerce.ingram_micro_send_order_grading_status im
    WHERE
        im.serial_number IN (
            SELECT
                serial_number
            FROM as_histo_returned_not_scanned
        )
)
, historical_returned_not_scanned  AS (
    SELECT
        a.date,
        a.shipping_country,
        a.allocation_id,
        a.allocation_status_original,
        a.return_delivery_date,
        a.is_last_allocation_per_asset,
        t.asset_status_original,
        (
            SELECT
                MIN(i.first_event)
            FROM im_events_returned_not_scanned i
            WHERE
                i.serial_number = t.serial_number
                AND i.first_event > a.return_delivery_date
        ) first_im_event
    FROM al_histo_returned_not_scanned  a
    LEFT JOIN as_histo_returned_not_scanned  t
        ON a.date = t.date
        AND a.asset_id = t.asset_id
)
, counts_returned_not_scanned AS (
    SELECT
    	'SUMMARY' AS section_name,
		CASE WHEN shipping_country IN ('United States of America', 'United States')
			THEN 'US' ELSE 'EU' END AS region,
		'RETURNED BUT NOT SCANNED' AS kpi_name,
		date_trunc('week',date) AS reporting_date,
        COUNT(
            CASE
                WHEN return_delivery_date IS NOT NULL
                AND allocation_status_original = 'IN TRANSIT'
                AND asset_status_original = 'ON LOAN'
                AND (
                    first_im_event IS NULL --not scanned yet
                    OR (
                        first_im_event > return_delivery_date
                        AND first_im_event > DATE
                    ) --scanned later , as of snapshot date it was not scanned
                )
                AND is_last_allocation_per_asset
                    THEN allocation_id
            END
        ) kpi_value
    FROM historical_returned_not_scanned
    GROUP BY 1,2,3,4
),
delivery_on_time AS (
	SELECT 
		'SUMMARY' AS section_name,
		CASE WHEN shipping_country IN ('United States of America', 'United States')
			THEN 'US' ELSE 'EU' END AS region,
		'DELIVERY ON TIME' AS kpi_name,
		date_trunc('week',fact_date) AS reporting_date,
		COUNT(DISTINCT CASE WHEN is_promised_delivery_time_fullfillment IS TRUE THEN tracking_number END)::float /
			count(DISTINCT tracking_number)::float AS kpi_value
    FROM dm_operations.delivery_times
    WHERE fact_date >= dateadd('week',-4,date_trunc('week', current_date))
    GROUP BY 1,2,3,4
),
------------
------------ UNION
------------
union_all AS (
	SELECT 
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM reverse_net_ops_cycle 
	UNION ALL
	SELECT 
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM reverse_delivery_time
	UNION ALL 
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM dm_klarfalle
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM pending_allocation
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM delivery_schedule
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM rejected_deliveries
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM assets_booked
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM inb_klarfalle
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM inventory_overview
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM stock_reconcilation_us
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM reconcilation_ups
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM lost_assets
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM lost_assets_rate
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM damaged_assets_rate
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM ready_to_ship
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM aged_orders
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM assets_shipped_all
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM assets_shipped_all_b2b_b2c
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM assets_shipped_countries
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM warehouse_lt
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM delivery_times
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM failed_deliveries
	UNION ALL
	SELECT
		section_name,
		region,
		'SHIPPING PRICE EFFICIENCY' || kpi_name,
		reporting_date,
		pricing_efficiency AS kpi_value,
		0 AS is_increase_good
	FROM shipping_costs
	UNION ALL
	SELECT
		section_name,
		region,
		'AVG. SHIPPING PRICE' || kpi_name,
		reporting_date,
		avg_price AS kpi_value,
		0 AS is_increase_good
	FROM shipping_costs
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM nps_after_receive_return
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM ingram_inventory
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS is_increase_good
	FROM counts_returned_not_scanned
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS is_increase_good
	FROM delivery_on_time
) -----------------------------
------END OF CTE-------------
-----------------------------
SELECT
	w.section_name,
	w.region,
	w.kpi_name,
	--case when f.kpi_value is not null then '*' else '' end is_forecast,
	'' AS is_forecast,
	w.reporting_date,
	w.kpi_value,
	LAG(kpi_value) OVER (
		PARTITION by region,
		kpi_name
		ORDER BY reporting_date ASC
	) AS prev_week_value,
	w.is_increase_good
FROM union_all w --join to get forecasted values
	--left join dm_operations.weekly_forecasts f 
	--on w.kpi_name = f.kpi_name and w.region = f.region
	--and case when reporting_date = date_trunc('week', current_date) then true end = true
;

GRANT
SELECT
	ON dm_operations.supply_chain_weekly TO tableau;

