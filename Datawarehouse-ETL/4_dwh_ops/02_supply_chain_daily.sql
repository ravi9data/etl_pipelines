DROP TABLE IF EXISTS dm_operations.supply_chain_daily;

CREATE TABLE dm_operations.supply_chain_daily AS WITH 
------------
------------ COMMON QUERIES
------------
dates AS (
	SELECT
		dateadd('day', - ordinal, current_date) :: DATE reporting_date
	FROM public.numbers
	WHERE
		reporting_date >= date_trunc('year', current_date)
),
outbound_historical AS (
	SELECT
		a."date",
		oa.order_id,
		oa.warehouse,
		a.allocation_status_original,
		a.shipment_label_created_at :: TIMESTAMP,
		a.ready_to_ship_at :: TIMESTAMP,
		a.push_to_wh_at :: TIMESTAMP,
		oa.region
	FROM master.allocation_historical a
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
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),
outbound AS (
	SELECT
		order_id,
		warehouse,
		asset_id,
		warehouse_detail,
		allocation_status_original,
		ready_to_ship_at,
		ready_to_ship_at AS push_to_wh_at,
		shipment_label_created_at,
		shipment_at,
		delivered_at,
		region
	FROM outbound_pre
	WHERE
		asset_id IS NOT NULL
),
------------
------------ DELIVERY MANAGEMENT
------------
	SELECT
		'delivery_mgmt' AS section_name,
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
			datum < current_date
			AND hours = 13
		) --let's take middle of the day, 
		--orders should be reviewed and allocated until 13
		OR (
			datum = current_date
			AND hours = (
				SELECT
					LEAST(MAX(hours), 13)
				FROM ods_production.inventory_reservation_pending_historical
				WHERE
					datum = current_date
			)
		)
	GROUP BY 1, 2, 3, 4
), dm_klarfalle AS (
	SELECT
		'delivery_mgmt' AS section_name,
		'EU' AS region,
		CASE
			WHEN case_status = 'Damaged'
				THEN 'KLÄRFÄLLE DAMAGED ASSETS'
			WHEN case_status = 'Missing Quantity'
				THEN 'KLÄRFÄLLE MISSING QUANTITIES'
			WHEN case_status IN ('No PPW', 'PO not in PPW', 'PO wrong in PPW')
				THEN 'KLÄRFÄLLE NO/WRONG PPW'
		END AS kpi_name,
		d.reporting_date,
		SUM(j.qty) AS kpi_value
	FROM dates d,
		stg_external_apis.guil_jira_processed j
	WHERE
		department = 'Delivery Mgmt'
		AND date_trunc('day', j.created) <= d.reporting_date
		AND (
			solved_at IS NULL
			OR date_trunc('day', j.solved_at) > d.reporting_date
		)
	GROUP BY 1, 2, 3, 4
),
delivery_schedule AS (
	SELECT
		'delivery_mgmt' AS section_name,
		'EU' AS region,
		'DELIVERY SCHEDULE' AS kpi_name,
		reporting_date,
		SUM(weekly_quantity) AS kpi_value
	FROM stg_external_apis.sc_eu_inbound_schedule
	WHERE
		warehouse = 'Roermond'
	GROUP BY 1, 2, 3, 4
),
------------
------------ INBOUND
------------
ib_klarfalle AS (
	SELECT
		'inbound' AS section_name,
		'EU' AS region,
		d.reporting_date,
		SUM(j.qty) AS total_klarfalle,
		ROUND(SUM(purchase_price * qty) / 1000, 2) AS unresolved_klarfalle_value,
		COALESCE(
			SUM(
				CASE
					WHEN purchase_price IS NULL
					OR purchase_price = 0
						THEN qty
				END
			),
			0
		) AS unresolved_klarfalle_pending
	FROM dates d,
		stg_external_apis.guil_jira_processed j
	WHERE
		department = 'Inbound'
		AND date_trunc('day', j.created) <= d.reporting_date
		AND (
			solved_at IS NULL
			OR date_trunc('day', j.solved_at) > d.reporting_date
		)
	GROUP BY 1, 2, 3
),
ib_pending_ups AS (
	SELECT
		'inbound' AS section_name,
		'EU' AS region,
		'PENDING KLÄRFÄLLE UPS' AS kpi_name,
		d.reporting_date,
		SUM(j.qty) kpi_value
	FROM dates d,
		stg_external_apis.guil_jira_processed j
	WHERE
		department = 'Inbound'
		AND date_trunc('day', j.created) <= d.reporting_date --ignore further cases
		AND (
			solved_at IS NULL
			OR date_trunc('day', j.solved_at) > d.reporting_date
		) --still unresolved, or solved later
		AND date_trunc('day', j.pending_ups) <= d.reporting_date --should be moved to ups before fact date
	GROUP BY 1, 2, 3, 4
),
assets_booked AS (
	SELECT
		'inbound' AS section_name,
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
		date_trunc('day', created_at) AS reporting_date,
		SUM(booked_asset) AS kpi_value
	FROM dm_operations.assets_booked
	WHERE
		warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse'
		)
	GROUP BY 1, 2, 3, 4
), ------------
------------ INVENTORY
------------
inventory_overview AS (
	SELECT
		'inventory' AS section_name,
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
	GROUP BY 1, 2, 3, 4
),
sn_mismatch_fix AS (
	SELECT
		'inventory' AS section_name,
		'EU' AS region,
		'SN MISMATCH FIX' AS kpi_name,
		reporting_date,
		COUNT(DISTINCT serial_number) asset_count,
		ROUND(SUM(initial_price) / 1000, 1) asset_price
	FROM dm_operations.mismatch_fixes
	GROUP BY 1, 2, 3, 4
),
------------
------------ QUALITY ASSURANCE
------------
reconcilation_ups AS (
	SELECT
		'quality_assurance' AS section_name,
		'EU' AS region,
		'RECONCILATION (UPS)' AS kpi_name,
		report_date AS reporting_date,
		COUNT(1) AS kpi_value
	FROM dm_operations.ups_eu_reconciliation
	WHERE
		asset_id IS NULL
	GROUP BY 1, 2, 3, 4
),
lost_assets AS (
	SELECT
		date_trunc(
			'day',
			COALESCE(h.createddate :: TIMESTAMP, a.lost_date :: TIMESTAMP)
		) AS reporting_date,
		--lost_date in allocation table is based on lastmodifieddate, which does not always represent when asset is lost
		--first, use when asset status has changed to lost, if null (especially for those which is lost in wh) then use lost_date
		COUNT(1) AS kpi_value
	FROM ods_production.asset a
	LEFT JOIN (
			SELECT
				assetid,
				MAX(createddate :: TIMESTAMP) AS createddate
			FROM stg_salesforce.asset_history
			WHERE
				newvalue = 'LOST'
			GROUP BY 1
		) h
		ON h.assetid = a.asset_id
	WHERE
		a.asset_status_original = 'LOST'
	GROUP BY 1
),
lost_assets_rate AS
(
	SELECT
		'quality_assurance' AS section_name,
		'EU' AS region,
		'LOST ASSET RATE' kpi_name,
		d.reporting_date,
		ROUND(
			SUM(la.kpi_value) OVER (
				ORDER BY d.reporting_date ROWS unbounded preceding
			) :: FLOAT * 100 / SUM(sa.kpi_value) OVER (
				ORDER BY d.reporting_date ROWS unbounded preceding
			) :: FLOAT,
			2
		) AS kpi_value
	FROM dates d
	LEFT JOIN lost_assets la
		ON d.reporting_date = la.reporting_date
	LEFT JOIN (
			SELECT
				date_trunc(
					'day',
					convert_timezone(
						'Europe/Berlin',
						shipment_label_created_at :: TIMESTAMP
					)
				) AS reporting_date,
				COUNT(1) AS kpi_value
			FROM outbound
			WHERE
				region = 'EU'
				AND reporting_date >= date_trunc('year', current_date)
			GROUP BY 1
		) sa
		ON d.reporting_date = sa.reporting_date
),
return_damaged AS (
	SELECT
		date_trunc('day', return_delivery_date :: TIMESTAMP) AS reporting_date,
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
			'day',
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
		'quality_assurance' AS section_name,
		'EU' AS region,
		'DAMAGED ASSETS RATE' AS kpi_name,
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
	LEFT JOIN return_delivered r
		ON d.reporting_date = r.reporting_date
	LEFT JOIN return_damaged rd
		ON d.reporting_date = rd.reporting_date
),
------------
------------ OUTBOUND
------------
outbound_ods AS (
	SELECT
		s.order_completed_at :: TIMESTAMP,
		s.delivered_at :: TIMESTAMP,
		s.shipment_label_created_at :: TIMESTAMP,
		s.shipment_at :: TIMESTAMP,
		s.region,
		s.carrier_first_mile,
		s.is_delivered_on_time
	FROM ods_operations.allocation_shipment s
	WHERE
		s.allocated_at > current_date - 180
		AND s.store <> 'Partners Offline' --left join ods_production.allocation a on a.allocation_id = s.allocation_id 
),
ready_to_ship_orders_sf AS (
	SELECT
		'outbound' AS section_name,
		region,
		'ORDERS PUSHED' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS kpi_name,
		DATE AS reporting_date,
		COUNT(DISTINCT order_id) AS kpi_value
	FROM outbound_historical
	WHERE
		"date" = date_trunc(
			'day',
			convert_timezone('Europe/Berlin', push_to_wh_at :: TIMESTAMP)
		)
		AND warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4
),
shipped_assets_sf AS (
	SELECT
		'outbound' AS section_name,
		region,
		'SHIPPED ASSETS' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS kpi_name,
		warehouse_detail,
		date_trunc('day', COALESCE (shipment_at, delivered_at)) AS reporting_date,
		COUNT(asset_id) AS kpi_value
	FROM outbound
	WHERE
		warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4, 5
),
aged_more_2_days AS (
	SELECT
		'outbound' AS section_name,
		region,
		'AGING ORDERS >2 DAYS' || CASE
			WHEN warehouse = 'ups_softeon_eu_nlrng'
				THEN ' (UPS)'
			WHEN warehouse = 'ingram_micro_eu_flensburg'
				THEN ' (IM)'
			WHEN warehouse IN ('office_us', 'ups_softeon_us_kylse')
				THEN ''
		END AS kpi_name,
		DATE AS reporting_date,
		COUNT(DISTINCT order_id) AS kpi_value
	FROM outbound_historical
	WHERE
		allocation_status_original = 'READY TO SHIP'
		AND shipment_label_created_at :: TIMESTAMP IS NULL --and shipment_tracking_number is null 
		AND datediff(
			DAY,
			convert_timezone('Europe/Berlin', ready_to_ship_at :: TIMESTAMP),
			DATE
		) > 2 --dismiss today (0), yesterday (1) and the day before (2)
		AND warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4
), delivered_on_time AS (
	SELECT
		'outbound' AS section_name,
		region,
		'DELIVERED ON TIME' AS kpi_name,
		date_trunc('day', delivered_at :: TIMESTAMP) AS reporting_date,
		--exclude sundays, expected delivery time is 5
		ROUND(
			COUNT(
				CASE
					WHEN is_delivered_on_time
						THEN 1
				END
			) :: FLOAT / COUNT(1) :: FLOAT * 100,
			1
		) AS kpi_value
	FROM outbound_ods
	WHERE
		delivered_at IS NOT NULL
	GROUP BY 1, 2, 3, 4
), picking_time_by_carrier AS (
	SELECT
		'outbound' AS section_name,
		region,
		'PICKING TIME BY CARRIER' AS kpi_name,
		date_trunc(
			'day',
			convert_timezone('Europe/Berlin', shipment_at :: TIMESTAMP)
		) AS reporting_date,
		ROUND(AVG(carrier_first_mile), 2) kpi_value
	FROM outbound_ods
	WHERE
		carrier_first_mile IS NOT NULL
	GROUP BY 1, 2, 3, 4
),
packed_within_24_hours AS (
	SELECT
		'outbound' AS section_name,
		region,
		'PICKED WITHIN 24 HOURS' AS kpi_name,
		date_trunc(
			'day',
			convert_timezone('Europe/Berlin', shipment_at :: TIMESTAMP)
		) AS reporting_date,
		CASE
			WHEN COUNT(
				convert_timezone('Europe/Berlin', shipment_at :: TIMESTAMP)
			) = 0
				THEN 0 --avoid divide by zero
			ELSE ROUND(
				COUNT(
					CASE
						WHEN carrier_first_mile <= 24
							THEN 1
					END
				) :: FLOAT / COUNT(
					convert_timezone('Europe/Berlin', shipment_at :: TIMESTAMP)
				) :: FLOAT * 100,
				1
			)
		END AS kpi_value
	FROM outbound_ods
	WHERE
		carrier_first_mile IS NOT NULL
	GROUP BY 1, 2, 3, 4
),
------------
------------ TRANSPORTATION
------------
transportation_initial AS (
	SELECT
		a.warehouse,
		a.region,
		a.shipping_country AS country,
		a.shipment_at,
		a.first_delivery_attempt,
		a.delivered_at,
		a.warehouse_lt,
		a.net_ops_cycle,
		a.transit_time,
		a.delivery_time
	FROM ods_operations.allocation_shipment a
	WHERE
		a.warehouse IN (
			'synerlogis_de',
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
		AND a.store <> 'Partners Offline'
		AND a.allocated_at > current_date - 180
),
warehouse_lt AS (
	SELECT
		'outbound' AS section_name,
		-- was in TRANSPORTATION before, do not move upwards since this uses transportation_initial
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
		date_trunc('day', shipment_at) AS reporting_date,
		ROUND(AVG(warehouse_lt :: FLOAT), 2) AS kpi_value
	FROM transportation_initial
	WHERE
		warehouse IN (
			'ups_softeon_eu_nlrng',
			'office_us',
			'ups_softeon_us_kylse',
			'ingram_micro_eu_flensburg'
		)
	GROUP BY 1, 2, 3, 4
), avg_transit_time AS (
	SELECT
		'transportation' AS section_name,
		region,
		'AVG.TRANSIT TIME' || CASE
			WHEN region = 'EU'
				THEN CASE
				WHEN country = 'Germany'
					THEN ' (DE)'
				WHEN country = 'Austria'
					THEN ' (AT)'
				WHEN country = 'Netherlands'
					THEN ' (NL)'
				WHEN country = 'Spain'
					THEN ' (ES)'
				ELSE ' (OTH)'
			END
			ELSE ''
		END --for united states
		AS kpi_name,
		-- Median Days to Process Shipping @ Tableau
		date_trunc('day', delivered_at) AS reporting_date,
		ROUND(AVG(ROUND(transit_time :: FLOAT / 24 * 2) / 2), 1) AS kpi_value
	FROM transportation_initial
	GROUP BY 1, 2, 3, 4
),
med_net_ops_cycle AS (
	SELECT
		'transportation' AS section_name,
		region,
		'MED.NET OPS CYCLE' || CASE
			WHEN region = 'EU'
				THEN CASE
				WHEN country = 'Germany'
					THEN ' (DE)'
				WHEN country = 'Austria'
					THEN ' (AT)'
				WHEN country = 'Netherlands'
					THEN ' (NL)'
				WHEN country = 'Spain'
					THEN ' (ES)'
				ELSE ' (OTH)'
			END
			ELSE ''
		END --for united states 
		AS kpi_name,
		date_trunc('day', first_delivery_attempt) AS reporting_date,
		median(net_ops_cycle) AS kpi_value
	FROM transportation_initial
	GROUP BY 1, 2, 3, 4
),
------------
------------ REVERSE
------------
ingram_inventory AS (
	SELECT
		'reverse' AS section_name,
		'EU' AS region,
		UPPER(main_bucket) AS kpi_name,
		reporting_date,
		SUM(total_assets) AS kpi_value
	FROM dm_operations.ingram_inventory_buckets
	WHERE
		reporting_date > current_date - 61
	GROUP BY 1, 2, 3, 4
),
unioned AS (
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM dm_klarfalle
	WHERE reporting_date > '2023-08-25'
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM delivery_schedule
	UNION ALL
	SELECT
		section_name,
		region,
		'TOTAL KLÄRFÄLLE' AS kpi_name,
		reporting_date,
		total_klarfalle AS kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM ib_klarfalle
	WHERE reporting_date > '2023-08-25'
	UNION ALL
	SELECT
		section_name,
		region,
		'PENDING VALUE KLÄRFÄLLE (QTY)' AS kpi_name,
		reporting_date,
		unresolved_klarfalle_pending AS kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM ib_klarfalle
	WHERE reporting_date > '2023-08-25'
	UNION ALL
	SELECT
		section_name,
		region,
		'UNRESOLVED KLÄRFÄLLE VALUE' AS kpi_name,
		reporting_date,
		unresolved_klarfalle_value AS kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM ib_klarfalle
	WHERE reporting_date > '2023-08-25'
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM ib_pending_ups
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM assets_booked
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM inventory_overview
	UNION ALL
	SELECT
		section_name,
		region,
		'# ' || kpi_name,
		reporting_date,
		asset_count AS kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM sn_mismatch_fix
	UNION ALL
	SELECT
		section_name,
		region,
		'K€ ' || kpi_name,
		reporting_date,
		asset_price AS kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM sn_mismatch_fix
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		0 AS show_yesterday,
		0 AS is_increase_good
	FROM reconcilation_ups
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM lost_assets_rate
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM damaged_assets_rate
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM ready_to_ship_orders_sf
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM shipped_assets_sf
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM aged_more_2_days
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM delivered_on_time
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM picking_time_by_carrier
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		1 AS is_increase_good
	FROM packed_within_24_hours
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM warehouse_lt
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM avg_transit_time
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM med_net_ops_cycle
	UNION ALL
	SELECT
		section_name,
		region,
		kpi_name,
		reporting_date,
		kpi_value,
		1 AS show_yesterday,
		0 AS is_increase_good
	FROM ingram_inventory
) -----------------------------
------END OF CTE-------------
-----------------------------
SELECT
	section_name,
	region,
	kpi_name,
	reporting_date,
	kpi_value,
	CASE
		WHEN DENSE_RANK() OVER (
			ORDER BY reporting_date DESC
		) = 1 + show_yesterday
			THEN 'Main'
		WHEN DENSE_RANK() OVER (
			ORDER BY reporting_date DESC
		) = 2 + show_yesterday
			THEN 'Previous'
		ELSE NULL
	END AS scorecard_indicator,
	is_increase_good
FROM unioned
WHERE
	reporting_date NOT IN (
		SELECT
			holiday_date
		FROM dm_operations.holidays
	)
	AND date_part(dow, reporting_date) NOT IN (0, 6)
	AND reporting_date <= current_date;

GRANT
SELECT
	ON dm_operations.supply_chain_daily TO tableau;
