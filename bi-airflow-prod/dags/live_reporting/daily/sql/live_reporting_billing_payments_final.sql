DROP TABLE IF EXISTS tmp_live_reporting_billing_payments_final;
CREATE TEMP TABLE tmp_live_reporting_billing_payments_final AS 
WITH temp AS
(
SELECT s.*, kafka_received_at AS event_timestamp
FROM stg_curated.stg_internal_billing_payments s
WHERE  is_valid_json(payload)

)

, billing_payments_grouping AS
	(
	SELECT
	DISTINCT *
	FROM temp
	)
	,numbers AS
	(
  	SELECT
	*
	FROM public.numbers
  	WHERE ordinal < 20
	)
	,order_payments AS
	(
	SELECT
		*
		,JSON_EXTRACT_PATH_TEXT(payload,'contract_type') AS rental_mode
		,JSON_EXTRACT_PATH_TEXT(payload,'line_items') AS line_item_payload
		,JSON_ARRAY_LENGTH(JSON_EXTRACT_PATH_TEXT(payload,'line_items'),true) AS total_order_items
		,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'line_items'),0),'order_number') AS order_number
		,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'payment_method'),'type') AS payment_method_type
		,COUNT(*) OVER (PARTITION BY order_number) AS total_events
		,ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp ASC) AS rank_event
		,CASE WHEN total_events = rank_event THEN true ELSE false END AS is_last_event
		,FIRST_VALUE(event_timestamp) OVER (PARTITION BY order_number ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_charge_date
	FROM billing_payments_grouping
--	where rental_mode = 'FLEX'
	)
	,dates as
	(
	SELECT 
		order_number,
		min(CASE WHEN event_name = 'paid' THEN event_timestamp END ) AS paid_date,
		max(CASE WHEN event_name = 'failed' THEN event_timestamp END ) AS failed_date,
		max(CASE WHEN event_name = 'refund' THEN event_timestamp END ) AS refund_date
	FROM order_payments
	GROUP BY 1
	)
	,line_items_extract AS
	(
	SELECT
		op.*,
        numbers.ordinal AS line_item_no,
		json_extract_array_element_text(line_item_payload,numbers.ordinal::INT, true) AS line_item
	FROM order_payments op
	CROSS JOIN numbers
	WHERE numbers.ordinal < total_order_items
	)
	,order_prices AS
	(
	SELECT
		*,
		JSON_EXTRACT_PATH_text(line_item,'quantity') AS line_item_quantity,
		nullif((JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(line_item,'base_price'),'in_cents')),'')/100.0 AS base_price,
		nullif((JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(line_item,'discount'),'in_cents')),'')/100.0 AS discount,
		nullif((JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(line_item,'price'),'in_cents')),'')/100.0 AS price,
		nullif((JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(line_item,'total'),'in_cents')),'')/100.0  AS total_price
	FROM line_items_extract
	)
	,paid_value AS
	(
	SELECT
		order_number,
		event_name,
		is_last_event,
		sum(line_item_quantity) AS line_item_quantity_total,
		sum(base_price)AS base_price,
		sum(discount) AS discount,
		sum(price) AS price,
		sum(total_price) AS total_price
		FROM order_prices
	WHERE is_last_event
	GROUP BY 1,2,3
	)
	SELECT
		op.order_number,
		op.event_name,
		payment_method_type,
		total_order_items,
		rental_mode,
		line_item_payload,
		line_item_quantity_total,
		base_price,
		discount,
		price,
		total_price,
		da.paid_date  AS paid_date,
		first_charge_date,
		da.failed_date AS failed_date
	FROM order_payments op
	LEFT JOIN paid_value pv
			ON op.order_number = pv.order_number AND op.event_name = pv.event_name
	LEFT JOIN dates da
		ON op.order_number = da.order_number
	WHERE op.is_last_event
	;

BEGIN TRANSACTION;

DELETE FROM live_reporting.billing_payments_final WHERE 1=1;

INSERT INTO live_reporting.billing_payments_final
SELECT * FROM tmp_live_reporting_billing_payments_final;

END TRANSACTION;