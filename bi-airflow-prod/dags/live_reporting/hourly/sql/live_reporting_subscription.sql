							
DROP TABLE IF EXISTS tmp_live_reporting_subscription;
CREATE TEMP TABLE tmp_live_reporting_subscription AS 
WITH stg_kafka_v2 AS
	(
SELECT
		event_timestamp,
		event_name,
		version,
		payload,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'id'),'')AS id,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'type'),'')AS type,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'user_id'),'')AS user_id,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'order_number'),'')AS order_number,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'state'),'')AS state,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'created_at'),'')AS created_at,
		COUNT(*) OVER (PARTITION BY id) AS total_events,
		NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'goods'),'')AS goods,
		NULLIF(JSON_EXTRACT_PATH_TEXT((JSON_EXTRACT_ARRAY_ELEMENT_TEXT(goods,0)),'variant_sku'),'')AS variant_sku,
		NULLIF(CASE WHEN event_name = 'extended'
				THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'duration_terms'),'new'), 'committed_length')
				else JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'duration_terms'), 'committed_length') END,'') AS committed_length,
		ROW_NUMBER () OVER (PARTITION BY id ORDER BY event_timestamp DESC) AS idx
	FROM  stg_kafka_events_full.stream_customers_contracts_v2
	)
	,sub_dates AS
	(
	SELECT
		id AS subscription_id,
		MAX(CASE WHEN state = 'fulfilling' THEN event_timestamp::TIMESTAMP END) as start_date,
		min(CASE WHEN state = 'active' THEN event_timestamp::TIMESTAMP END) as active_date,
		max(CASE WHEN state = 'active' THEN event_timestamp::TIMESTAMP END) as reactivated_date,
		max(CASE WHEN state IN ('cancelled','ended','refused') THEN event_timestamp::TIMESTAMP END) AS cancellation_date,
		max(CASE WHEN state = 'paused' THEN event_timestamp::TIMESTAMP END) AS paused_date
	FROM stg_kafka_v2
  --where type ='flex'
	GROUP BY 1
	)
    ,idx AS (
	SELECT skv.*,
	ROW_NUMBER() OVER (PARTITION BY order_number,variant_sku ORDER BY created_at ASC) AS c_idx
	FROM stg_kafka_v2 skv
		LEFT JOIN  ods_production.contracts_deleted_in_source cds
		ON cds.contract_id = skv.id
	WHERE
		idx = 1
		AND cds.id IS NULL /*exclude contracts deleted in source system */
	)
	,last_event_pre AS
	(
	SELECT
		le.*
		--,is_pay_by_invoice
	FROM idx le
	LEFT JOIN live_reporting.ORDER o
	ON le.order_number = o.order_id
	--where (o.paid_date is not null
	--((is_pay_by_invoice is true and state = 'active') or
	-- (is_pay_by_invoice is true and state in('ended', 'cancelled'))  or
	-- (is_pay_by_invoice is true and state = 'paused'))
	--)
	)
	,res_agg
		AS
			(SELECT DISTINCT * FROM  stg_kafka_events_full.stream_inventory_reservation
			)
		,res
		AS (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY order_id,variant_sku ORDER BY updated_at ASC) AS idx
			,*
			FROM res_agg
		WHERE  ( order_mode = 'flex'
		AND ((initial_quantity::INT > quantity::INT AND status = 'paid')
		OR status = 'fulfilled')
			)
		)
		,last_event AS (
	SELECT l.*,
	CASE WHEN r.order_id IS NOT NULL THEN true ELSE false END is_fulfilled
	FROM last_event_pre l
	LEFT JOIN res r ON l.order_number = r.order_id AND l.c_idx = r.idx  AND l.variant_sku = r.variant_sku)

SELECT DISTINCT
	--store_id,
	s.order_number AS order_id,
	s.variant_sku AS variant_sku,
	CASE WHEN p.product_sku IS NOT NULL THEN p.product_sku ELSE split_part(s.variant_sku,'V',1) END AS product_sku,
CASE
		WHEN is_fulfilled IS true THEN 'ALLOCATED'
		WHEN aa.allocation_status_original IS NOT NULL THEN 'ALLOCATED'
		WHEN (active_date IS NOT NULL AND start_date < '2021-10-01') THEN 'ALLOCATED'
		ELSE 'PENDING ALLOCATION'
	END AS allocation_status,
	CASE
		WHEN active_date IS NULL AND reactivated_date IS NULL AND cancellation_date IS NOT NULL THEN 'CANCELLED'
		WHEN cancellation_date >= active_date AND reactivated_date IS NULL THEN 'CANCELLED'
		WHEN cancellation_date >= reactivated_date THEN 'CANCELLED'
		WHEN cancellation_date IS NULL THEN 'ACTIVE'
		WHEN active_date IS NULL AND reactivated_date IS NULL AND cancellation_date IS NULL THEN 'ACTIVE'
		ELSE 'ACTIVE' END AS status,
		start_date::TIMESTAMP WITHOUT TIME ZONE ,
		product_name,
		committed_length::DOUBLE PRECISION AS rental_period
	FROM
	last_event s
	LEFT JOIN sub_dates sd
	ON s.id = sd.subscription_id
	LEFT JOIN stg_kafka_events_full.allocation_us aa
    ON s.id = aa.subscription_id
	LEFT JOIN bi_ods.variant v 
	ON s.variant_sku = v.variant_sku
	LEFT JOIN bi_ods.product p
	ON p.product_id = v.product_id OR (v.product_id IS NULL AND split_part(s.variant_sku,'V',1) = p.product_sku)
   LEFT JOIN ods_production.new_infra_missing_history_months_required h
--> this is a static table and won't generate locks.
	on s.id =  h.contract_id
UNION ALL
--subs old infra
SELECT DISTINCT
   -- cast(o.store_id__c as VARCHAR) as store_id,
    o.spree_order_number__c AS order_id,
    i.f_product_sku_variant__c AS variant_sku,
   CASE WHEN p.product_sku IS NOT NULL THEN p.product_sku ELSE split_part(i.f_product_sku_variant__c,'V',1) END AS product_sku,
 -- cast(c.spree_customer_id__c as integer) as customer_id,
s.allocation_status__c AS allocation_status,
CASE WHEN s.date_cancellation__c IS NOT NULL THEN 'CANCELLED' ELSE 'ACTIVE' END AS status,
s.date_start__c::TIMESTAMP WITHOUT TIME ZONE  AS start_date,
product_name,
CASE WHEN COALESCE(i.minimum_term_months__c,0)::INT = 0 THEN 1 ELSE COALESCE(i.minimum_term_months__c,1) END AS rental_period
FROM stg_salesforce.subscription__c s
LEFT JOIN stg_salesforce.orderitem i
   ON s.order_product__c = i.id
 LEFT JOIN stg_salesforce.account c
   ON c.id = s.customer__c
 LEFT JOIN stg_salesforce.ORDER o
   ON s.order__c = o.id
  LEFT JOIN bi_ods.variant v 
   ON v.VARIANT_SKU=i.f_product_sku_variant__c
  LEFT JOIN bi_ods.product p
   ON p.product_id=v.product_id OR (v.product_id IS NULL AND split_part(i.f_product_sku_variant__c,'V',1) = p.product_sku)
 ;

BEGIN TRANSACTION;

DELETE FROM live_reporting.subscription WHERE 1=1;

INSERT INTO live_reporting.subscription
SELECT * FROM tmp_live_reporting_subscription;

END TRANSACTION;