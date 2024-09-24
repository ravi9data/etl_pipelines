CREATE TEMP TABLE tmp_us_dc_customer_contact AS
WITH src AS (
SELECT a.*,
b.last_failed_reason ,
ROW_NUMBER()OVER(PARTITION BY a.subscription_id ORDER BY date DESC ) latest_entry
FROM debt_collection.us_dc_customer_contact a
LEFT JOIN ods_production.detailed_view_us_dc b
ON a.subscription_id =b.subscription_id 
)
SELECT
	subscription_id,
	"owner",
	"date",
	channel,
	responded,
	current_state,
	team_notes,
	last_failed_reason,
	follow_up_date
FROM src
WHERE latest_entry=1 ;

MERGE INTO dm_debt_collection.us_dc_customer_contact_retained
USING tmp_us_dc_customer_contact dcc
	ON dm_debt_collection.us_dc_customer_contact_retained.subscription_id = dcc.subscription_id
WHEN MATCHED THEN
UPDATE
	SET "date" = dcc."date",
	channel = dcc.channel,
	responded=dcc.responded,
	current_state=dcc.current_state,
	team_notes=dcc.team_notes,
	follow_up_date=dcc.follow_up_date,
    	last_failed_reason=dcc.last_failed_reason
WHEN NOT MATCHED THEN INSERT VALUES
	(dcc.subscription_id,
	dcc."owner",
	dcc."date",
	dcc.channel,
	dcc.responded,
	dcc.current_state,
	dcc.team_notes,
	dcc.follow_up_date,
    	dcc.last_failed_reason);

drop table if exists trans_dev.internal_billing_payments;
create table trans_dev.internal_billing_payments as
select
	s.*,
    consumed_at as event_timestamp
from stg_curated.stg_internal_billing_payments s
where is_valid_json(payload);

/* create last_payment_event extract */

drop table if exists ods_production.last_payment_event;
CREATE  TABLE ods_production.last_payment_event AS
WITH numbers AS(
    SELECT *
    FROM public.numbers
    WHERE ordinal < 20
),
last_event AS (
    SELECT JSON_EXTRACT_PATH_text(payload, 'uuid') AS uuid,
        RANK() OVER (
            PARTITION BY uuid
            ORDER BY event_timestamp DESC
        ) AS idx_,
        v2.*
    FROM trans_dev.internal_billing_payments v2 --change here
),
raw_tmp AS(
    SELECT JSON_EXTRACT_PATH_text(payload, 'uuid') AS uuid,
        JSON_EXTRACT_PATH_text(payload, 'type') AS type_,
        JSON_EXTRACT_PATH_text(payload, 'due_date')::date AS due_date,
        json_extract_path_text(payload, 'line_items') AS line_items,
        JSON_ARRAY_LENGTH(json_extract_path_text(payload, 'line_items')) AS no_of_line_items,
        json_extract_array_element_text(line_items, numbers.ordinal::int, TRUE) AS line_item_split,
        json_extract_array_element_text(line_items, numbers.ordinal::int, TRUE) AS line_item_split,
        json_extract_path_text(line_item_split, 'contract_ids') AS contract_ids,
        json_extract_path_text(line_item_split, 'order_number') AS order_number,
        json_array_length(contract_ids) AS no_of_contracts,
        JSON_EXTRACT_PATH_text(
            json_extract_path_text(line_item_split, 'total'),
            'in_cents'
        ) / 100 AS amount_due,
        JSON_EXTRACT_PATH_text(
            JSON_EXTRACT_PATH_text(payload, 'amount_due'),
            'in_cents'
        ) / 100 total_order_amount,
        JSON_EXTRACT_PATH_text(
            JSON_EXTRACT_PATH_text(payload, 'tax'),
            'in_cents'
        ) / 100 AS total_order_tax,
        RANK() OVER (
            PARTITION BY uuid,
            event_name
            ORDER BY event_timestamp DESC
        ) AS idx,
        v2.event_name,
        v2.event_timestamp,
        v2.payload
    FROM last_event v2
        CROSS JOIN numbers
    WHERE 1 = 1
        AND v2.idx_ = 1
        AND numbers.ordinal < no_of_line_items
        AND payload LIKE '%USD%'
        AND uuid NOT IN (
            SELECT group_id
            FROM david.group_ids_to_discard --change here (static table, move another schema)
        )
),
raw_ AS (
    SELECT v2.*,
        json_extract_array_element_text(contract_ids, numbers.ordinal::int, TRUE) AS contract_id
    FROM raw_tmp v2
        CROSS JOIN numbers
    WHERE numbers.ordinal < no_of_contracts
        AND 1 = 1
        AND type_ != 'purchase'
),
valid_ AS (
    SELECT order_number,
        contract_id,
        due_date,
        event_name,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY contract_id
            ORDER BY due_date DESC
        ) row_n,
        max(
            CASE
                WHEN event_name = 'paid' THEN due_date
            END
        ) OVER (
            PARTITION BY contract_id
            ORDER BY due_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) last_paid,
        max(
            CASE
                WHEN event_name = 'failed' THEN due_date
            END
        ) OVER (
            PARTITION BY contract_id
            ORDER BY due_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) last_failed
    FROM raw_
    WHERE 1 = 1
),
raw_3 AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY contract_id
            ORDER BY due_date ASC
        ) row_n1
    FROM valid_ v1
    WHERE 1 = 1 --and order_number = 'F2549766175'
        AND (
            (
                event_name = 'failed'
                AND due_date > COALESCE(last_paid, '1901-01-01')
            )
            OR (
                event_name = 'paid'
                AND row_n = 1
            )
        )
)
SELECT DISTINCT *
FROM raw_3
WHERE 1 = 1
    AND row_n1 = 1;
