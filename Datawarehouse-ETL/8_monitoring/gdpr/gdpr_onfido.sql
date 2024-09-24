--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_onfido;

INSERT INTO hightouch_sources.gdpr_onfido 
    (customer_id
    ,order_id
    ,verification_state
    ,"trigger"
    ,onfido_trigger
    ,is_processed
    ,onfido_timestamp
    )
WITH onfido AS (
	SELECT
		o.customer_id,
        v.order_id,
        verification_state,
        "trigger",
        is_processed,
        "data" as data_json,
        CASE
            WHEN "trigger" = 1
                THEN 'Prepaid Card'
            WHEN "trigger" = 2
                THEN 'Unverified PayPal'
            WHEN "trigger" = 3
                THEN 'FFP'
            WHEN "trigger" = 6 --Deprecated
                THEN 'Above 400'
            WHEN "trigger" IN (4, 5, 7)
                THEN 'High Risk Assets'
            WHEN "trigger" = 8
                THEN 'Onfido Bonus'
            WHEN "trigger" = 9
                THEN 'Manual Request'
            WHEN "trigger" = 10
                THEN 'Multiple phones'
            WHEN "trigger" = 11
                THEN 'Multiple sessions'
            WHEN "trigger" = 12
                THEN 'Spain'
            WHEN "trigger" = 13
                THEN 'Test'
            WHEN "trigger" = 14
                THEN 'DE High Risk'
            WHEN "trigger" = 20
                THEN 'Indirect'
            WHEN "trigger" = 100
                THEN 'No trigger data available'
            ELSE NULL
        END AS onfido_trigger,
        updated_at,
        row_number() over (partition by v.order_id order by updated_at desc) as row_n
    FROM stg_curated.id_verification_order v
    INNER JOIN master.ORDER o
      ON v.order_id = o.order_id
    WHERE o.customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
)
SELECT 
	customer_id,
	order_id,
	verification_state,
	"trigger",
	onfido_trigger,
	is_processed,
	updated_at AS onfido_timestamp
FROM onfido
WHERE row_n = 1
;