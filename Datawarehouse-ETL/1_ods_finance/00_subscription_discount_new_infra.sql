DELETE FROM ods_production.subscription_discount_new_infra
WHERE kafka_received_at >= CURRENT_DATE - 2;

INSERT INTO ods_production.subscription_discount_new_infra
/*THIS SOURCE SHOWS ALL DISCOUNTS COMING FROM customer.contracts 
 * SOME SUBSCRIPTIONS GET REFUSED (eg. F-WE74MK8A) 
 * OR SOME SUBSCRIPTIONS DO NOT APPEAR IN ADMIN PANEL AT ALL (eg. F-6DW34BR8)
 * SO NOT ALL DISCOUNTS WERE USED EVENTUALLY 
 * ALSO THE DATA FOR 2021 SEEMS NOT ALWAYS CONSISTENT 
 * WHERE DISCOUNT AMOUNTS ARE DIFFERENT BETWEEN customer.contracts AND ADMIN PANEL (eg. F-6V7V87AB, F-9EK9RB38)
 * ALSO SOME DISCOUNTS (SUPRISE DISCOUNTS) APPEAR TO HAVE BEEN APPLIED AT PERIOD 1 
 * WHEREAS THEY ARE ACTUALLY APPLIED AT A LATER STAGE (eg. F-F4T9JD9N) PROD ENGINEERING IS SUPPOSED TO FIX THIS
WITH number_sequance AS (
SELECT ordinal
FROM public.numbers
WHERE ordinal < 20
)
, discount_base AS (
SELECT 
  contract_id AS subscription_id
 ,kafka_received_at
 ,event_name
 ,JSON_EXTRACT_PATH_TEXT(billing_terms, 'price_before_discount') AS price_before_discount
 ,CASE
	 WHEN event_name = 'discount_applied' 
	  THEN NULLIF(JSON_EXTRACT_PATH_TEXT(new_discount, 'period'), '')::INT 
	 WHEN event_name = 'discount_removed' 
	  THEN NULLIF(JSON_EXTRACT_PATH_TEXT(discount_removed, 'period'), '')::INT   
	 WHEN  event_name = 'created'
	  THEN 1
	END AS period_number
 ,CASE
	 WHEN event_name = 'discount_applied' 
	  THEN new_discount
	 ELSE JSON_EXTRACT_PATH_TEXT(billing_terms, 'discounts')
	END AS discount_applied_discount_detail
 ,CASE
	 WHEN event_name = 'discount_removed' 
	  THEN discount_removed
	 ELSE NULL
	END AS discount_removed_discount_detail	
 ,JSON_ARRAY_LENGTH(discount_applied_discount_detail, TRUE) AS discount_applied_discount_nr_items
 ,JSON_ARRAY_LENGTH(discount_removed_discount_detail, TRUE) AS discount_removed_discount_nr_items
FROM staging.customers_contracts  
WHERE TRUE
  AND kafka_received_at >= CURRENT_DATE - 2
/*THERE ARE 3 EVENTS FOR DISCOUNTS
 * 'created': DISCOUNTS APPLIED DURING INITIAL ORDER CREATION
 * 'discount_applied': DISCOUNTS APPLIED AT A LATER STAGE
 * 'discount_removed': DISCOUNTS REMOVED AT A LATER STAGE
 * THEY HAVE SLIGHLTLY DIFFERENT JSON STRUCTURES*/
  AND event_name IN ('discount_applied', 'created', 'discount_removed')
  AND NOT (event_name = 'created' AND discount_applied_discount_detail = '[]')
)
,discount_appplied_or_order_created AS (
/*ONE PAYLOAD MIGHT HAVE MULTIPLE DISCOUNTS COMBINED EG. F-3J399V96 on PAYMENT NR. 3 
 * IN SUCH CASES, WE SPLIT THEM*/
SELECT 
  le.subscription_id
 ,le.kafka_received_at
 ,le.event_name
 ,le.price_before_discount::DECIMAL(22, 6)
 ,le.period_number
 ,CASE 
   WHEN le.discount_applied_discount_nr_items IS NULL 
    THEN le.discount_applied_discount_detail
 	 ELSE JSON_EXTRACT_ARRAY_ELEMENT_TEXT(le.discount_applied_discount_detail, ns.ordinal::INT, TRUE)
 	END AS discount_splitted
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'source'), '') AS discount_source
 ,CASE
  	WHEN discount_splitted LIKE '%amount_in_cents%'
  	 THEN NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'amount_in_cents'), '')::DECIMAL(22, 6)/100 
	 	ELSE NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'amount'), '')::DECIMAL(22, 6) 
  END AS discount_amount
 ,DECODE(NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'recurring'), ''), 'false', 0, 'true', 1)::BOOLEAN AS is_recurring 
FROM discount_base le
  CROSS JOIN number_sequance ns 
WHERE TRUE
	AND ns.ordinal < COALESCE(discount_applied_discount_nr_items, 1)
	AND event_name IN ('discount_applied', 'created')
/*THEORATICALLY, 'discount_applied' SHOULD ONLY SHOW DISCOUNTS APPLIED AFTER INITIAL ORDER
 * WE EXCLUDE THE ONES THAT ARE ON FIRST PAYMENT BECAUSE THEY SHOULD ALREADY BE AVAILABLE UNDER created event.*/	
	AND NOT (le.event_name = 'discount_applied' AND le.period_number = 1)
)
,discount_removed AS (
SELECT 
  le.subscription_id
 ,le.kafka_received_at
 ,le.event_name
 ,le.price_before_discount::DECIMAL(22, 6)
 ,le.period_number
 ,CASE 
   WHEN le.discount_removed_discount_nr_items IS NULL 
    THEN le.discount_removed_discount_detail
 	 ELSE JSON_EXTRACT_ARRAY_ELEMENT_TEXT(le.discount_removed_discount_detail	, ns.ordinal::INT, TRUE)
 	END AS discount_splitted
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'source'), '') AS discount_source
 ,CASE
  	WHEN discount_splitted LIKE '%amount_in_cents%'
  	 THEN NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'amount_in_cents'), '')::DECIMAL(22, 6)/100 
	 	ELSE NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'amount'), '')::DECIMAL(22, 6) 
  END AS discount_amount
 ,DECODE(NULLIF(JSON_EXTRACT_PATH_TEXT(discount_splitted, 'recurring'), ''), 'false', 0, 'true', 1)::BOOLEAN AS is_recurring 
FROM discount_base le
  CROSS JOIN number_sequance ns 
WHERE TRUE
	AND ns.ordinal < COALESCE(le.discount_removed_discount_nr_items, 1)
	AND event_name = 'discount_removed'
)
, combined AS(
SELECT *
FROM discount_appplied_or_order_created
UNION
SELECT *
FROM discount_removed nm
)
,clean_recurring_payments AS (
/*THE SOURCE IS SHOWING RECURRING DISCOUNTS SOMETIMES ONLY ONCE AND SOMETIMES MULTIPLE TIMES
 * TO AVOID CONFUSION, WE WILL SHOW THE FIRST OCCURENCE (Eg. F-TM787HR6) */
SELECT 
 *
 ,CASE 
 	 WHEN is_recurring = 'false'
 	  THEN 1
 	 ELSE ROW_NUMBER() OVER (PARTITION BY subscription_id, discount_source, discount_amount,
/*WE WANT TO PARTITION 'discount_applied', 'created' events together and discount_removed SEPARATELY*/
 	  CASE 
 	 	 WHEN event_name IN ('discount_applied', 'created')
 	 	  THEN 'discount_added'
 	 	 WHEN event_name = 'discount_removed'
 	 	  THEN 'discount_removed'
 	 END
 	 ORDER BY period_number)   
 END AS rx_
FROM combined
)
SELECT 
  subscription_id
 ,kafka_received_at
 ,event_name
 ,price_before_discount
 ,period_number
 ,discount_source
 ,discount_amount
 ,is_recurring
FROM clean_recurring_payments
WHERE rx_ = 1
;