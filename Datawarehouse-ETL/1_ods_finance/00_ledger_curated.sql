DELETE FROM ods_production.ledger_curated
WHERE consumed_at::DATE >= CURRENT_DATE -2 
;

INSERT INTO ods_production.ledger_curated
SELECT DISTINCT 
 ,c.salesforce_subscription_id 
 ,c.user_id 
FROM oltp_legacy_care_api.payment p
  INNER JOIN oltp_legacy_care_api.contract c
    ON p.contract_id = c.id
)
SELECT 
  led.status
 ,led.value
 ,led.consumed_at
 ,led.resource_created_at
 ,led.slug
 ,led.tax_rate
 ,led.created_at
 ,led.current_allocated_taxes
 ,led.currency
 ,led.value_without_tax
 ,led.external_id
 ,led.origin
 ,led.metadata
 ,led.published_at
 ,led.latest_movement
 ,led.id
 ,led.transactions
 ,led.tax_amount
 ,led.country_iso
 ,led."type"
 ,led.current_allocated_amount
 ,led.replay_reason
 ,led.replay
 ,COALESCE(gcm.user_id::INT, s.customer_id, led.customer_id::INT, NULLIF(JSON_EXTRACT_PATH_TEXT(led.metadata,'customer_id'), '')::INT) AS customer_id 
 ,NULLIF(COALESCE(gcm.salesforce_subscription_id, s.subscription_id, NULLIF(contract_id,''), CASE 
   WHEN led.type IN ('subscription', 'purchase')
 	THEN led.external_id  
  END),'') AS subscription_id
 ,CASE 
   WHEN led.type IN ('shipping', 'addon')
 	  THEN led.external_id 
   WHEN TYPE = 'addon-purchase'  
   	THEN COALESCE(nullif(JSON_EXTRACT_PATH_TEXT(led.metadata,'order_id'),''),JSON_EXTRACT_PATH_TEXT(led.metadata,'order_number'))
   WHEN TYPE = 'external-physical-otp'
	  THEN JSON_EXTRACT_PATH_TEXT(led.metadata,'order_number')
 	  THEN COALESCE(s.order_id, led.order_number, NULLIF(JSON_EXTRACT_PATH_TEXT(led.metadata,'order_number'), ''))
  END AS order_id
 ,CASE 
   WHEN led.type in ( 'addon-purchase', 'external-physical-otp')
    THEN led.external_id 
  END AS addon_id  
 ,JSON_EXTRACT_PATH_TEXT(led.metadata,'period') AS payment_number
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions, 0) , 'id'), '') AS transaction_id
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'id') latest_movement_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(led.latest_movement,'metadatas'),0, TRUE),'payment_reference'), '') AS psp_reference_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(led.latest_movement,'metadatas'),0, TRUE),'payment_method_type'), '') AS payment_method
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'context') AS latest_movement_context
 ,TIMESTAMP 'EPOCH' + CAST(NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions,0) , 'created_at'), '') 
   AS BIGINT)/1000 * INTERVAL '1 second' AS transaction_created_at_timestamp
 ,TIMESTAMP 'EPOCH' + CAST(led.resource_created_at AS BIGINT)/1000 * INTERVAL '1 second' AS resource_created_at_timestamp
 ,TIMESTAMP 'EPOCH' + CAST(led.created_at AS BIGINT)/1000 * INTERVAL '1 second' AS created_at_timestamp
 ,CASE 
   WHEN NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions,0), 'effective_date'), '') IS NULL  
	THEN NULL
   ELSE TIMESTAMP 'EPOCH' + CAST(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions,0), 'effective_date') AS BIGINT)/1000 * INTERVAL '1 second'
  END AS money_received_at_timestamp 
 ,TIMESTAMP 'EPOCH' + CAST(JSON_EXTRACT_PATH_TEXT(led.latest_movement,'created_at') AS BIGINT)/1000 * INTERVAL '1 second' AS latest_movement_created_at_timestamp 
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'status') as latest_movement_status 
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'amount_to_allocate')::DECIMAL(22, 6) AS amount_to_allocate
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'context_reason') AS latest_movement_context_reason
 ,REGEXP_REPLACE(led.latest_movement, '\\\\{3}\"', '') AS latest_movement_clean
 ,NULLIF(REPLACE(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
     JSON_EXTRACT_PATH_TEXT(latest_movement_clean,'metadatas'),0),'provider_response'),'\\', ''), '') AS provider_response_clean
 ,JSON_EXTRACT_PATH_TEXT(led.metadata,'variant_id') AS variant_id
FROM s3_spectrum_kafka_topics_raw.payments_eu_notifications_resource_v1 led
  LEFT JOIN ods_production.subscription s
    ON led.external_id = s.subscription_id
WHERE TRUE 
  AND created_at >= '1679565089106'
  AND consumed_at::DATE >= CURRENT_DATE -2 
UNION ALL
SELECT 
  led.status
 ,led.value
 ,led.consumed_at
 ,led.resource_created_at
 ,led.slug
 ,led.tax_rate
 ,led.created_at
 ,led.current_allocated_taxes
 ,led.currency
 ,led.value_without_tax
 ,led.external_id
 ,led.origin
 ,led.metadata
 ,led.published_at
 ,led.latest_movement
 ,led.id
 ,led.transactions
 ,led.tax_amount
 ,led.country_iso
 ,led."type"
 ,led.current_allocated_amount
 ,led.replay_reason
 ,led.replay
 ,COALESCE(gcm.user_id::INT, s.customer_id, led.customer_id::INT, NULLIF(JSON_EXTRACT_PATH_TEXT(led.metadata,'customer_id'), '')::INT) AS customer_id 
 ,NULLIF(COALESCE(gcm.salesforce_subscription_id, s.subscription_id, NULLIF(contract_id,''), CASE 
   WHEN led.type IN ('subscription', 'purchase')
 	THEN led.external_id  
  END),'') AS subscription_id
 ,CASE 
   WHEN led.type IN ('shipping', 'addon')
 	  THEN led.external_id 
   WHEN TYPE = 'addon-purchase'  
   	THEN COALESCE(nullif(JSON_EXTRACT_PATH_TEXT(led.metadata,'order_id'),''),JSON_EXTRACT_PATH_TEXT(led.metadata,'order_number'))
   WHEN TYPE = 'external-physical-otp'
	  THEN JSON_EXTRACT_PATH_TEXT(led.metadata,'order_number')
 	  THEN COALESCE(s.order_id, led.order_number, NULLIF(JSON_EXTRACT_PATH_TEXT(led.metadata,'order_number'), ''))
  END AS order_id
 ,CASE 
   WHEN led.type in ( 'addon-purchase', 'external-physical-otp')
    THEN led.external_id 
  END AS addon_id  
 ,JSON_EXTRACT_PATH_TEXT(led.metadata,'period') AS payment_number
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions, 0) , 'id'), '') AS transaction_id
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'id') latest_movement_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(led.latest_movement,'metadatas'),0, TRUE),'payment_reference'), '') AS psp_reference_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(led.latest_movement,'metadatas'),0, TRUE),'payment_method_type'), '') AS payment_method
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'context') AS latest_movement_context
 ,TIMESTAMP 'EPOCH' + CAST(NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions,0) , 'created_at'), '') 
   AS BIGINT)/1000 * INTERVAL '1 second' AS transaction_created_at_timestamp
 ,TIMESTAMP 'EPOCH' + CAST(led.resource_created_at AS BIGINT)/1000 * INTERVAL '1 second' AS resource_created_at_timestamp
 ,TIMESTAMP 'EPOCH' + CAST(led.created_at AS BIGINT)/1000 * INTERVAL '1 second' AS created_at_timestamp
 ,CASE 
   WHEN NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions,0), 'effective_date'), '') IS NULL  
	THEN NULL
   ELSE TIMESTAMP 'EPOCH' + CAST(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(led.transactions,0), 'effective_date') AS BIGINT)/1000 * INTERVAL '1 second'
  END AS money_received_at_timestamp 
 ,TIMESTAMP 'EPOCH' + CAST(JSON_EXTRACT_PATH_TEXT(led.latest_movement,'created_at') AS BIGINT)/1000 * INTERVAL '1 second' AS latest_movement_created_at_timestamp 
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'status') as latest_movement_status 
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'amount_to_allocate')::DECIMAL(22, 6) AS amount_to_allocate
 ,JSON_EXTRACT_PATH_TEXT(led.latest_movement,'context_reason') AS latest_movement_context_reason
 ,REGEXP_REPLACE(led.latest_movement, '\\\\{3}\"', '') AS latest_movement_clean
 ,NULLIF(REPLACE(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
     JSON_EXTRACT_PATH_TEXT(latest_movement_clean,'metadatas'),0),'provider_response'),'\\', ''), '') AS provider_response_clean
 ,JSON_EXTRACT_PATH_TEXT(led.metadata,'variant_id') AS variant_id
FROM s3_spectrum_kafka_topics_raw.payments_us_notifications_resource_v1 AS led
  LEFT JOIN ods_production.subscription s
    ON led.external_id = s.subscription_id
WHERE TRUE 
  AND created_at >= '1679565089106'
  AND consumed_at::DATE >= CURRENT_DATE -2 
;
