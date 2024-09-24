---------Grover Cash table (Earning and Redeem) 
WITH earning AS (
SELECT ---historical dump FOR earning
userid::int AS customer_id,
externalid AS trace_id,
actionId AS Action_id,
'Earning' AS event_name,
"source" AS earning_type,
CASE 
	WHEN status IN ('completed','active') THEN 'add' ELSE status 
END
 AS transaction_type,
 CASE 
	WHEN SOURCE ='referral_host' THEN startat::timestamp
	ELSE createdat::timestamp
END AS event_timestamp,
CASE 
	WHEN store='de' THEN 'Germany'
	WHEN store='nl' THEN 'Netherlands'
	WHEN store='es' THEN 'Spain'
	WHEN store='at' THEN 'Austria'
	WHEN store='us' THEN 'United States'
END AS country,
CASE 
	WHEN store ='us' THEN 'USD' ELSE 'EUR'
END AS currency,
NULL AS payment_id,
NULL AS manual_grant_reason,
NULL AS manual_grant_comments,
1 AS idx
FROM public."credit-transactions" ct 
WHERE TRUE 
AND len(inputtransaction)<1
--AND earning_type<>'manual_grant'
AND event_timestamp<'2023-03-15'
UNION ALL --Lastest earning FROM kafka---
SELECT 
JSON_EXTRACT_PATH_TEXT(payload,'userId')::int AS customer_id,
JSON_EXTRACT_PATH_TEXT(payload,'externalId') AS trace_id,
JSON_EXTRACT_PATH_TEXT(payload,'actionId') AS Action_id,
'Earning' AS event_name,
JSON_EXTRACT_PATH_TEXT(payload,'source') earning_type,
CASE 
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'status') IN ('completed','active') THEN 'add' ELSE JSON_EXTRACT_PATH_TEXT(payload,'status')  
END
 AS transaction_type,
CASE 
WHEN  JSON_EXTRACT_PATH_TEXT(payload,'source')='referral_host' THEN JSON_EXTRACT_PATH_TEXT(payload,'startAt')::timestamp 
ELSE JSON_EXTRACT_PATH_TEXT(payload,'createdAt')::timestamp END AS event_timestamp,
CASE 
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'store')='de' THEN 'Germany'
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'store')='nl' THEN 'Netherlands'
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'store')='es' THEN 'Spain'
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'store')='at' THEN 'Austria'
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'store')='us' THEN 'United States'
END
AS country,
CASE
	WHEN JSON_EXTRACT_PATH_TEXT(payload,'store') IN ('us') THEN 'USD'
	ELSE 'EUR' END currency,
	NULL AS payment_id,
	NULL AS manual_grant_reason,
NULL AS manual_grant_comments,
FROM stg_curated.internal_loyalty_service_transactions_v1
WHERE event_timestamp>='2023-03-15'
UNION ALL --LAtest earning manual GRANT--
SELECT
		DISTINCT JSON_EXTRACT_PATH_TEXT(payload, 'userId') :: integer AS customer_id,
		JSON_EXTRACT_PATH_text(
			JSON_EXTRACT_PATH_text(payload, 'metadata'),
			'traceId'
		) AS trace_id,
		JSON_EXTRACT_PATH_TEXT(payload, 'id') AS action_id,
		CASE WHEN event_name='new_action' THEN 'Earning' ELSE event_name END AS event_name,
		JSON_EXTRACT_PATH_TEXT(payload, 'source') AS earning_type,
		JSON_EXTRACT_PATH_TEXT(payload, 'type') AS transaction_type,
		--dateadd(day,StartinDays::int,current_date) AS test_date,
		JSON_EXTRACT_PATH_TEXT(payload, 'createdAt')::timestamp  AS event_timestamp,
		CASE
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'de' THEN 'Germany'
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'es' THEN 'Spain'
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'nl' THEN 'Netherlands'
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'at' THEN 'Austria'
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'us' THEN 'United States'
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = '' THEN 'Germany'
		END AS country,
		CASE 
			WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store')='us' THEN 'USD' 
			ELSE 'EUR'
		END AS currency,
		NULL AS payment_id,
		JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'metadata'),'reason') AS manual_grant_reason,
		JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'metadata'),'comment') AS manual_grant_comments,
		1 AS idx
		FROM stg_curated.internal_loyalty_service_actions_queue ilsaq	
		WHERE payload LIKE '%manual_grant%'
		AND event_timestamp>'2023-03-15'
)
, src AS (
	---Cash Redemption-----
SELECT
		DISTINCT JSON_EXTRACT_PATH_text(payload, 'user_id') :: integer AS customer_id,
		NULL AS trace_id,
		NULL AS action_id,
		CASE 
			when event_name='credits-commit' THEN 'Redemption' 
		END AS event_name,
		NULL AS earning_type,
		NULL AS transaction_type,
		JSON_EXTRACT_PATH_text(payload, 'date') :: timestamp AS event_timestamp,
		 'Germany' AS country,
		JSON_EXTRACT_PATH_text(
			JSON_EXTRACT_PATH_text(payload, 'amount'),
			'currency'
		) AS Currency,
		JSON_EXTRACT_PATH_text(payload, 'payment_id') AS payment_id,
		NULL AS manual_grant_reason,
		NULL AS manual_grant_comments,
		1 AS idx
from stg_kafka_events_full.stream_internal_loyalty_service_credits silsc--Redemption before 2022-01-01 is missing the spectrum, doing the union as suggest by Deng team
where event_timestamp<='2022-01-01' 
union distinct 
SELECT
		DISTINCT JSON_EXTRACT_PATH_text(payload, 'user_id') :: integer AS customer_id,
		NULL AS trace_id,
		NULL AS action_id,
		CASE 
			when event_name='credits-commit' THEN 'Redemption' 
		END AS event_name,
		NULL AS earning_type,
		NULL AS transaction_type,
		JSON_EXTRACT_PATH_text(payload, 'date') :: timestamp AS event_timestamp,
		CASE
			WHEN event_timestamp >= '2022-06-30' THEN (
				CASE
					WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'de' THEN 'Germany'
					WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'es' THEN 'Spain'
					WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'nl' THEN 'Netherlands'
					WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'at' THEN 'Austria'
					WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = 'us' THEN 'United States'
					WHEN JSON_EXTRACT_PATH_TEXT(payload, 'store') = '' THEN 'Germany'
				END
			)
			ELSE 'Germany'
		END AS country,
		JSON_EXTRACT_PATH_text(
			JSON_EXTRACT_PATH_text(payload, 'amount'),
			'currency'
		) AS Currency,
		JSON_EXTRACT_PATH_text(payload, 'payment_id') AS payment_id,
		NULL AS manual_grant_reason,
		NULL AS manual_grant_comments,
		1 AS idx 
from  stg_curated.internal_loyalty_service_credits
where event_timestamp>='2022-01-01'
UNION ALL 
	---Cash Earning-----
SELECT * FROM earning WHERE idx=1)
,user_class AS(
SELECT c.customer_id ,
CASE WHEN gc.customer_id IS NOT NULL THEN 1 ELSE 0 END is_card_customer,
CASE WHEN gr.customer_id IS NOT NULL THEN 1 ELSE 0 END is_referral_customer,
CASE
	WHEN is_card_customer =1 AND is_referral_customer=0 THEN 'Card_user'
	WHEN is_card_customer =0 AND is_referral_customer=1 THEN 'Referral_user'
	WHEN is_card_customer =1 AND is_referral_customer=1 THEN 'Both'
	ELSE 'Other'
	END AS user_classification
FROM 
ods_production.customer c 
ON c.customer_id =gc.customer_id 
LEFT JOIN 
	(SELECT DISTINCT customer_id
		FROM master_referral.loyalty_customer
			WHERE first_referral_event IS NOT NULL ) gr
ON c.customer_id =gr.customer_id)
SELECT s.*, user_classification
FROM src s 
LEFT JOIN user_class uc 
	ON s.customer_id=uc.customer_id;

