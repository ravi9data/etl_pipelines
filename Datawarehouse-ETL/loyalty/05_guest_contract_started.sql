
/*
 * REFERRAL GUEST CONTRACT STARTED NEW
 */
DROP TABLE IF EXISTS ods_referral.guest_contract_started;
CREATE TABLE ods_referral.guest_contract_started AS 
WITH orders_basic AS (
	SELECT
	customer_id::int AS guest_id,
	split_part(transaction_id,'_',2) AS transaction_id ,
	order_id,
	CASE 
			WHEN created_at NOT ILIKE '%-%' THEN CAST(timestamp 'epoch' + cast(created_at AS bigint)/1000 * interval '1 second' AS timestamp)
			WHEN created_at ILIKE '%-%' THEN CAST(created_at AS timestamp) 
		END AS created_date,
	CASE 
			WHEN created_date<='2022-06-30' THEN 'Germany'
			ELSE 
				CASE 
					WHEN default_store_code = 'de' THEN 'Germany'
					WHEN default_store_code ='es' THEN 'Spain'
					WHEN default_store_code ='nl' THEN 'Netherlands'
					WHEN default_store_code='at' THEN 'Austria'
					WHEN default_store_code='us' THEN 'United States'
				ELSE NULL 
				END
			END AS guest_country
	FROM stg_curated.referral_eu_guest_contract_started_v1 regcsv 
	union
	SELECT 
		guest_id::int AS guest_id,
		split_part(transaction_id,'_',2) AS transaction_id ,
		ordeR_id,
		CASE 
			WHEN created_at NOT ILIKE '%-%' THEN CAST(timestamp 'epoch' + cast(created_at AS bigint)/1000 * interval '1 second' AS timestamp)
			WHEN created_at ILIKE '%-%' THEN CAST(created_at AS timestamp) 
		END AS created_date,
		CASE 
			WHEN created_date<='2022-06-30' THEN 'Germany'
			ELSE 
				CASE 
					WHEN default_store_code = 'de' THEN 'Germany'
					WHEN default_store_code ='es' THEN 'Spain'
					WHEN default_store_code ='nl' THEN 'Netherlands'
					WHEN default_store_code='at' THEN 'Austria'
					WHEN default_store_code='us' THEN 'United States'
				ELSE NULL 
				END
			END AS guest_country
		--ROW_NUMBER () OVER (PARTITION BY guest_id ORDER BY contract_start_date) AS idx 
	FROM stg_curated.referral_eu_host_invitation_fulfilled_v2 rehifv
	WHERE created_date>'2022-06-10'
	UNION ---Adding OLD orders which ARE NOT present IN kafka events above
	SELECT 
	guestid::int AS guest_id,
	id1 AS transaction_id,
	ordernumber AS order_id,
	createdat AS created_date,
	'Germany' AS guest_country
	FROM ods_referral.contract_started_historical
	WHERE created_date<='2022-06-10'
	)
,orders AS (
	SELECT 
		DISTINCT 
		guest_id,
		transaction_id,
		order_id,
		guest_country
	FROM orders_basic
	WHERE order_id IS NOT NULL 
	)
, allocations AS (
	SELECT 
		Order_id,
		delivered_at::timestamp AS delivered_date ,
		COALESCE(widerruf_validity_date,dateadd('day',14,delivered_at::timestamp)) AS wideruf_date,
		ROW_NUMBER ()OVER(PARTITION BY ORDER_ID ORDER BY delivered_at) idx
	FROM ods_production.allocation a2 )
,idx AS (SELECT 
	o.guest_id,
	o.transaction_id,
	CASE WHEN revoked_date IS NOT NULL THEN NULL ELSE o.ordeR_id END AS order_id,
	CASE WHEN revoked_date IS NOT NULL THEN NULL ELSE delivered_date END AS Loyalty_contract_start_date,
	CASE WHEN revoked_date IS NOT NULL THEN NULL ELSE wideruf_date END AS wideruf_date,
	revoked_date,
	o.guest_country,
	ROW_NUMBER()OVER(PARTITION BY o.guest_id ORDER BY loyalty_contract_start_date)idx
FROM  orders o
LEFT JOIN ods_referral.guest_revoked gr 
ON o.order_id=gr.order_id 
LEFT JOIN allocations a 
ON a.order_id=o.order_id
WHERE a.idx=1
AND Loyalty_contract_start_date IS NOT NULL )
SELECT * FROM idx WHERE idx=1;