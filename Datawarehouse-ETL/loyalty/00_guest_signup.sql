/*
 * REFERRAL GUEST SIGNUP 
 */
DROP TABLE IF EXISTS ods_referral.guest_signup;
CREATE TABLE ods_referral.guest_signup AS 
WITH signup_prep AS (
	SELECT 
		customer_id::int AS guest_id,
		'kafka' AS src,
		split_part(transaction_id,'_',2)::text AS transaction_id ,
		campaign_id ,
		created_at AS guest_created_date ,
		default_store_code,
		code_owner_id::int  AS host_id
FROM stg_curated.referral_eu_code_used_v1
WHERE created_at::Date>'2022-06-25'
	UNION DISTINCT 
		SELECT 
			guestid::int AS guest_id,
			'old' AS src,
			id1::text AS transaction_id,
			campaignid AS campaign_id,
			createdat::timestamp AS guest_created_date,
			'de' AS default_store_code,
			hostid::int AS host_id
		FROM ods_referral.contract_started_historical 
			WHERE guest_created_date::DAte<='2022-06-25')
	,signup AS (
	SELECT 
	*,
	CASE 
		WHEN default_store_code = 'de' THEN 'Germany'
		WHEN default_store_code ='es' THEN 'Spain'
		WHEN default_store_code ='nl' THEN 'Netherlands'
		WHEN default_store_code='at' THEN 'Austria'
		WHEN default_store_code='us' THEN 'United States'
	ELSE NULL 
	END AS guest_country,
	ROW_NUMBER () OVER (PARTITION BY guest_id ORDER BY guest_created_date  DESC ) idx 
	FROM signup_prep )
	SELECT 
	guest_id,
	transaction_id,
	campaign_id,
	guest_created_date,
	host_id,
	guest_country
	FROM signup WHERE idx =1;
	