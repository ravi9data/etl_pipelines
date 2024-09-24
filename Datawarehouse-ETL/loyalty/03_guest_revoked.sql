/*
 * REFERRAL GUEST REVOKED
 */

DROP TABLE IF EXISTS ods_referral.guest_revoked;
CREATE TABLE ods_referral.guest_revoked AS 
	SELECT 
		CUSTOMER_ID::int AS guest_id,
		code_owner_id::int AS host_id,
		order_id,
		split_part(transaction_id,'_',2) AS transaction_id ,
		created_at AS revoked_date,
		CASE 
			WHEN default_store_code = 'de' THEN 'Germany'
			WHEN default_store_code ='es' THEN 'Spain'
			WHEN default_store_code ='nl' THEN 'Netherlands'
			WHEN default_store_code='at' THEN 'Austria'
			WHEN default_store_code='us' THEN 'United States'
		ELSE NULL 
		END AS guest_country,
		ROW_NUMBER() OVER(Partition BY guest_id ORDER BY created_at) AS idx
	FROM stg_curated.referral_eu_guest_item_returned_v1 regirv2;
