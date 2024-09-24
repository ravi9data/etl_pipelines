/*
 * REFERRAL HOST CODE CREATED
 */
DROP TABLE IF EXISTS ods_referral.host_code_created;
CREATE TABLE ods_referral.host_code_created AS 
SELECT 
userid::int AS host_id,
code AS host_code,
id AS transaction_id,
createdat::timestamp AS code_created_date
FROM ods_finance.referral_eu_codes rec ;


/*
 * REFERRAL HOST REFERRED
 */
DROP TABLE IF EXISTS ods_referral.host_referred;
CREATE TABLE ods_referral.host_referred AS 
WITH hosts_pre AS (
	SELECT 
		host_id::int,
		guest_id AS first_guest_referred,
		transaction_id,
		campaign_id ,
		guest_created_date AS host_referred_date ,
		COALESCE(c.shipping_country,o.store_country) AS host_country,
		ROW_NUMBER() OVER (PARTITION BY host_id ORDER BY guest_created_date) idx
	FROM ods_referral.guest_signup gs
	LEFT JOIN ods_production.customer c
	ON gs.host_id=c.customer_id
	LEFT JOIN ods_production.ORDER o 
	ON gs.host_id=o.customer_id)
SELECT 
	host_id,
	first_guest_referred,
	transaction_id,
	campaign_id ,
	host_referred_date,
	host_country
FROM hosts_pre
WHERE idx=1;


GRANT all ON ALL TABLES IN SCHEMA ods_referral TO group BI;