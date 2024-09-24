--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_credit_bureau;

INSERT INTO hightouch_sources.gdpr_credit_bureau
(
	credit_bureau,
	customer_id,
	creation_timestamp,
	score,
	credit_bureau_class,
	first_name,
	last_name,
	gender,
	date_of_birth,
	country,
	city,
	plz,
	street,
	risk_ratio,
	infotext
)
WITH burgel AS (
	SELECT
		customer_id::INT,
		score_value::float AS burgel_score,
		created_at::timestamp AS creation_timestamp
	FROM stg_curated.crifburgel_data
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	UNION 
	SELECT
		customer_id,
		burgel_score,
		creation_timestamp
	FROM stg_realtimebrandenburg.burgel_data 
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
)
, boniversum AS (
	SELECT 
		customer_id::INT AS customer_id,
		verita_score::INT AS verita_score,
		person_summary,
		created_at::timestamp AS creation_timestamp
	FROM stg_curated.boniversum_data
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	UNION
	SELECT 
		customer_id,
		verita_score,
		person_summary,
		creation_timestamp
	FROM stg_realtimebrandenburg.boniversum_data b 
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	  AND customer_id NOT IN (SELECT customer_id FROM s3_spectrum_rds_dwh_order_approval.boniversum_data WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input))
)
, schufa AS (
	select 
		customer_id,
		creation_timestamp,
		schufa_score::float score,
		schufa_class credit_bureau_class,
		schufa_first_name first_name,
		schufa_last_name last_name,
		schufa_gender gender,
		schufa_date_of_birth date_of_birth,
		schufa_current_country country,
		schufa_current_city city,
		schufa_current_plz plz,
		schufa_current_street street,
		schufa_risk risk_ratio,
		schufa_infotext infotext
	from ods_data_sensitive.schufa
	where customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	  and schufa_score <> ''
)
, external_scores AS (
	SELECT 
		customer_id,
		submitted_date::timestamp AS creation_timestamp,
		-- ES Experian
		es_experian_score_value,
		es_experian_score_rating,
		-- ES Equifax
		es_equifax_score_value,
		es_equifax_score_rating,
		-- NL FOCUM
		nl_focum_score_value,
		nl_focum_score_rating,
		-- NL EXPERIAN
		nl_experian_score_value,
		nl_experian_score_rating,
		-- US PRECISE
		us_precise_score_value,
		us_precise_score_rating,
		-- US FICO
		us_fico_score_value,
		us_fico_score_rating,
		-- GLOBAL SEON
		global_seon_fraud_score,
		-- EU EKATA
		eu_ekata_identity_risk_score,
		eu_ekata_identity_network_score,
		-- US CLARITY
		us_clarity_score_value, ---- CHECK IF VALUE INCLUDED OR NOT 
		us_clarity_score_rating,
		-- US VANTAGE 
		us_vantage_score_value,
		us_vantage_score_rating
	FROM ods_data_sensitive.external_provider_order_score
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
)
SELECT 
	'schufa' AS credit_bureau,
	customer_id,
	creation_timestamp,
	score,
	credit_bureau_class,
	first_name,
	last_name,
	gender,
	date_of_birth,
	country,
	city,
	plz, 
	street,
	risk_ratio,
	infotext
FROM schufa
UNION ALL
SELECT
	'burgel' AS credit_bureau,
	customer_id,
	creation_timestamp,
	burgel_score,
	NULL AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM burgel
UNION ALL
SELECT
	'boniversum' AS credit_bureau,
	customer_id,
	creation_timestamp,
	verita_score,
	NULL AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	person_summary AS infotext
FROM boniversum
UNION ALL
SELECT
	'es_experian' AS credit_bureau,
	customer_id,
	creation_timestamp,
	es_experian_score_value,
	es_experian_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE es_experian_score_value IS NOT NULL
UNION ALL
SELECT
	'equifax' AS credit_bureau,
	customer_id,
	creation_timestamp,
	es_equifax_score_value,
	es_equifax_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE es_equifax_score_value IS NOT NULL
UNION ALL
SELECT
	'focum' AS credit_bureau,
	customer_id,
	creation_timestamp,
	nl_focum_score_value,
	nl_focum_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE nl_focum_score_value IS NOT NULL
UNION ALL
SELECT
	'nl_experian' AS credit_bureau,
	customer_id,
	creation_timestamp,
	nl_experian_score_value,
	nl_experian_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE nl_experian_score_value IS NOT NULL
UNION ALL
SELECT
	'precise' AS credit_bureau,
	customer_id,
	creation_timestamp,
	us_precise_score_value,
	us_precise_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE us_precise_score_value IS NOT NULL
UNION ALL
SELECT
	'fico' AS credit_bureau,
	customer_id,
	creation_timestamp,
	us_fico_score_value,
	us_fico_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE us_fico_score_value IS NOT NULL
UNION ALL
SELECT
	'seon' AS credit_bureau,
	customer_id,
	creation_timestamp,
	global_seon_fraud_score,
	NULL AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE global_seon_fraud_score IS NOT NULL
UNION ALL
SELECT
	'ekata_risk' AS credit_bureau,
	customer_id,
	creation_timestamp,
	eu_ekata_identity_risk_score,
	NULL AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE eu_ekata_identity_risk_score IS NOT NULL
UNION ALL
SELECT
	'ekata_network' AS credit_bureau,
	customer_id,
	creation_timestamp,
	eu_ekata_identity_network_score,
	NULL AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE eu_ekata_identity_network_score IS NOT NULL
UNION ALL
SELECT
	'clarity' AS credit_bureau,
	customer_id,
	creation_timestamp,
	us_clarity_score_value,
	us_clarity_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE us_clarity_score_value IS NOT NULL
UNION ALL
SELECT
	'vantage' AS credit_bureau,
	customer_id,
	creation_timestamp,
	us_vantage_score_value,
	us_vantage_score_rating AS credit_bureau_class,
	NULL AS first_name,
	NULL AS last_name,
	NULL AS gender,
	NULL AS date_of_birth,
	NULL AS country,
	NULL AS city,
	NULL AS plz,
	NULL AS street,
	NULL AS risk_ratio,
	NULL AS infotext
FROM external_scores
WHERE us_vantage_score_value IS NOT NULL
;
