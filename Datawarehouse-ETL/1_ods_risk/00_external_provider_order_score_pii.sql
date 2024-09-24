
DROP TABLE IF EXISTS spain_experian_delphi;
CREATE TEMP TABLE spain_experian_delphi
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'es_experian' AS score_provider,
	   	score_note AS es_experian_score_rating,
	   	score_probability::decimal(18,6) AS es_experian_avg_pd,
	   	score_value::decimal(38,2) AS es_experian_score_value,
	   	ROW_NUMBER() OVER(PARTITION BY customer_id, order_id ORDER BY created_at DESC, updated_at DESC) AS row_no,
	   	ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_rds_dwh_order_approval.experian_es_delphi_data ; 

DROP TABLE IF EXISTS spain_equifax;
CREATE TEMP TABLE spain_equifax
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'es_equifax' AS score_provider,
		score_value::decimal(38,2) AS es_equifax_score_value,
		CASE WHEN es_equifax_score_value BETWEEN -9999 AND 6 THEN 'ES08'
			 WHEN es_equifax_score_value BETWEEN 7 AND 29 THEN 'ES07'
			 WHEN es_equifax_score_value BETWEEN 30 AND 292 THEN 'ES06'
			 WHEN es_equifax_score_value BETWEEN 293 AND 520 THEN 'ES05'
			 WHEN es_equifax_score_value BETWEEN 521 AND 628 THEN 'ES04'
			 WHEN es_equifax_score_value BETWEEN 629 AND 779 THEN 'ES03'
			 WHEN es_equifax_score_value BETWEEN 780 AND 827 THEN 'ES02'
			 WHEN es_equifax_score_value BETWEEN 828 AND 9999 THEN 'ES01'
		ELSE 'N/A'
		END AS es_equifax_score_rating,
		CASE WHEN es_equifax_score_rating = 'ES08' THEN 0.1792
			 WHEN es_equifax_score_rating = 'ES07' THEN 0.1449
			 WHEN es_equifax_score_rating = 'ES06' THEN 0.1123
			 WHEN es_equifax_score_rating = 'ES05' THEN 0.0989
			 WHEN es_equifax_score_rating = 'ES04' THEN 0.0827
			 WHEN es_equifax_score_rating = 'ES03' THEN 0.0615
			 WHEN es_equifax_score_rating = 'ES02' THEN 0.0462
			 WHEN es_equifax_score_rating = 'ES01' THEN 0.0339
		END::decimal(18,6) AS es_equifax_avg_pd,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY created_at DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_rds_dwh_order_approval.equifax_risk_score_data ; 


DROP TABLE IF EXISTS germany_schufa;
CREATE TEMP TABLE germany_schufa
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'de_schufa' AS score_provider,
		schufa_score::decimal(38,2) AS de_schufa_score_value,
		risk_rate/100::decimal(18,6) AS de_schufa_avg_pd,
		score_range AS de_schufa_score_rating,
		CASE WHEN score_details LIKE '%24 GEPRUEFTE IDENTITAET%' THEN 1 ELSE 0 END AS de_schufa_is_person_known,
		score_details AS schufa_score_details,
		raw_data AS schufa_range_text,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY created_at DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_rds_dwh_order_approval.schufa_data	; 


DROP TABLE IF EXISTS de_at_crif ;
CREATE TEMP TABLE de_at_crif -- CONTAINS Crif data on both German AND Austrian orders. We will JOIN them separately below.
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT 
		c.id::TEXT AS provider_query_id,
		c.customer_id::int,
		c.order_id,
		c.created_at::date AS score_date,
		'de_at_Crif' AS score_provider,
		c.score_value::decimal(38,2) AS crif_score_value,
		CASE WHEN c.person_known = 'True' THEN 1 WHEN c.person_known = 'False' THEN 0 END AS crif_is_person_known,
		CASE WHEN c.address_known = 'True' THEN 1 WHEN c.address_known = 'False' THEN 0 END AS crif_is_address_known,
		ROW_NUMBER() OVER (PARTITION BY c.customer_id, c.order_id ORDER BY c.created_at DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY c.id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_rds_dwh_order_approval.crifburgel_data c ; 
	

DROP TABLE IF EXISTS netherlands_experian;
CREATE TEMP TABLE netherlands_experian
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT 
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'nl_experian' AS score_provider,
		score_value::decimal(38,2) AS nl_experian_score_value,
		CASE WHEN nl_experian_score_value BETWEEN -9999 AND 741 THEN '7'
			 WHEN nl_experian_score_value BETWEEN 742 AND 799 THEN '6'
			 WHEN nl_experian_score_value BETWEEN 800 AND 849 THEN '5'
			 WHEN nl_experian_score_value BETWEEN 850 AND 874 THEN '4'
			 WHEN nl_experian_score_value BETWEEN 875 AND 899 THEN '3'
			 WHEN nl_experian_score_value BETWEEN 900 AND 949 THEN '2'
			 WHEN nl_experian_score_value BETWEEN 950 AND 9999 THEN '1'
		ELSE 'N/A'
		END AS nl_experian_score_rating,
		CASE WHEN person_known = 'True' THEN 1 WHEN person_known = 'False' THEN 0 END AS nl_exp_is_person_known,
		CASE WHEN address_known = 'True' THEN 1 WHEN address_known = 'False' THEN 0 END AS nl_exp_is_address_known,
		NULL AS nl_experian_avg_pd,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY created_at DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_rds_dwh_order_approval.experian_data ; 


DROP TABLE IF EXISTS netherlands_focum;
CREATE TEMP TABLE netherlands_focum
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT 
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'nl_focum' AS score_provider,
		risk_score::decimal(38,2) AS nl_focum_score_value,
		CASE WHEN nl_focum_score_value BETWEEN -90 AND 590 THEN 'NL10'
			 WHEN nl_focum_score_value BETWEEN 591 AND 703 THEN 'NL09'
			 WHEN nl_focum_score_value BETWEEN 704 AND 783 THEN 'NL08'
			 WHEN nl_focum_score_value BETWEEN 784 AND 848 THEN 'NL07'
			 WHEN nl_focum_score_value BETWEEN 849 AND 898 THEN 'NL06'
			 WHEN nl_focum_score_value BETWEEN 899 AND 937 THEN 'NL05'
			 WHEN nl_focum_score_value BETWEEN 938 AND 953 THEN 'NL04'
			 WHEN nl_focum_score_value BETWEEN 954 AND 968 THEN 'NL03'
			 WHEN nl_focum_score_value BETWEEN 969 AND 981 THEN 'NL02'
			 WHEN nl_focum_score_value BETWEEN 982 AND 9999 THEN 'NL01'
		ELSE 'N/A'
		END AS nl_focum_score_rating,
		CASE WHEN nl_focum_score_rating = 'NL10' THEN 0.1768
			 WHEN nl_focum_score_rating = 'NL09' THEN 0.1551
			 WHEN nl_focum_score_rating = 'NL08' THEN 0.0819
			 WHEN nl_focum_score_rating = 'NL07' THEN 0.0688
			 WHEN nl_focum_score_rating = 'NL06' THEN 0.0588
			 WHEN nl_focum_score_rating = 'NL05' THEN 0.0550
			 WHEN nl_focum_score_rating = 'NL04' THEN 0.0439
			 WHEN nl_focum_score_rating = 'NL03' THEN 0.0346
			 WHEN nl_focum_score_rating = 'NL02' THEN 0.0333
			 WHEN nl_focum_score_rating = 'NL01' THEN 0.0118
		ELSE 0.0
		END::decimal(18,6) AS nl_focum_avg_pd,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY created_at DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_rds_dwh_order_approval.focum_data ; 


DROP TABLE IF EXISTS united_states_precise;
CREATE TEMP TABLE united_states_precise
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
WITH precise_raw AS 
(
	SELECT 
		id::TEXT AS provider_query_id,
		customer_id,
		order_id,
		created_at,
		updated_at,
		precise_id_score
	FROM staging.risk_experian_us_precise_id_data_v1
	UNION 
	SELECT 
		id::TEXT AS provider_query_id,
		customer_id,
		order_id,
		created_at,
		updated_at,
		precise_id_score
	FROM s3_spectrum_kafka_topics_raw.risk_internal_us_experian_us_precise_id_data_v2
	)
	SELECT 
		provider_query_id::TEXT,
		customer_id::int AS customer_id,
		order_id,
		created_at::date AS score_date,
		'us_precise' AS score_provider,
		CASE WHEN precise_id_score <> 'null' THEN precise_id_score::decimal(38,2) END AS us_precise_score_value ,
		CASE WHEN us_precise_score_value IS NULL OR us_precise_score_value > 9000 THEN 'INVALID & BLANK'
			 WHEN us_precise_score_value <= 142 THEN 'US10'
			 WHEN us_precise_score_value BETWEEN 143 AND 186 THEN 'US09'
			 WHEN us_precise_score_value BETWEEN 187 AND 303 THEN 'US08'
			 WHEN us_precise_score_value BETWEEN 304 AND 355 THEN 'US07'
			 WHEN us_precise_score_value BETWEEN 356 AND 439 THEN 'US06'
			 WHEN us_precise_score_value BETWEEN 440 AND 498 THEN 'US05'
			 WHEN us_precise_score_value BETWEEN 499 AND 579 THEN 'US04'
			 WHEN us_precise_score_value BETWEEN 580 AND 618 THEN 'US03'
			 WHEN us_precise_score_value BETWEEN 619 AND 680 THEN 'US02'
			 WHEN us_precise_score_value > 680 THEN 'US01'
		ELSE 'N/A'
		END AS us_precise_score_rating,
		CASE WHEN us_precise_score_rating = 'US01' THEN 0.614
			 WHEN us_precise_score_rating = 'US02' THEN 0.545
			 WHEN us_precise_score_rating = 'US03' THEN 0.457
			 WHEN us_precise_score_rating = 'US04' THEN 0.446
			 WHEN us_precise_score_rating = 'US05' THEN 0.393
			 WHEN us_precise_score_rating = 'US06' THEN 0.392
			 WHEN us_precise_score_rating = 'US07' THEN 0.318
			 WHEN us_precise_score_rating = 'US08' THEN 0.284
			 WHEN us_precise_score_rating = 'US09' THEN 0.20
			 WHEN us_precise_score_rating = 'US10' THEN 0.137
		ELSE 0.0
		END::decimal(18,6) AS us_precise_avg_pd ,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY created_at DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY provider_query_id ORDER BY updated_at DESC) AS row_no_id
	FROM precise_raw ;
	
DROP TABLE IF EXISTS united_states_fico;
CREATE TEMP TABLE united_states_fico
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS 
WITH fico_pre AS 
(
SELECT 
		id::TEXT,
		customer_id::int AS customer_id,
		order_id,
		to_date(created_at, 'YYYY-MM-DD')::date AS score_date,
		updated_at,
		score::decimal(38,2) as score
 FROM ods_production.us_experian_data 
 UNION
 SELECT 
		id::TEXT,
		customer_id::int AS customer_id,
		order_id,
		to_date(created_at, 'YYYY-MM-DD')::date AS score_date,
		updated_at,
		 case when score = 'null' then NULL else score::decimal(38,2) end as score
FROM s3_spectrum_kafka_topics_raw.risk_internal_us_experian_credit_report_v2
 where to_date(created_at, 'YYYY-MM-DD')::date  >= '2023-01-30'
)
SELECT 
		id AS provider_query_id,
		customer_id::int AS customer_id,
		order_id,
		score_date,
		'us_fico' AS score_provider ,
		CASE WHEN score is not null THEN score END AS us_fico_score_value,
		CASE WHEN score IS NULL OR score < 300 THEN '1. <300 or Blank'
			 WHEN score between 300 and 324 then '2. 300-324'
			 WHEN score between 325 and 349 then '3. 325-349'
			 WHEN score between 350 and 374 then '4. 350-374'
			 WHEN score between 375 and 399 then '5. 375-399'
			 WHEN score between 400 and 424 then '6. 400-424'
			 WHEN score between 425 and 449 then '7. 425-449'
			 WHEN score between 450 and 474 then '8. 450-474'
			 WHEN score between 475 and 499 then '9. 475-499'
			 WHEN score between 500 and 524 then '10. 500-524'
			 WHEN score between 525 and 549 then '11. 525-549'
			 WHEN score between 550 and 574 then '12. 550-574'
			 WHEN score between 575 and 599 then '13. 575-599'
			 WHEN score between 600 and 624 then '14. 600-624'
			 WHEN score between 625 and 649 then '15. 625-649'
			 WHEN score between 650 and 674 then '16. 650-674'
			 WHEN score between 675 and 699 then '17. 675-699'
			 WHEN score between 700 and 724 then '18. 700-724'
		 	 WHEN score between 725 and 749 then '19. 725-749'
			 WHEN score between 750 and 774 then '20. 750-774'
			 WHEN score between 775 and 799 then '21. 775-799'
			 WHEN score between 800 and 824 then '22. 800-824'
			 WHEN score between 825 and 849 then '23. 825-849'
			 WHEN score between 850 and 874 then '24. 850-874'
			 WHEN score between 9000 and 9024 then '25. 9000-9024'
		ELSE 'N/A'
		END AS us_fico_score_rating ,
		0.0 :: decimal(18,6) AS us_fico_avg_pd,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY score_date DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM fico_pre ;


DROP TABLE IF EXISTS global_seon_fraud;
CREATE TEMP TABLE global_seon_fraud
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		seon_id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		score_created_at::date AS score_date,
		'Seon' AS score_provider,
		seon_fraud_score::decimal(38,2) AS global_seon_fraud_score,
		'N/A' AS rating_field,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY score_date DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER(PARTITION BY seon_id ORDER BY updated_at DESC) AS row_no_id
	FROM ods_production.seon_fraud_scores_curated s ; 


DROP TABLE IF EXISTS eu_ekata_fraud;
CREATE TEMP TABLE eu_ekata_fraud
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'Ekata' AS score_provider,
		identity_risk_score::decimal(38,2) AS eu_ekata_identity_risk_score,
		identity_network_score::decimal(18,6) AS eu_ekata_identity_network_score,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY score_date DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM stg_order_approval.ekata_eu_transaction_risk_data ; 


DROP TABLE IF EXISTS us_clarity_score;
CREATE TEMP TABLE us_clarity_score
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'Experian-Clarity' AS score_provider,
		CASE WHEN score != 'null' THEN score::DECIMAL(38,2) END AS us_clarity_score_value,
		CASE
			WHEN us_clarity_score_value IS NULL THEN 'BLANK'
			WHEN us_clarity_score_value = 9001 THEN '01' -- DECEASED.
			WHEN us_clarity_score_value < 440 THEN '02'
			WHEN us_clarity_score_value BETWEEN 440 AND 459 THEN '03'
			WHEN us_clarity_score_value BETWEEN 460 AND 479 THEN '04'
			WHEN us_clarity_score_value BETWEEN 480 AND 499 THEN '05'
			WHEN us_clarity_score_value BETWEEN 500 AND 519 THEN '06'
			WHEN us_clarity_score_value BETWEEN 520 AND 539 THEN '07'
			WHEN us_clarity_score_value BETWEEN 540 AND 559 THEN '08'
			WHEN us_clarity_score_value BETWEEN 560 AND 579 THEN '09'
			WHEN us_clarity_score_value BETWEEN 580 AND 599 THEN '10'
			WHEN us_clarity_score_value BETWEEN 600 AND 619 THEN '11'
			WHEN us_clarity_score_value >= 620 THEN '12'
		END AS us_clarity_score_rating,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY score_date DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_kafka_topics_raw.risk_internal_us_clarity_services_us_v1 ; 

DROP TABLE IF EXISTS us_vantage_score;
CREATE TEMP TABLE us_vantage_score
DISTKEY(provider_query_id)
SORTKEY(provider_query_id) AS
	SELECT
		id::TEXT AS provider_query_id,
		customer_id::int,
		order_id,
		created_at::date AS score_date,
		'Vantage' AS score_provider,
		CASE 
			WHEN POSITION ('score' IN raw_data ) != 0
				AND SUBSTRING(raw_data, POSITION( 'modelIndicator' IN raw_data ) + 18, 2) = 'V4'
					THEN SUBSTRING( raw_data, POSITION ('score' IN raw_data ) + 9 , 4 )
		END::DECIMAL(38,2) AS us_vantage_score_value,
		CASE
			WHEN us_vantage_score_value = 1 THEN '01'
			WHEN us_vantage_score_value = 4 THEN '02'
			WHEN us_vantage_score_value < 420 THEN '03'
			WHEN us_vantage_score_value < 440 THEN '04'
			WHEN us_vantage_score_value < 460 THEN '05'
			WHEN us_vantage_score_value < 480 THEN '06'
			WHEN us_vantage_score_value < 500 THEN '07'
			WHEN us_vantage_score_value < 520 THEN '08'
			WHEN us_vantage_score_value < 540 THEN '09'
			WHEN us_vantage_score_value < 580 THEN '10'
			WHEN us_vantage_score_value < 620 THEN '11'
			WHEN us_vantage_score_value < 660 THEN '12'
			WHEN us_vantage_score_value < 700 THEN '13'
			WHEN us_vantage_score_value < 740 THEN '14'
			WHEN us_vantage_score_value < 780 THEN '15'
			WHEN us_vantage_score_value >= 780 THEN '16'
			ELSE 'BLANK'
		END AS us_vantage_score_rating,
		ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY score_date DESC, updated_at DESC) AS row_no,
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_no_id
	FROM s3_spectrum_kafka_topics_raw.risk_internal_us_experian_credit_report_v3 ; 


DROP TABLE IF EXISTS ods_data_sensitive.external_provider_order_score;
CREATE TABLE ods_data_sensitive.external_provider_order_score AS
WITH union_ AS (
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM spain_experian_delphi WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM spain_equifax WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM germany_schufa WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM de_at_crif WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM netherlands_experian WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM netherlands_focum WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM united_states_precise WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM united_states_fico WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM global_seon_fraud WHERE row_no = 1 OR order_id IS NULL 
	UNION ALL 
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM eu_ekata_fraud WHERE row_no = 1 OR order_id IS NULL
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM us_clarity_score WHERE row_no = 1 OR order_id IS NULL
	UNION ALL
	SELECT provider_query_id, customer_id, order_id, score_provider, score_date
	FROM us_vantage_score WHERE row_no = 1 OR order_id IS NULL
)
, score_ranges AS (
	SELECT
		u.provider_query_id,
		u.customer_id,
		u.order_id,
		u.score_date,
		COALESCE(DATEADD(DAY, -1, LEAD(u.score_date) OVER (PARTITION BY u.customer_id, u.score_provider ORDER BY u.score_date)), '9999-12-31')::date AS next_score_date,
		u.score_provider
	FROM union_ u
)
, order_score_matching AS 
(
SELECT DISTINCT
	o.customer_id,
	o.order_id ,
	o.store_country, 
	o.submitted_date,
	-- Add All Provider IDs
	COALESCE(experian_spain_o.provider_query_id, experian_spain_s.provider_query_id) AS es_delphi_id,
	COALESCE(equifax_spain_o.provider_query_id, equifax_spain_s.provider_query_id) AS es_equifax_id,
	COALESCE(germany_schufa_o.provider_query_id, germany_schufa_s.provider_query_id) AS de_schufa_id, 
	COALESCE(at_de_crif_o.provider_query_id, at_de_crif_s.provider_query_id) AS at_de_crif_id,
	COALESCE(netherlands_focum_o.provider_query_id, netherlands_focum_s.provider_query_id) AS nl_focum_id,
	COALESCE(netherlands_experian_o.provider_query_id, netherlands_experian_s.provider_query_id) AS nl_experian_id,
	COALESCE(united_states_precise_o.provider_query_id, united_states_precise_s.provider_query_id) AS us_precise_id,
	COALESCE(united_states_fico_o.provider_query_id, united_states_fico_s.provider_query_id) AS us_fico_id,
	COALESCE(global_seon_fraud_o.provider_query_id, global_seon_fraud_s.provider_query_id) AS global_seon_id,
	COALESCE(eu_ekata_fraud_o.provider_query_id, eu_ekata_fraud_s.provider_query_id) AS eu_ekata_id,
	COALESCE(us_clarity_o.provider_query_id, us_clarity_s.provider_query_id) AS us_clarity_id,
	COALESCE(us_vantage_o.provider_query_id, us_vantage_s.provider_query_id) AS us_vantage_id
FROM ods_production.order o
LEFT JOIN score_ranges experian_spain_o
    ON o.customer_id = experian_spain_o.customer_id
   AND o.order_id = experian_spain_o.order_id
   AND o.store_country = 'Spain'
   AND experian_spain_o.score_provider = 'es_experian'
LEFT JOIN score_ranges experian_spain_s
    ON o.customer_id = experian_spain_s.customer_id
   AND o.submitted_date BETWEEN experian_spain_s.score_date AND experian_spain_s.next_score_date
   AND o.store_country = 'Spain'
   AND experian_spain_s.score_provider = 'es_experian'
LEFT JOIN score_ranges equifax_spain_o
    ON o.customer_id = equifax_spain_o.customer_id
   AND o.order_id = equifax_spain_o.order_id
   AND o.store_country = 'Spain'
   AND equifax_spain_o.score_provider = 'es_equifax'
LEFT JOIN score_ranges equifax_spain_s
    ON o.customer_id = equifax_spain_s.customer_id
   AND o.submitted_date BETWEEN equifax_spain_s.score_date AND equifax_spain_s.next_score_date
   AND o.store_country = 'Spain'
   AND equifax_spain_s.score_provider = 'es_equifax'
LEFT JOIN score_ranges germany_schufa_o
    ON o.customer_id = germany_schufa_o.customer_id
   AND o.order_id = germany_schufa_o.order_id
   AND o.store_country = 'Germany'
   AND germany_schufa_o.score_provider = 'de_schufa'
LEFT JOIN score_ranges germany_schufa_s
    ON o.customer_id = germany_schufa_s.customer_id
   AND o.submitted_date BETWEEN germany_schufa_s.score_date AND germany_schufa_s.next_score_date
   AND o.store_country = 'Germany'
   AND germany_schufa_s.score_provider = 'de_schufa'
LEFT JOIN score_ranges at_de_crif_o
    ON o.customer_id = at_de_crif_o.customer_id
   AND o.order_id = at_de_crif_o.order_id
   AND o.store_country IN ('Germany', 'Austria')
   AND at_de_crif_o.score_provider = 'de_at_Crif'
LEFT JOIN score_ranges at_de_crif_s
    ON o.customer_id = at_de_crif_s.customer_id
   AND o.submitted_date BETWEEN at_de_crif_s.score_date AND at_de_crif_s.next_score_date
   AND o.store_country IN ('Germany', 'Austria')
   AND at_de_crif_s.score_provider = 'de_at_Crif'
LEFT JOIN score_ranges netherlands_focum_o
    ON o.customer_id = netherlands_focum_o.customer_id
   AND o.order_id = netherlands_focum_o.order_id
   AND o.store_country = 'Netherlands'
   AND netherlands_focum_o.score_provider = 'nl_focum'
LEFT JOIN score_ranges netherlands_focum_s
    ON o.customer_id = netherlands_focum_s.customer_id
   AND o.submitted_date BETWEEN netherlands_focum_s.score_date AND netherlands_focum_s.next_score_date
   AND o.store_country = 'Netherlands'
   AND netherlands_focum_s.score_provider = 'nl_focum'
LEFT JOIN score_ranges netherlands_experian_o
    ON o.customer_id = netherlands_experian_o.customer_id
   AND o.order_id = netherlands_experian_o.order_id
   AND o.store_country = 'Netherlands'
   AND netherlands_experian_o.score_provider = 'nl_experian'
LEFT JOIN score_ranges netherlands_experian_s
    ON o.customer_id = netherlands_experian_s.customer_id
   AND o.submitted_date BETWEEN netherlands_experian_s.score_date AND netherlands_experian_s.next_score_date
   AND o.store_country = 'Netherlands'
   AND netherlands_experian_s.score_provider = 'nl_experian'
LEFT JOIN score_ranges united_states_precise_o
    ON o.customer_id = united_states_precise_o.customer_id
   AND o.order_id = united_states_precise_o.order_id
   AND o.store_country = 'United States'
   AND united_states_precise_o.score_provider = 'us_precise'
LEFT JOIN score_ranges united_states_precise_s
    ON o.customer_id = united_states_precise_s.customer_id
   AND o.submitted_date BETWEEN united_states_precise_s.score_date AND united_states_precise_s.next_score_date
   AND o.store_country = 'United States'
   AND united_states_precise_s.score_provider = 'us_precise'
LEFT JOIN score_ranges united_states_fico_o
    ON o.customer_id = united_states_fico_o.customer_id
   AND o.order_id = united_states_fico_o.order_id
   AND o.store_country = 'United States'
   AND united_states_fico_o.score_provider = 'us_fico'
LEFT JOIN score_ranges united_states_fico_s
    ON o.customer_id = united_states_fico_s.customer_id
   AND o.submitted_date BETWEEN united_states_fico_s.score_date AND united_states_fico_s.next_score_date
   AND o.store_country = 'United States'
   AND united_states_fico_s.score_provider = 'us_fico'
LEFT JOIN score_ranges global_seon_fraud_o
    ON o.customer_id = global_seon_fraud_o.customer_id
   AND o.order_id = global_seon_fraud_o.order_id
   AND global_seon_fraud_o.score_provider = 'Seon'
LEFT JOIN score_ranges global_seon_fraud_s
    ON o.customer_id = global_seon_fraud_s.customer_id
   AND o.submitted_date BETWEEN global_seon_fraud_s.score_date AND global_seon_fraud_s.next_score_date
   AND global_seon_fraud_s.score_provider = 'Seon'
LEFT JOIN score_ranges eu_ekata_fraud_o
    ON o.customer_id = eu_ekata_fraud_o.customer_id
   AND o.order_id = eu_ekata_fraud_o.order_id
   AND eu_ekata_fraud_o.score_provider = 'Ekata'
LEFT JOIN score_ranges eu_ekata_fraud_s
    ON o.customer_id = eu_ekata_fraud_s.customer_id
   AND o.submitted_date BETWEEN eu_ekata_fraud_s.score_date AND eu_ekata_fraud_s.next_score_date
   AND eu_ekata_fraud_s.score_provider = 'Ekata'  
LEFT JOIN score_ranges us_clarity_o
    ON o.customer_id = us_clarity_o.customer_id
   AND o.order_id = us_clarity_o.order_id
   AND us_clarity_o.score_provider = 'Experian-Clarity'
LEFT JOIN score_ranges us_clarity_s
    ON o.customer_id = us_clarity_s.customer_id
   AND o.submitted_date BETWEEN us_clarity_s.score_date AND us_clarity_s.next_score_date
   AND us_clarity_s.score_provider = 'Experian-Clarity'
LEFT JOIN score_ranges us_vantage_o
    ON o.customer_id = us_vantage_o.customer_id
   AND o.order_id = us_vantage_o.order_id
   AND us_vantage_o.score_provider = 'Vantage'
LEFT JOIN score_ranges us_vantage_s
    ON o.customer_id = us_vantage_s.customer_id
   AND o.submitted_date BETWEEN us_vantage_s.score_date AND us_vantage_s.next_score_date
   AND us_vantage_s.score_provider = 'Vantage'
WHERE o.submitted_date IS NOT NULL
  AND o.store_country IN ('Spain', 'Germany', 'Austria', 'Netherlands', 'United States')
  AND (es_delphi_id IS NOT NULL
	   OR es_equifax_id IS NOT NULL
	   OR de_schufa_id IS NOT NULL
	   OR at_de_crif_id IS NOT NULL
	   OR nl_focum_id IS NOT NULL
	   OR nl_experian_id IS NOT NULL
	   OR us_precise_id IS NOT NULL
	   OR us_fico_id IS NOT NULL
	   OR global_seon_id IS NOT NULL
	   OR eu_ekata_id IS NOT NULL
	   OR us_clarity_id IS NOT NULL
	   OR us_vantage_id IS NOT NULL
      ))
SELECT
	sm.customer_id,
	sm.order_id ,
	sm.submitted_date,
	-- ES Experian Delphi
	sm.es_delphi_id,
	sed.es_experian_score_value,
	sed.es_experian_score_rating,
	sed.es_experian_avg_pd,
	-- ES Equifax
	sm.es_equifax_id,
	se.es_equifax_score_value,
	se.es_equifax_score_rating,
	se.es_equifax_avg_pd,
	-- DE Schufa
	sm.de_schufa_id,
	ds.de_schufa_score_value,
	ds.de_schufa_score_rating,
	ds.de_schufa_avg_pd,
	ds.de_schufa_is_person_known,
	ds.schufa_score_details,
	ds.schufa_range_text,
	-- DE CRIF
	CASE WHEN store_country = 'Germany' THEN sm.at_de_crif_id END AS de_crif_id, 
	CASE WHEN store_country = 'Germany' THEN cf.crif_score_value END AS de_crif_score_value,
	NULL AS de_crif_score_rating,
	NULL AS de_crif_avg_pd, -- ALSO NULL.
	CASE WHEN store_country = 'Germany' THEN cf.crif_is_person_known END AS de_crif_is_person_known,
	CASE WHEN store_country = 'Germany' THEN cf.crif_is_address_known END AS de_crif_is_address_known,
	--	 AT CRIF
	CASE WHEN store_country = 'Austria' THEN sm.at_de_crif_id END AS at_crif_id, 
	CASE WHEN store_country = 'Austria' THEN cf.crif_score_value END AS at_crif_score_value,
	CASE WHEN store_country = 'Austria' THEN 
		CASE
			WHEN (cf.crif_score_value) <= 250 THEN '<= 250'
			WHEN (cf.crif_score_value) <= 300 THEN '<= 300'
			WHEN (cf.crif_score_value) <= 370 THEN '<= 370'
			WHEN (cf.crif_score_value) <= 400 THEN '<= 400'
			WHEN (cf.crif_score_value) <= 450 THEN '<= 450'
			WHEN (cf.crif_score_value) <= 500 THEN '<= 500'
			WHEN (cf.crif_score_value) <= 550 THEN '<= 550'
			WHEN (cf.crif_score_value) > 550 THEN '> 550'
			ELSE '99- Error'
		END
	END AS at_crif_score_rating,
	NULL AS at_crif_avg_pd, -- ALSO NULL.
	CASE WHEN store_country = 'Austria' THEN cf.crif_is_person_known END AS at_crif_is_person_known,
	CASE WHEN store_country = 'Austria' THEN cf.crif_is_address_known END AS at_crif_is_address_known,
	-- NL FOCUM
	sm.nl_focum_id,
	nlf.nl_focum_score_value,
	nlf.nl_focum_score_rating,
	nlf.nl_focum_avg_pd,
	-- NL EXPERIAN
	sm.nl_experian_id,
	nle.nl_experian_score_value,
	nle.nl_experian_score_rating,
	nle.nl_experian_avg_pd,
	nle.nl_exp_is_person_known,
	nle.nl_exp_is_address_known,
	-- US PRECISE
	sm.us_precise_id,
	usp.us_precise_score_value,
	usp.us_precise_score_rating,
	usp.us_precise_avg_pd,
	-- US FICO
	sm.us_fico_id,
	usf.us_fico_score_value,
	usf.us_fico_score_rating,
	-- GLOBAL SEON
	sm.global_seon_id,
	gs.global_seon_fraud_score,
	-- EU EKATA
	sm.eu_ekata_id,
	ek.eu_ekata_identity_risk_score,
	ek.eu_ekata_identity_network_score,
	-- US CLARITY
	sm.us_clarity_id,
	cl.us_clarity_score_value, ---- CHECK IF VALUE INCLUDED OR NOT 
	cl.us_clarity_score_rating,
	-- US VANTAGE 
	sm.us_vantage_id,
	va.us_vantage_score_value,
	va.us_vantage_score_rating
FROM order_score_matching sm
LEFT JOIN spain_experian_delphi sed
	ON sed.provider_query_id = sm.es_delphi_id
	AND sed.row_no_id = 1
LEFT JOIN spain_equifax se
	ON se.provider_query_id = sm.es_equifax_id
	AND se.row_no_id = 1
LEFT JOIN germany_schufa ds
	ON ds.provider_query_id = sm.de_schufa_id
	AND ds.row_no_id = 1
LEFT JOIN de_at_crif cf
	ON cf.provider_query_id = sm.at_de_crif_id
	AND cf.row_no_id = 1	
LEFT JOIN netherlands_focum nlf
	ON nlf.provider_query_id = sm.nl_focum_id
	AND nlf.row_no_id = 1
LEFT JOIN netherlands_experian nle
	ON nle.provider_query_id = sm.nl_experian_id
	AND nle.row_no_id = 1
LEFT JOIN united_states_precise usp
	ON usp.provider_query_id = sm.us_precise_id
	AND usp.row_no_id = 1
LEFT JOIN united_states_fico usf
	ON usf.provider_query_id = sm.us_fico_id
	AND usf.row_no_id = 1 
LEFT JOIN global_seon_fraud gs
	ON gs.provider_query_id = sm.global_seon_id
	AND gs.row_no_id = 1
LEFT JOIN eu_ekata_fraud ek
	ON ek.provider_query_id = sm.eu_ekata_id
	AND ek.row_no_id = 1
LEFT JOIN us_clarity_score cl
	ON cl.provider_query_id = sm.us_clarity_id
	AND cl.row_no_id = 1
LEFT JOIN us_vantage_score va
	ON va.provider_query_id = sm.us_vantage_id
	AND va.row_no_id = 1
WHERE 
	(sed.es_experian_score_value IS NOT NULL
	OR se.es_equifax_score_value IS NOT NULL
	OR ds.de_schufa_score_value IS NOT NULL
	OR cf.crif_score_value IS NOT NULL 
	OR nlf.nl_focum_score_value IS NOT NULL
	OR nle.nl_experian_score_value IS NOT NULL
	OR usp.us_precise_score_value IS NOT NULL
	OR usf.us_fico_score_value IS NOT NULL
	OR gs.global_seon_fraud_score IS NOT NULL
	OR ek.eu_ekata_identity_risk_score IS NOT NULL
	OR ek.eu_ekata_identity_network_score IS NOT NULL
	OR cl.us_clarity_score_value IS NOT NULL
	OR va.us_vantage_score_value IS NOT NULL)
;

grant select on ods_data_sensitive.external_provider_order_score TO group risk_users,risk_users_redash;
