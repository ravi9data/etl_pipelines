DROP TABLE IF EXISTS burgel;
CREATE TEMP TABLE burgel
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS (
SELECT
	customer_id::int as customer_id ,
	updated_at::TIMESTAMP WITHOUT TIME ZONE ,
	burgel_score,
	score_details,
	CASE 
		WHEN person_known = 'True' THEN TRUE
		WHEN person_known = 'False' THEN FALSE
		ELSE NULL
	END::bool AS person_known,
	CASE 
		WHEN person_known = 'True' THEN TRUE
		WHEN person_known = 'False' THEN FALSE
		ELSE NULL
	END::bool AS address_known,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num 
FROM stg_curated.burgel_data
)
SELECT
	customer_id,
	updated_at,
	burgel_score,
	score_details AS burgel_score_details,
	person_known AS burgel_person_known,
	address_known AS burgel_address_details
FROM
	CTE
WHERE
	row_num=1;

--boniversum data preparation
DROP TABLE IF EXISTS boniversum;
CREATE TEMP TABLE boniversum
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS
(
SELECT
	customer_id::int as customer_id ,
	updated_at::TIMESTAMP WITHOUT TIME ZONE  ,
	person_ident::TEXT AS person_ident,
	post_address_validation::TEXT AS post_address_validation,
	verita_score::int as verita_score,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num
FROM stg_curated.boniversum_data)
SELECT
	customer_id,
	updated_at,
	CASE
    	WHEN (person_ident::TEXT IN ('02', '03') AND post_address_validation <> '03') THEN True
        ELSE False
    END AS verita_person_known_at_address,
	verita_score
FROM
	CTE
WHERE
	row_num=1;


--schufa data preparation
DROP TABLE IF EXISTS schufa;
CREATE TEMP TABLE schufa
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS 
(
SELECT
	customer_id,
	updated_at::TIMESTAMP WITHOUT TIME ZONE  ,
	schufa_class,
	CASE
		WHEN schufa_score='' THEN NULL --Sometime we get empty strings from schufa, IF this is the case we turn them into NULLs to avoid type incompatibilities
		ELSE schufa_score::INT
		END AS schufa_score,
	is_identity_confirmed AS schufa_identity_confirmed,
	is_negative_remarks AS schufa_is_negative_remarks,
	schufa_paid_debt AS schufa_paid_debt,
	schufa_total_debt AS schufa_total_debt,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY creation_timestamp DESC) AS row_num
FROM ods_data_sensitive.schufa
)
SELECT
	customer_id,
	updated_at,
	schufa_class,
	schufa_score,
	schufa_identity_confirmed,
	schufa_is_negative_remarks,
	schufa_paid_debt,
	schufa_total_debt
FROM
	CTE
WHERE
	row_num=1;
	

--crifburgel data preparation
DROP TABLE IF EXISTS crifburgel;
CREATE TEMP TABLE crifburgel
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS (
SELECT
	customer_id::int as customer_id ,
	country_code,
	score_value,
	CASE 
		WHEN person_known = 'True' THEN TRUE
		WHEN person_known = 'False' THEN FALSE
		ELSE NULL
	END::bool AS person_known,
	score_decision,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num
FROM stg_curated.crifburgel_data)
SELECT
	customer_id,
	country_code AS burgelcrif_country_code,
	score_value AS burgelcrif_score_value,
	person_known AS burgelcrif_person_known,
	score_decision AS burgelcrif_score_decision
FROM
	CTE
WHERE
	row_num=1;


--experian data preparation - Netherlands
DROP TABLE IF EXISTS experian;
CREATE TEMP TABLE experian
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS (
SELECT
	customer_id::int as customer_id ,
	updated_at,
	score_value,
	CASE 
		WHEN person_known = 'True' THEN TRUE
		WHEN person_known = 'False' THEN FALSE
		ELSE NULL
	END::bool AS person_known,
	address_known,
	person_in_address,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num
FROM stg_curated.experian_data)
SELECT
	customer_id,
	updated_at AS updated_at,
	cast(round(score_value,0) as int) AS experian_score,
	person_known AS experian_person_known,
	address_known AS experian_address_known,
	person_in_address AS experian_person_known_at_address
FROM
	CTE
WHERE
	row_num=1;


--delphi data preparation - Spain .
DROP TABLE IF EXISTS delphi;
CREATE TEMP TABLE delphi
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS (
SELECT
	customer_id,
	updated_at::timestamp as updated_at,
	score_value,
	score_note,
	score_probability,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at::timestamp DESC) AS row_num
FROM stg_curated.experian_es_delphi_data
)
SELECT
	customer_id,
	updated_at,
	cast(round(score_value,0) as int) AS delphi_score_value,
	score_note AS delphi_score,
	score_probability AS delphi_probability
FROM
	CTE
WHERE
	row_num=1;



-- Spain - Equifax data preparation
DROP TABLE IF EXISTS es_equifax;
CREATE TEMP TABLE es_equifax
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS (
SELECT
	customer_id,
	updated_at,
	score_value::FLOAT,
    agg_unpaid_products,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num
FROM stg_curated.equifax_risk_score_data )
SELECT
	customer_id,
	updated_at,
	score_value AS equifax_score,
	CASE 
		WHEN score_value BETWEEN -9999 AND 6 THEN 'ES08'
		WHEN score_value BETWEEN 7 AND 29 THEN 'ES07'
		WHEN score_value BETWEEN 30 AND 292 THEN 'ES06'
		WHEN score_value BETWEEN 293 AND 520 THEN 'ES05'
		WHEN score_value BETWEEN 521 AND 628 THEN 'ES04'
		WHEN score_value BETWEEN 629 AND 779 THEN 'ES03'
		WHEN score_value BETWEEN 780 AND 827 THEN 'ES02'
		WHEN score_value BETWEEN 828 AND 9999 THEN 'ES01'
		ELSE 'N/A'
	END AS equifax_rating,
	agg_unpaid_products
FROM
	CTE
WHERE
	row_num=1;



-- Netherlands - Focum data preparation
DROP TABLE IF EXISTS nl_focum;
CREATE TEMP TABLE nl_focum
SORTKEY(customer_id)
DISTSTYLE EVEN
AS
WITH CTE AS (
SELECT
	customer_id,
	updated_at,
	risk_score::FLOAT,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num
FROM stg_order_approval.focum_data)
SELECT
	customer_id,
	updated_at,
	risk_score AS focum_score,
	CASE 
		WHEN risk_score BETWEEN -90 AND 590 THEN 'NL10'
		WHEN risk_score BETWEEN 591 AND 703 THEN 'NL09'
		WHEN risk_score BETWEEN 704 AND 783 THEN 'NL08'
		WHEN risk_score BETWEEN 784 AND 848 THEN 'NL07'
		WHEN risk_score BETWEEN 849 AND 898 THEN 'NL06'
		WHEN risk_score BETWEEN 899 AND 937 THEN 'NL05'
		WHEN risk_score BETWEEN 938 AND 953 THEN 'NL04'
		WHEN risk_score BETWEEN 954 AND 968 THEN 'NL03'
		WHEN risk_score BETWEEN 969 AND 981 THEN 'NL02'
		WHEN risk_score BETWEEN 982 AND 9999 THEN 'NL01'
		ELSE 'N/A'
	END AS focum_rating
FROM
	CTE
WHERE
	row_num=1 ;


--preaparing risk data. 1 row per customer_id
DROP TABLE IF EXISTS customer_risk;
CREATE TEMP TABLE customer_risk
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    u.id AS customer_id,
    cf.orders AS orders_count,
    cf.subscriptions AS subscriptions_count,
    cf.first_paid_payments,
    cf.first_failed_payments,
    cf.recurring_paid_payments,
    cf.recurring_failed_payments,
    cf.recurring_paid_months,
    cf.paid_amount,
    cl.customer_label AS customer_label_old,
    cl2.label AS customer_label_new
FROM stg_api_production.spree_users AS u
LEFT JOIN stg_detectingfrankfurt.customer_features AS cf ON u.id = cf.customer_id
LEFT JOIN stg_detectingfrankfurt.customer_labels AS cl ON u.id = cl.customer_id
LEFT JOIN stg_order_approval.customer_labels3 AS cl2 ON u.id = cl2.customer_id;

--nethone data preparation
DROP TABLE IF EXISTS stg_last_value_nethone;
CREATE TEMP TABLE stg_last_value_nethone
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	signal_risk_category AS nethone_risk_category,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS row_num
FROM data_science_dev.nethone_signal_risk_categories_order;

--getting only most up to date row from delphi data per customer
DROP TABLE IF EXISTS nethone;
CREATE TEMP TABLE nethone
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	nethone_risk_category
FROM
	stg_last_value_nethone
WHERE
	row_num=1;

--Getting limits from EU and US risk-credit-limit services
DROP TABLE IF EXISTS stg_eu_us_decision_tree;
CREATE TEMP TABLE stg_eu_us_decision_tree
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	subscription_limit,
	comment,
	to_timestamp(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at
FROM
	stg_curated.risk_us_customer_credit_limit_v1
UNION
SELECT
	customer_id,
	subscription_limit,
	comment,
	to_timestamp(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at
FROM
	stg_curated.risk_eu_customer_credit_limit_v1;

-- Deduplicating risk-credit-limit data
DROP TABLE IF EXISTS stg_decision_tree;
CREATE TEMP TABLE stg_decision_tree
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id::int as customer_id ,
	subscription_limit::int AS subscription_limit,
	comment AS main_reason,
	updated_at AS creation_timestamp,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY creation_timestamp DESC) AS row_num
FROM
	stg_eu_us_decision_tree;

--Getting the lastest data from all risk-credit-limit services
DROP TABLE IF EXISTS decision_tree;
CREATE TEMP TABLE decision_tree
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	subscription_limit,
	main_reason,
	creation_timestamp
FROM
	stg_decision_tree
WHERE
	row_num=1;


-- Getting tags applied by Risk
DROP TABLE IF EXISTS tags_from_risk;
CREATE TEMP TABLE tags_from_risk
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	tag_id,
	tag_name,
	action_type,
	row_number() over (partition by customer_id, tag_id order BY created_at desc) as row_n
FROM stg_curated.risk_customer_tags_apply_v1;

--- Is the customer is_blacklisted or is_whitelisted, 1 row per customer.
DROP TABLE IF EXISTS risk_tags;
CREATE TEMP TABLE risk_tags
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	su.id AS customer_id,
	LISTAGG(
		DISTINCT ct.tag_name::VARCHAR,
		', '
	) AS tag_name,
	BOOL_OR(CASE
                WHEN ct.tag_id = 1 THEN TRUE
                WHEN acc.is_blacklisted__c IS TRUE THEN TRUE
                ELSE FALSE
            END) AS is_blacklisted,
	BOOL_OR(CASE
                WHEN ct.tag_id = 2 THEN TRUE
                WHEN acc.is_whitelisted__c IS TRUE THEN TRUE
                ELSE FALSE
            END) AS is_whitelisted
FROM
	stg_api_production.spree_users AS su
LEFT JOIN stg_salesforce."account" AS acc ON
	su.id = acc.spree_customer_id__c::INT
LEFT JOIN tags_from_risk AS ct ON
	su.id = ct.customer_id
	AND ct.row_n = 1
	AND ct.action_type = 'apply'
GROUP BY
	su.id;

--- Customers that went through id_verification_order
DROP TABLE IF EXISTS id_verification_risk;
CREATE TEMP TABLE id_verification_risk
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
     DISTINCT ivo.customer_id
FROM stg_curated.id_verification_order AS ivo;

--- Customer id verification results
DROP TABLE IF EXISTS id_verification_applicants;
CREATE TEMP TABLE id_verification_applicants
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    iva.user_id AS customer_id,
    iva.remote_id,
    iva."result",
    iva.raw_result,
    CASE
        WHEN iva.verification_type = 'id_verification'
            THEN 'Onfido'
        WHEN iva.verification_type = 'bank_account_snapshot'
            THEN 'FinTecSystems'
        ELSE iva.verification_type
    END AS verification_type
FROM (
	SELECT
		a.user_id,
		c.remote_id,
		c."result",
        c.raw_result,
        c.verification_type,
		row_number() OVER (PARTITION BY a.user_id ORDER BY c.updated_at DESC) AS rn
    FROM
    	stg_api_production.applicants AS a
      	LEFT JOIN stg_api_production.checks AS c ON a.id = c.applicant_id) as iva
WHERE iva.rn = 1;

--- Company id verification results
DROP TABLE IF EXISTS id_verification_applicants_b2b;
CREATE TEMP TABLE id_verification_applicants_b2b
SORTKEY(company_id)
DISTKEY(company_id)
AS
SELECT
    iva.company_id,
    iva.remote_id,
    iva."result",
    iva.raw_result,
    CASE
        WHEN iva.verification_type = 'id_verification'
            THEN 'Onfido'
        WHEN iva.verification_type = 'bank_account_snapshot'
            THEN 'FinTecSystems'
        ELSE iva.verification_type
    END AS verification_type
FROM (
	SELECT
		a.company_id,
		c.remote_id,
		c."result",
        c.raw_result,
        c.verification_type,
		row_number() OVER (PARTITION BY a.company_id ORDER BY c.updated_at DESC) AS rn
    FROM
    	stg_api_production.applicants AS a
      	LEFT JOIN stg_api_production.checks AS c ON a.id = c.applicant_id) as iva
WHERE iva.rn = 1;

--Customer fraud detected
DROP TABLE IF EXISTS fraud;
CREATE TEMP TABLE fraud
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    u.id AS customer_id,
    LISTAGG(DISTINCT COALESCE(pf.fraud_type::TEXT, fl.fraud_type), ', ') WITHIN GROUP (ORDER BY LEAST(pf.creation_time, fl.creation_timestamp)) AS fraud_type,
    MIN(LEAST(pf.creation_time, fl.creation_timestamp)) AS min_fraud_detected,
    MAX(GREATEST(pf.creation_time, fl.creation_timestamp)) AS max_fraud_detected,
    MAX(GREATEST(u.updated_at, pf.updated_at, fl.updated_at)) AS updated_at
FROM
	stg_api_production.spree_users AS u
	LEFT JOIN stg_detectingfrankfurt.possible_fraudsters AS pf ON u.id = pf.customer_id
	LEFT JOIN stg_detectingfrankfurt.fraud_links AS fl ON u.id = fl.new_customer_id
GROUP BY
	u.id
HAVING
	MIN(LEAST(pf.creation_time, fl.creation_timestamp)) IS NOT NULL;

--Preparing session data
SORTKEY(session_id)
DISTKEY(session_id)
AS
SELECT
	*
FROM

DROP TABLE IF EXISTS staging_sessions;
CREATE TEMP TABLE staging_sessions
SORTKEY(session_id)
DISTKEY(session_id)
AS
SELECT
	session_id,
	user_id,
	store_id
FROM
;

--Find all the unique customers who shared a session_id
DROP TABLE IF EXISTS cookie_count;
CREATE TEMP TABLE cookie_count
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
  glv.user_id AS customer_id,
  LISTAGG(DISTINCT glv2.user_id, ', ') AS web_users,
  (REGEXP_COUNT(web_users, ',') + 1) AS accounts_cookie,
  True AS multiple_accounts
FROM
	INNER JOIN staging_sessions AS glv2 ON
			glv.session_id = glv2.session_id
	    AND glv.user_id <> glv2.user_id
 	    AND glv.session_id IS NOT NULL
       	AND glv.session_id <> ''
	LEFT JOIN stg_api_production.spree_stores AS ss ON (glv.store_id = ss.id OR glv2.store_id = ss.id)
													 AND (NOT ss.offline OR ss.offline IS NULL)
WHERE (NOT ss.offline OR ss.offline IS NULL)
GROUP BY glv.user_id;

--find all the unique Ips who shared a customer_id
DROP TABLE IF EXISTS ip_count;
CREATE TEMP TABLE ip_count
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    so.user_id AS customer_id,
    COUNT(DISTINCT su.current_sign_in_ip) AS distinct_customers_with_ip
FROM stg_api_production.spree_orders AS so
INNER JOIN stg_api_production.spree_users AS su ON so.user_id = su.id
INNER JOIN stg_api_production.spree_stores AS ss ON so.store_id = ss.id
WHERE so.created_at > CURRENT_DATE - 5
AND NOT ss.offline
GROUP BY 1;

--Customer sorted Reviews data
DROP TABLE IF EXISTS sorted_reviews;
CREATE TEMP TABLE sorted_reviews
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    o.customer_id,
    mrd.order_id,
    mrd.reviewer,
    mrd.decision,
    mrd.reason,
    mrd."comment",
    mrd.synced_at,
    ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY mrd.synced_at DESC) AS grouprownum
FROM ods_data_sensitive.manual_review_decisions AS mrd
INNER JOIN ods_production."order" AS o ON mrd.order_id = o.order_id;

--Customer previous Reviews data
DROP TABLE IF EXISTS previous_reviews;
CREATE TEMP TABLE previous_reviews
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    customer_id,
    '['||listagg(CASE
                 	WHEN x.grouprownum < 6
                 	THEN '{"order_id" :"'||x.order_id||'","decision":"'||x.decision||'","reason":"'||x.reason||'","comment":"'||x."comment"||'","user":"'||x.reviewer||'","ts":'||extract('epoch' from x.synced_at)||'}'
                 	ELSE NULL
                END, ', ') WITHIN GROUP (ORDER BY x.synced_at DESC)||']' AS reviews
FROM sorted_reviews AS x
GROUP BY 1;

--PayPal data preparation
DROP TABLE IF EXISTS stg_paypal;
CREATE TEMP TABLE stg_paypal
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	user_id AS customer_id,
	meta,
	row_number() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
FROM stg_api_production.user_payment_methods
WHERE
	payment_gateway_id = 1
	AND status = 'published'
    AND (JSON_EXTRACT_PATH_TEXT(meta, 'verified') IS NOT NULL OR JSON_EXTRACT_PATH_TEXT(meta, 'address_confirmed') IS NOT NULL);

--Customer PayPal data
DROP TABLE IF EXISTS paypal_meta;
CREATE TEMP TABLE paypal_meta
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	CASE
	    WHEN JSON_EXTRACT_PATH_TEXT(meta, 'verified') = 'true' THEN True
	    WHEN JSON_EXTRACT_PATH_TEXT(meta, 'verified') = 'false' THEN False
	    ELSE NULL
	END AS pp_verified,
	CASE
	    WHEN JSON_EXTRACT_PATH_TEXT(meta, 'address_confirmed') = 'true' THEN True
	    WHEN JSON_EXTRACT_PATH_TEXT(meta, 'address_confirmed') = 'false' THEN False
	    ELSE NULL
	END AS pp_address_confirmed
FROM stg_paypal
WHERE rn=1;

--seon data preparation
DROP TABLE IF EXISTS stg_last_seon_data;
CREATE TEMP TABLE stg_last_seon_data
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	seon_id,
	carrier,
	ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY updated_at::timestamp DESC) AS row_num
FROM stg_curated.seon_phone_data;

--getting only most up to date row from experian data per customer
DROP TABLE IF EXISTS seon;
CREATE TEMP TABLE seon
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	'https://admin.seon.io/transactions/'||seon_id||'/' AS seon_link,
	carrier AS phone_carrier
FROM
	stg_last_seon_data
WHERE
	row_num=1;

--checkout documents data preparation
DROP TABLE IF EXISTS stg_last_personal_identifications;
CREATE TEMP TABLE stg_last_personal_identifications
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	user_id AS customer_id,
	identification_number,
	ROW_NUMBER () OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS row_num
FROM stg_api_production.personal_identifications;

--getting only most up to date row from personal_identifications per customer
DROP TABLE IF EXISTS personal_identifications;
CREATE TEMP TABLE personal_identifications
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
	customer_id,
	identification_number
FROM
	stg_last_personal_identifications
WHERE
	row_num=1;

--Payment errors
DROP TABLE IF EXISTS extracted_errors;
CREATE TEMP TABLE extracted_errors
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    upm.user_id AS customer_id,
    it.created_at,
    JSON_EXTRACT_ARRAY_ELEMENT_TEXT(it.errors, 0) AS error
FROM stg_external_apis.ixopay_transactions AS it
INNER JOIN stg_api_production.user_payment_methods AS upm
ON it.merchant_txid = upm.merchant_transaction_id
WHERE it.errors IS NOT NULL
and it.uuid!='2b325c6f0332e8c579f3'  -- this id have invalid json
;

--Payment errors by customer
DROP TABLE IF EXISTS errors_list;
CREATE TEMP TABLE errors_list
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    customer_id,
    '['||listagg(error, ', ') WITHIN GROUP (ORDER BY created_at DESC)||']' AS payment_errors
FROM extracted_errors
GROUP BY 1;

--Grover Card customers
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
SELECT
    DISTINCT customer_id
WHERE event_journey LIKE '%activated%';

--Final Table
DROP TABLE IF EXISTS ods_production.customer_scoring; 
CREATE TABLE ods_production.customer_scoring
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
 SELECT
    u.id AS customer_id,
    u.created_at AS customer_created_at,
    u.subscription_limit AS current_subscription_limit,
    case when isnumeric(dt.subscription_limit::text) then dt.subscription_limit::int else null end AS initial_subscription_limit,
    dt.creation_timestamp::timestamp AS subscription_limit_defined_date,
    u.subscription_limit_change_date AS subscription_limit_change_date,
    GREATEST(u.updated_at, b.updated_at, v.updated_at, f.updated_at, s.updated_at) AS updated_at,
    dt.main_reason AS customer_scoring_result,
    s.schufa_class,
    s.schufa_score,
    s.schufa_identity_confirmed,
    s.schufa_paid_debt,
    s.schufa_total_debt,
    b.burgel_score,
    b.burgel_score_details,
    b.burgel_person_known,
    b.burgel_address_details,
    v.verita_score,
    v.verita_person_known_at_address,
    crif.burgelcrif_person_known,
    crif.burgelcrif_score_value,
    crif.burgelcrif_score_decision,
    expe.experian_score,
    expe.experian_person_known,
    expe.experian_address_known,
    expe.experian_person_known_at_address,
    del.delphi_score,
    del.delphi_score_value,
    eq.equifax_score,
    eq.equifax_rating,
    eq.agg_unpaid_products,
    fc.focum_score,
    fc.focum_rating,
    u.trust_type,
    CASE
        WHEN u.verification_state = 'not_started'
         AND idr.customer_id IS NULL
            THEN NULL
        ELSE u.verification_state
    END AS id_check_state,
    CASE
        WHEN gcu.customer_id IS NOT NULL
            THEN 'IDnow'
        ELSE COALESCE(iva.verification_type, ivab.verification_type)
    END AS id_check_type,
    'https://dashboard.onfido.com/checks/'||COALESCE(iva.remote_id, ivab.remote_id)||'/reports' AS id_check_url,
    COALESCE(iva."result", ivab."result") AS id_check_result,
    COALESCE(iva.raw_result, ivab."result") LIKE '%'||per.identification_number||'%' AS documents_match,
    per.identification_number AS document_number,
    f.fraud_type,
    f.min_fraud_detected,
    f.max_fraud_detected,
    CASE WHEN u.user_type ='normal_customer' THEN
---Germany
		----SCHUFA (person known flag should be present as schufa_identity_confirmed in customer scoring script)
	CASE
	   	when s.schufa_class in ('A', 'B', 'C', 'D') and s.schufa_identity_confirmed = TRUE then '1_Low1'::TEXT
	   	when s.schufa_class in ('E','F','G') and s.schufa_identity_confirmed = TRUE then '2_Low2'::TEXT
	   	when (s.schufa_class in ('H', 'I', 'K', 'L') and s.schufa_identity_confirmed = TRUE) or s.schufa_identity_confirmed = FALSE then '3_Medium'::TEXT
	   	when s.schufa_class in ('M','N','O','P') then '4_High'::TEXT
	   	----CRIF
	   when crif.burgelcrif_score_value::float <= 1.9 then '1_Low1'::TEXT 
		when crif.burgelcrif_score_value::float <= 2.5 then '2_Low2'::TEXT 
		when crif.burgelcrif_score_value::float = 2.6 then '3_Medium'::TEXT 
		when crif.burgelcrif_score_value::float > 2.6 then '4_High'::TEXT
		----Burgel
		when b.burgel_score::float <= 1.9 then '1_Low1'::TEXT 
		when b.burgel_score::float <= 2.5 then '2_Low2'::TEXT 
		when b.burgel_score::float = 2.6 then '3_Medium'::TEXT 
		when b.burgel_score::float > 2.6 then '4_High'::TEXT
----Austria
		   when crif_at.burgelcrif_score_value >= 500 then '1_Low1'::TEXT
		   when crif_at.burgelcrif_score_value >= 450 then '2_Low2'::TEXT
		   when crif_at.burgelcrif_score_value >= 370 then '3_Medium'::TEXT
		   when crif_at.burgelcrif_score_value < 370 then '4_High'::TEXT
----Spain
		WHEN eq.equifax_rating IN ('ES01', 'ES02')
            THEN '1_Low1'::TEXT  
        WHEN eq.equifax_rating = 'ES03'
            THEN '2_Low2'::TEXT
        WHEN eq.equifax_rating IN ('ES04', 'ES05')
            THEN '3_Medium'::TEXT    
        WHEN eq.equifax_rating IN ('ES06', 'ES07', 'ES08')
            THEN '4_High'::TEXT
        WHEN del.delphi_score  in ('A','B')
            THEN '1_Low1'::TEXT
        WHEN del.delphi_score  in ('C','D')
            THEN '2_Low2'::TEXT
        WHEN del.delphi_score  in ('E','F')
            THEN '3_Medium'::TEXT   
       	WHEN del.delphi_score  in ('G','H','I')
            THEN '4_High'::TEXT
---NL
		WHEN fc.focum_rating IN ('NL01', 'NL02')
            THEN '1_Low1'::TEXT
        WHEN fc.focum_rating IN ('NL03', 'NL04')
            THEN '2_Low2'::TEXT
        WHEN fc.focum_rating IN ('NL05', 'NL06')
            THEN '3_Medium'::TEXT   
       	WHEN fc.focum_rating IN ('NL07','NL08', 'NL09', 'NL10')
            THEN '4_High'::TEXT
        WHEN expe.experian_score  >= 962
            THEN '1_Low1'::TEXT
        WHEN expe.experian_score  >= 929
            THEN '2_Low2'::TEXT
        WHEN expe.experian_score  >= 894
            THEN '3_Medium'::TEXT   
       	WHEN expe.experian_score  < 894
            THEN '4_High'::TEXT
		ELSE '4_High'::TEXT
 	end 
 	ELSE NULL 
 	END as burgel_risk_category,
    CASE
        -- Germany
        WHEN s.schufa_class IN ('A','B','C')
            THEN 'DE_1'::TEXT
        WHEN s.schufa_class IN ('D','E','F')
            THEN 'DE_2'::TEXT
        WHEN s.schufa_class IN ('G','H','I')
            THEN 'DE_3'::TEXT
        WHEN s.schufa_class IN ('K','L')
            THEN 'DE_4'::TEXT
        WHEN s.schufa_class IN ('M','N')
            THEN 'DE_5'::TEXT
        WHEN s.schufa_class IN ('O','P')
            THEN 'DE_6'::TEXT
        WHEN crif.burgelcrif_country_code = 'DE'
            THEN NULL
        -- Austria
        WHEN crif.burgelcrif_score_value >= 550
            THEN 'AT_1'::TEXT
        WHEN crif.burgelcrif_score_value >= 510
            THEN 'AT_2'::TEXT
        WHEN crif.burgelcrif_score_value >= 450
            THEN 'AT_3'::TEXT
        WHEN crif.burgelcrif_score_value >= 370
            THEN 'AT_4'::TEXT
        WHEN crif.burgelcrif_score_value < 370
            THEN 'AT_5'::TEXT
    -- Netherlands
        WHEN focum_rating IS NOT NULL
        	THEN focum_rating::TEXT
	-- Spain
        WHEN equifax_rating IS NOT NULL
        	THEN equifax_rating::TEXT
        ELSE NULL
    END AS country_grade,
    r.orders_count,
    r.subscriptions_count,
    r.first_paid_payments,
    r.first_failed_payments,
    r.recurring_paid_payments,
    r.recurring_failed_payments,
    r.recurring_paid_months,
    r.paid_amount,
    r.customer_label_old AS customer_label,
    r.customer_label_new,
    n.nethone_risk_category,
    seon.seon_link,
    seon.phone_carrier,
    t.tag_name,
    t.is_blacklisted,
    t.is_whitelisted,
    CASE
        WHEN b.burgel_score_details LIKE '%Negative remarks%' THEN True
        WHEN s.schufa_is_negative_remarks IS True Then True
        ELSE False
    END AS is_negative_remarks,
    pr.reviews AS previous_manual_reviews,
    pm.pp_verified AS paypal_verified,
    pm.pp_address_confirmed AS paypal_address_confirmed,
    cc.web_users,
    cc.accounts_cookie,
    ic.distinct_customers_with_ip,
    el.payment_errors
FROM stg_api_production.spree_users AS u
LEFT JOIN decision_tree AS dt ON
	u.id = dt.customer_id
LEFT JOIN burgel AS b ON u.id = b.customer_id
LEFT JOIN boniversum AS v ON u.id  = v.customer_id
LEFT JOIN schufa AS s ON u.id = s.customer_id
LEFT JOIN crifburgel AS crif ON u.id = crif.customer_id AND crif.burgelcrif_country_code = 'DE'
LEFT JOIN crifburgel AS crif_at ON u.id = crif_at.customer_id AND crif_at.burgelcrif_country_code = 'AT'
LEFT JOIN experian AS expe ON u.id = expe.customer_id
LEFT JOIN delphi AS del ON u.id = del.customer_id
LEFT JOIN es_equifax AS eq ON u.id = eq.customer_id
LEFT JOIN nl_focum AS fc ON u.id = fc.customer_id
LEFT JOIN customer_risk AS r ON u.id = r.customer_id
LEFT JOIN nethone AS n ON u.id = n.customer_id
LEFT JOIN risk_tags AS t ON u.id = t.customer_id
LEFT JOIN id_verification_risk AS idr ON u.id = idr.customer_id
LEFT JOIN id_verification_applicants AS iva ON u.id = iva.customer_id
LEFT JOIN id_verification_applicants_b2b AS ivab ON u.company_id = iva.customer_id
LEFT JOIN fraud AS f ON u.id = f.customer_id
LEFT JOIN cookie_count AS cc ON u.id = cc.customer_id
LEFT JOIN ip_count AS ic ON u.id = ic.customer_id
LEFT JOIN previous_reviews AS pr ON u.id = pr.customer_id
LEFT JOIN paypal_meta AS pm ON u.id = pm.customer_id
LEFT JOIN seon ON u.id = seon.customer_id
LEFT JOIN personal_identifications AS per ON u.id = per.customer_id
LEFT JOIN errors_list AS el ON u.id = el.customer_id
ORDER BY u.id DESC;

grant select on ods_production.customer_scoring TO group risk_users,risk_users_redash;

GRANT SELECT ON ods_production.customer_scoring TO tableau;
