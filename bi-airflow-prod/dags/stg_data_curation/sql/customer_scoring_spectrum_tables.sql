------------------------ONE-----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.burgel_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.burgel_data
SELECT distinct
created_at, 
updated_at, 
id, 
customer_id, 
address_id, 
burgel_score, 
score_details, 
person_known,
address_known, 
product_number, 
person_summary, 
raw_data, 
address_key, 
order_id, 
raw_data_old, 
extracted_at, 
"year", "month", "day", "hour"
FROM s3_spectrum_rds_dwh_order_approval.burgel_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;


------------------------two-----------------------------------
--------------------------------------------------------------

DELETE FROM stg_curated.boniversum_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.boniversum_data
SELECT DISTINCT
created_at,
updated_at,
id,
customer_id,
address_id,
verita_score,
person_ident,
post_address_validation,
address_geomodul_existenz,
public_address,
person_summary,
xml_string,
address_key,
order_id,
extracted_at,
year,
month,
day,
hour
FROM s3_spectrum_rds_dwh_order_approval.boniversum_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2 ;


------------------------three-----------------------------------
--------------------------------------------------------------

DELETE FROM stg_curated.crifburgel_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.crifburgel_data
SELECT distinct
created_at, 
updated_at, 
id, 
customer_id, 
address_id,
country_code, 
score_value, 
score_text, 
score_decision, 
person_known, 
address_known, 
raw_data, 
raw_json, 
address_key, 
order_id, 
extracted_at, 
"year", 
"month", 
"day", 
"hour"
FROM s3_spectrum_rds_dwh_order_approval.crifburgel_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2 ;



------------------------four-----------------------------------
--------------------------------------------------------------


DELETE FROM stg_curated.experian_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.experian_data
SELECT distinct
created_at, 
updated_at, 
id, 
customer_id, 
address_id, 
score_value, 
person_known, 
address_known, 
person_in_address, 
age_over_18, 
high_risk_area, 
raw_data, 
raw_json,
address_key,
order_id,
extracted_at,
"year",
"month",
"day",
"hour"
FROM s3_spectrum_rds_dwh_order_approval.experian_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2 ;

------------------------five-----------------------------------
--------------------------------------------------------------


DELETE FROM stg_curated.experian_es_delphi_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.experian_es_delphi_data
SELECT distinct
created_at, 
updated_at, 
id, 
order_id, 
customer_id, 
address_id, 
address_key,
raw_json,
percentile, 
score_code_description,
score_code_source,
score_divider,
score_note,
score_probability,
score_value, 
extracted_at,
"year",
"month",
"day",
"hour"
FROM s3_spectrum_rds_dwh_order_approval.experian_es_delphi_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2 ;


------------------------six-----------------------------------
--------------------------------------------------------------


DELETE FROM stg_curated.equifax_risk_score_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.equifax_risk_score_data
SELECT distinct
created_at, 
updated_at, 
id, 
order_id, 
customer_id,
transaction_id, 
transaction_state, 
interaction_id,
raw_data, 
score_value,
agg_unpaid_products,
extracted_at, 
address_key,
"year",
"month",
"day", 
"hour"
FROM s3_spectrum_rds_dwh_order_approval.equifax_risk_score_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2 ;



------------------------seven-----------------------------------
--------------------------------------------------------------



DELETE FROM stg_curated.risk_us_customer_credit_limit_v1
WHERE consumed_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.risk_us_customer_credit_limit_v1
SELECT distinct
consumed_at, 
created_at, 
updated_at, 
subscription_limit,
currency, 
"comment", 
customer_id, 
credit_limit, 
author,
"year",
"month", 
"day",
"hour"
FROM s3_spectrum_kafka_topics_raw_sensitive.risk_us_customer_credit_limit_v1 s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND consumed_at >= CURRENT_DATE -2 ;


------------------------eight-----------------------------------
--------------------------------------------------------------



DELETE FROM stg_curated.risk_eu_customer_credit_limit_v1
WHERE consumed_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.risk_eu_customer_credit_limit_v1
SELECT distinct
customer_id, 
consumed_at, 
updated_at, 
subscription_limit, 
currency, 
credit_limit, 
created_at, 
author, 
"comment", 
"year", 
"month", 
"day", 
"hour"
FROM s3_spectrum_kafka_topics_raw_sensitive.risk_eu_customer_credit_limit_v1 s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND consumed_at >= CURRENT_DATE -2 ;


------------------------nine-----------------------------------
--------------------------------------------------------------


DELETE FROM stg_curated.risk_customer_tags_apply_v1
WHERE consumed_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.risk_customer_tags_apply_v1
SELECT distinct
comments, 
tag_type, 
published_at, 
consumed_at, 
created_at, 
tag_id, 
customer_id, 
updated_at, 
action_type, 
tag_name, 
"year", 
"month", 
"day", 
"hour"
FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1 s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND consumed_at >= CURRENT_DATE -2 ;

------------------------ten-----------------------------------
--------------------------------------------------------------


DELETE FROM stg_curated.seon_phone_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.seon_phone_data
SELECT DISTINCT
created_at, 
updated_at, 
id, 
customer_id, 
seon_id, 
seon_transaction_id, 
seon_transaction_state, 
is_valid, 
phone_score, 
phone_number, 
phone_type, 
carrier, 
registered_country, 
fraud_score, 
order_id, 
phone_accounts, 
phone_names_dict, 
extracted_at, 
"year", 
"month", 
"day", 
"hour"
FROM s3_spectrum_rds_dwh_order_approval.seon_phone_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2 ;
