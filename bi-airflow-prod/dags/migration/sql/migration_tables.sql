/* DROP TABLE stg_curated.migrated_contracts;
CREATE TABLE stg_curated.migrated_contracts AS
--new source
SELECT
	DISTINCT
	'kafka' AS source_type,
	contract_id AS subscription_bo_id,
	migrated_at AS migration_date
FROM s3_spectrum_kafka_topics_raw.subscriptions_bigmigration_v1 m
WHERE m.status ='success' AND m.user_type ='business' --for now only B2B
UNION ALL
--old source
SELECT
	DISTINCT
	'googlesheets' AS source_type,
	subscription_bo_id,
	migration_date
FROM staging_airbyte_bi.final_migrated_contracts_dates_3 mig; */

--JOB
DROP TABLE IF EXISTS tmp_new_migrated_cases;

CREATE TEMP TABLE tmp_new_migrated_cases AS
SELECT *
FROM s3_spectrum_kafka_topics_raw.subscriptions_bigmigration_v2 m
WHERE TRUE 
  AND status ='success' 
  AND user_type ='business'
  AND consumed_at >= CURRENT_DATE -2
;

DELETE FROM stg_curated.migrated_contracts
USING tmp_new_migrated_cases m
WHERE m.contract_id = stg_curated.migrated_contracts.subscription_bo_id
;

INSERT INTO stg_curated.migrated_contracts
SELECT DISTINCT
 'kafka' AS source_type
 ,contract_id AS subscription_bo_id
 ,migrated_at::timestamp AS migration_date
FROM tmp_new_migrated_cases m
;
