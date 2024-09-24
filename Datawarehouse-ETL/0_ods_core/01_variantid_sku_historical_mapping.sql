DROP TABLE IF EXISTS ods_production.variantid_sku_historical_mapping;

CREATE TABLE ods_production.variantid_sku_historical_mapping AS
SELECT DISTINCT sku AS variant_sku,
                id  AS variant_id
FROM stg_api_production.spree_variants v;

GRANT SELECT ON ods_production.variantid_sku_historical_mapping TO tableau;