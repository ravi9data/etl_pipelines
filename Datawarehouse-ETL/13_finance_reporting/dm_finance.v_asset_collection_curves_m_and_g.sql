DROP VIEW IF EXISTS dm_finance.v_asset_collection_curves_m_and_g_raw;
CREATE VIEW dm_finance.v_asset_collection_curves_m_and_g_raw AS
WITH m_and_g_assets AS (
SELECT DISTINCT asset_id 
FROM dm_finance.asset_collection_curves_historical
WHERE capital_source_name = 'SUSTAINABLE TECH RENTAL EUROPE II GMBH'
)
SELECT cur.*
FROM dm_finance.asset_collection_curves_historical cur 
  INNER JOIN m_and_g_assets m
    ON cur.asset_id = m.asset_id
WHERE TRUE
  AND cur.shifted_creation_cohort >= '2019-07-01'
  AND cur.months_since_shifted_creation_cohort >= 0 
WITH NO SCHEMA BINDING
;