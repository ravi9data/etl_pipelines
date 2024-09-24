DROP TABLE IF EXISTS marketing.marketing_daily_asv_targets;
CREATE TABLE marketing.marketing_daily_asv_targets AS
WITH targets AS
(
  SELECT
    measures,
    categories,
    CASE
      WHEN store_label = 'AT'
        THEN 'Austria'
      WHEN store_label = 'ES'
        THEN 'Spain'
      WHEN store_label = 'NL'
        THEN 'Netherlands'
      WHEN store_label = 'US'
        THEN 'United States'
      WHEN store_label = 'DE'
        THEN 'Germany'
    END AS country,
    CASE
      WHEN store_label IN ('AT','ES','NL')
        THEN 'Rest of Europe'
      WHEN store_label = 'US'
        THEN 'United States Region'
      WHEN store_label = 'DE'
        THEN 'Germany Region'
    END AS region,
    CASE
      WHEN channel_type = 'B2B' THEN 'B2B' || ' ' || country
      WHEN channel_type = 'Retail'
        THEN 'Partnerships' || ' ' || country
      WHEN channel_type = 'B2C'
        THEN 'Grover' || ' ' || country
    END AS store,
    to_date AS datum,
    SUM(amount) AS measure_value
  FROM dm_commercial.r_commercial_daily_targets_since_2022
  WHERE to_date >= '2022-06-01'
  GROUP BY 1,2,3,4,5,6
),

main_data AS (
SELECT
  country,
  region,
  store,
  categories,
  datum,
  COALESCE(SUM(CASE WHEN measures = 'ASV' THEN measure_value END),0) AS active_subs_value,
  COALESCE(SUM(CASE WHEN measures = 'Acquired ASV' THEN measure_value END),0) AS acquired_subs_value,
  COALESCE(SUM(CASE WHEN measures = 'Cancelled ASV' THEN measure_value END),0) AS cancelled_sub_value,
  COALESCE(SUM(CASE WHEN measures = 'Acquired Subscriptions' THEN measure_value END),0) AS acquired_subs,
  acquired_subs_value - cancelled_sub_value AS incremental_subs_value
FROM targets
GROUP BY 1,2,3,4,5),

country_data AS (
SELECT
  country,
  region,
  country || ' ' || 'Total' AS store,
  categories,
  datum,
  SUM(active_subs_value) AS active_subs_value,
  SUM(acquired_subs_value) AS acquired_subs_value,
  SUM(cancelled_sub_value) AS cancelled_sub_value,
  SUM(acquired_subs) AS acquired_subs,
  SUM(incremental_subs_value) AS incremental_subs_value
FROM main_data
GROUP BY 1,2,3,4,5),

region_data AS (
SELECT
  region AS country,
  region,
  region || ' ' || 'Total' AS store,
  categories,
  datum,
  SUM(active_subs_value) AS active_subs_value,
  SUM(acquired_subs_value) AS acquired_subs_value,
  SUM(cancelled_sub_value) AS cancelled_sub_value,
  SUM(acquired_subs) AS acquired_subs,
  SUM(incremental_subs_value) AS incremental_subs_value
FROM main_data
GROUP BY 1,2,3,4,5),

total_data AS (
SELECT
  'Total'::text AS country,
  'Total'::text AS region,
  'Total'::text store,
  categories,
  datum,
  SUM(active_subs_value) AS active_subs_value,
  SUM(acquired_subs_value) AS acquired_subs_value,
  SUM(cancelled_sub_value) AS cancelled_sub_value,
  SUM(acquired_subs) AS acquired_subs,
  SUM(incremental_subs_value) AS incremental_subs_value
FROM main_data
GROUP BY 1,2,3,4,5)


SELECT * FROM main_data
UNION ALL
SELECT * FROM country_data
UNION ALL
SELECT * FROM region_data
UNION ALL
SELECT * FROM total_data;

GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;