CREATE OR REPLACE VIEW dm_commercial.v_target_dynamic_data_cancellation_rate_forecast AS 
-- Note: Targets are not official
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-01-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-01-01" = '  -   ' THEN '0' ELSE replace(replace("2023-01-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-02-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-02-01" = '  -   ' THEN '0' ELSE replace(replace("2023-02-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-03-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-03-01" = '  -   ' THEN '0' ELSE replace(replace("2023-03-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-04-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-04-01" = '  -   ' THEN '0' ELSE replace(replace("2023-04-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-05-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-05-01" = '  -   ' THEN '0' ELSE replace(replace("2023-05-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-06-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-06-01" = '  -   ' THEN '0' ELSE replace(replace("2023-06-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-07-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-07-01" = '  -   ' THEN '0' ELSE replace(replace("2023-07-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-08-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-08-01" = '  -   ' THEN '0' ELSE replace(replace("2023-08-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-09-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-09-01" = '  -   ' THEN '0' ELSE replace(replace("2023-09-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-10-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-10-01" = '  -   ' THEN '0' ELSE replace(replace("2023-10-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-11-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-11-01" = '  -   ' THEN '0' ELSE replace(replace("2023-11-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
UNION ALL 
SELECT 
	region,
	country,
	"store final" AS store_final_freelancers,
	CASE WHEN store_final_freelancers ILIKE '%b2b%' THEN 'B2B' ELSE store_final_freelancers END AS store_final,
	category,
	subcategory,
	KEY,
	'2023-12-01'::date AS "date",
	kpi,
	(CASE WHEN "2023-12-01" = '  -   ' THEN '0' ELSE replace(replace("2023-12-01", ',', ''),'.',',') END)::float AS value
FROM staging.monthly_acquired_targets_not_official
WITH NO SCHEMA BINDING	 
;

GRANT SELECT ON dm_commercial.v_target_dynamic_data_cancellation_rate_forecast TO giorgia_vicari;