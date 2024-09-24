DROP TABLE dm_commercial.r_commercial_daily_targets_since_2022;
CREATE TABLE dm_commercial.r_commercial_daily_targets_since_2022 AS 
WITH commercial_targets_daily_2022 AS (
	SELECT *
	FROM dm_commercial.r_commercial_targets_2022_last
	WHERE to_date BETWEEN '2022-01-01' AND '2022-12-31'
)
, commercial_targets_daily_2023_v1 AS (
	SELECT 
			target_date::date AS to_date,
			country AS store_label,
			split_part("store final",'/',1) AS channel_type,
			split_part("store final",'/',2) AS b2b_split,
			category AS categories,
			subcategory AS subcategories,
			kpi AS measures,
			target_value AS amount
	FROM stg_external_apis.gs_commercial_targets_2023
	WHERE target_date::date BETWEEN '2023-01-01' AND '2023-07-31'
)
, commercial_targets_daily_2023_v2 AS (
	SELECT 
		target_date::date AS to_date,
		country AS store_label,
		split_part("store final",'/',1) AS channel_type,
		NULLIF(split_part("store final",'/',2),'') AS b2b_split,
		category AS categories,
		subcategory AS subcategories,
		kpi AS measures,
		target_value AS amount
	FROM stg_external_apis.gs_commercial_targets_2023_v2
	WHERE target_date::date BETWEEN '2023-08-01' AND '2023-12-31'
)
, commercial_targets_daily_2024_v1 AS (
	SELECT 
		target_date::date AS to_date,
		country AS store_label,
		split_part("store final",'/',1) AS channel_type,
		NULLIF(split_part("store final",'/',2),'') AS b2b_split,
		category AS categories,
		subcategory AS subcategories,
		kpi AS measures,
		target_value AS amount
	FROM dev.stg_external_apis.gs_commercial_targets_2024
	WHERE target_date::date BETWEEN '2024-01-01' AND '2024-12-31'
)
SELECT * FROM commercial_targets_daily_2022
UNION ALL 
SELECT * FROM commercial_targets_daily_2023_v1
UNION ALL 
SELECT * FROM commercial_targets_daily_2023_v2
UNION ALL 
SELECT * FROM commercial_targets_daily_2024_v1
;
