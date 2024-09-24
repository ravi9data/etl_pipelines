CREATE OR REPLACE VIEW ods_spv_historical.v_luxco_data_snapshot_checks AS 
WITH luxco AS (
SELECT 
*,
CASE WHEN capital_source_name <>'USA_test' then 'Germany' ELSE 'USA' END region
FROM ods_spv_historical.luxco_reporting_snapshot lrs )
,MD AS (
SELECT 
DISTINCT region,
dateadd('month',-1,luxco_month::date)::Date AS act_month,
product_sku 
FROM ods_spv_historical.luxco_manual_deletions lmd )
,MR AS (
	SELECT DISTINCT 
		dateadd('month',-1,luxco_month::date)::Date AS act_month,
		luxco_month,
		product_sku,
		capital_source 
	FROM ods_spv_historical.luxco_manual_revisions lmr
	WHERE product_sku IS NOT NULL )
SELECT 
l.*,
COALESCE(CASE 
	WHEN md.product_sku IS NOT NULL THEN 'MD' 
END,
CASE
	WHEN mr.product_sku IS NOT NULL THEN 'MR' 
END) AS MR_MD
FROM luxco l
LEFT JOIN MD md 
ON md.product_sku=l.product_sku
AND date_trunc('month',reporting_date)::Date=md.act_month
AND CASE WHEN capital_source_name <>'USA_test' then 'Germany' ELSE 'USA' END = md.region 
LEFT JOIN MR mr 
	ON mr.product_sku=l.product_sku
	AND mr.act_month=date_trunc('month',reporting_date)::Date 
	AND mr.capital_source =l.capital_source_name 
	WITH NO SCHEMA BINDING;