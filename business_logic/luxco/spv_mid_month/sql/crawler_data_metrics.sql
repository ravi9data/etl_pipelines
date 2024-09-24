SELECT
	*
FROM
	(
	SELECT
		'Amazon' AS src,
		crawler ,
		date_trunc('day', reporting_month ::date)::date AS date,
		count(*) AS count,
		count(DISTINCT product_sku) AS product_sku_count
	FROM
		staging_price_collection.ods_amazon
	WHERE
		reporting_month >= DATE_TRUNC('MONTH', CURRENT_DATE)::DATE  - interval '3 month'
		AND crawler IN ('RAINFOREST', 'MOZENDA')
	GROUP BY
		1,
		2,
		3  
UNION
	SELECT
			'Ebay' AS src,
		crawler ,
		date_trunc('day', reporting_month ::date)::date AS date,
		count(*) AS count,
		count(DISTINCT product_sku) AS product_sku_count
	FROM
		staging_price_collection.ods_ebay
	WHERE
		reporting_month >= DATE_TRUNC('MONTH', CURRENT_DATE)::DATE  - interval '3 month'
		AND crawler IN ('COUNTDOWN', 'MOZENDA')
	GROUP BY
		1,
		2,
		3 
UNION
	SELECT
			'Rebuy' AS src,
		crawler ,
		date_trunc('day', reporting_month ::date)::date AS date,
		count(*) AS count,
		count(DISTINCT product_sku) AS product_sku_count
	FROM
		staging_price_collection.ods_rebuy
	WHERE
		reporting_month >= DATE_TRUNC('MONTH', CURRENT_DATE)::DATE  - interval '3 month'
		AND crawler = 'MOZENDA'
	GROUP BY
		1,
		2,
		3 
UNION
	SELECT
			'MediaMarkt' AS src,
		src AS crawler ,
		date_trunc('day', reporting_month ::date)::date AS date,
		count(*) AS count,
		count(DISTINCT product_sku) AS product_sku_count
	FROM
		staging_price_collection.ods_mediamarkt
	WHERE
		reporting_month >= DATE_TRUNC('MONTH', CURRENT_DATE)::DATE  - interval '3 month'
	GROUP BY
		1,
		2,
		3
)
ORDER BY
	1,
	2, 
	3 DESC
;

