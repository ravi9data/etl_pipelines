DROP TABLE IF EXISTS tpm_store_stocks;
CREATE TEMP TABLE tpm_store_stocks
SORTKEY(sku)
DISTKEY(sku)
AS
WITH distinct_sku AS (
	SELECT DISTINCT sku
	FROM staging.store_1
	UNION 
	SELECT DISTINCT sku
	FROM staging.store_4
	UNION
	SELECT DISTINCT sku
	FROM staging.store_5
	UNION
	SELECT DISTINCT sku
	FROM staging.store_618
)
SELECT 
	d.sku,
	GREATEST(s1.instockcount, s4.instockcount, s5.instockcount, s618.instockcount) AS instockcount_total,
	GREATEST(s1.availablecount, s4.availablecount, s5.availablecount, s618.availablecount) AS availablecount_total,
	GREATEST(s1.reservedcount, s4.reservedcount, s5.reservedcount, s618.reservedcount) AS reservedcount_total,
	s1.mode AS mode_s1,
	s1.AvailableCount AS AvailableCount_s1,
	s1.instockcount AS instockcount_s1,
	s1.reservedcount AS reservedcount_s1,
	s4.mode AS mode_s4,
	s4.AvailableCount AS AvailableCount_s4,
	s4.instockcount AS instockcount_s4,
	s4.reservedcount AS reservedcount_s4,
	s5.mode AS mode_s5,
	s5.AvailableCount AS AvailableCount_s5,
	s5.instockcount AS instockcount_s5,
	s5.reservedcount AS reservedcount_s5,
	s618.mode AS mode_s618,
	s618.AvailableCount AS AvailableCount_s618,
	s618.instockcount AS instockcount_s618,
	s618.reservedcount AS reservedcount_s618,
	current_date AS date
FROM distinct_sku d
LEFT JOIN staging.store_1 s1 
	ON d.sku = s1.sku 
LEFT JOIN staging.store_4 s4 
	ON d.sku = s4.sku 
LEFT JOIN staging.store_5 s5 
	ON d.sku = s5.sku 
LEFT JOIN staging.store_618 s618
	ON d.sku = s618.sku
;



BEGIN TRANSACTION;

DELETE FROM dm_product.store_stocks_historical
WHERE store_stocks_historical.date = current_date::date
	OR store_stocks_historical.date <= dateadd('year', -2, current_date);

INSERT INTO dm_product.store_stocks_historical
SELECT * 
FROM tpm_store_stocks;
 
END TRANSACTION;


GRANT SELECT ON dm_product.store_stocks_historical TO tableau;

GRANT SELECT ON dm_product.store_stocks_historical TO hightouch;

GRANT SELECT ON dm_product.store_stocks_historical TO GROUP pricing;

GRANT SELECT ON dm_product.store_stocks_historical TO hightouch_pricing;

GRANT SELECT ON dm_product.store_stocks_historical TO catman;

GRANT SELECT ON dm_product.store_stocks_historical TO redash_pricing;
	