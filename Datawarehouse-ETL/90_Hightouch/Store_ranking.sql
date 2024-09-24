DROP TABLE IF EXISTS hightouch_sources.store_specific_rank;
CREATE TABLE hightouch_sources.store_specific_rank AS
WITH rankings AS (
		SELECT
			rp.store_name,
			p.product_sku,
			sr.product_id,
			sr.store_id,
			sr."rank",
			ROW_NUMBER() OVER(
				PARTITION BY sr.product_id, sr.store_id 
				ORDER BY 
					sr.created_at DESC,
					sr.updated_at DESC) AS row_num
		FROM
			s3_spectrum_rds_dwh_api_production.product_store_ranks sr
		LEFT JOIN ods_production.store rp ON sr.store_id = rp.id
		LEFT JOIN ods_production.product p ON sr.product_id = p.product_id)
SELECT DISTINCT 
	r.product_id,
	r.product_sku,
	a."rank" AS Austria,
	g."rank" AS Germany,
	n."rank" AS Netherlands,
	s."rank" AS Spain,
	us."rank" AS US,
	ga."rank" AS Austria_B2B,
	gg."rank" AS Germany_B2B,
	gn."rank" AS Netherlands_B2B,
	gs."rank" AS Spain_B2B
FROM rankings r
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Austria' AND row_num = 1) a USING(product_id)
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Germany' AND row_num = 1) g USING(product_id)
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Netherlands' AND row_num = 1) n USING(product_id)
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Spain' AND row_num = 1) s USING(product_id)
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'United States' AND row_num = 1) us USING(product_id)
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Grover B2B Austria' AND row_num = 1) ga USING(product_id) 
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Grover B2B Germany' AND row_num = 1) gg USING(product_id) 
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Grover B2B Netherlands' AND row_num = 1) gn USING(product_id) 
LEFT JOIN (SELECT product_id, "rank" FROM rankings WHERE store_name = 'Grover B2B Spain' AND row_num = 1) gs USING(product_id) 
WHERE row_num = 1;

GRANT SELECT ON hightouch_sources.store_specific_rank TO hightouch;