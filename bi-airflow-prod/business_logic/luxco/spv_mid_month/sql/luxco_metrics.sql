SELECT
	capital_source_name ,
	count(DISTINCT asset_id) AS asset_count,
	count(DISTINCT product_sku) AS product_sku_count,
	round(sum(original_valuation), 2) AS original_valuation,
	round(sum(final_valuation), 2) AS final_valuation,
	round(sum(CASE WHEN m_since_last_valuation_price = 0 THEN original_valuation ELSE 0 END)* 100 / NULLIF(sum(original_valuation), 0), 2) AS m_value
FROM
	ods_spv_historical.luxco_reporting_{tbl_suffix} lrmm
WHERE
	capital_source_name IN ('Grover Finance I GmbH', 'SUSTAINABLE TECH RENTAL EUROPE II GMBH', 'USA_test')
GROUP BY
	1
ORDER BY
	1;
