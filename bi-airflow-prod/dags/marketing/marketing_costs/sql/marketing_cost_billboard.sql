--billboard cost

DELETE FROM marketing.marketing_cost_billboard 
USING staging.marketing_billboard_cost cur
WHERE marketing_cost_billboard.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_billboard
SELECT
	date::TIMESTAMP,
	country,
	replace(gross_budget_spend,',','')::BIGINT,
	replace(gross_actual_spend,',','')::DOUBLE PRECISION,
	replace(cash_percentage,',','')::DOUBLE PRECISION,
	replace(non_cash_percentage,',','')::DOUBLE PRECISION,
	currency,
	brand_non_brand
FROM staging.marketing_billboard_cost;
