--tv cost

DELETE FROM marketing.marketing_cost_tv 
USING staging.marketing_tv_cost cur
WHERE marketing_cost_tv.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_tv
SELECT
	date::TIMESTAMP,
	country,
	replace(gross_budget_spend,',','')::BIGINT,
	replace(gross_actual_spend,',','')::BIGINT,
	replace(cash_percentage,',','')::DOUBLE PRECISION,
	replace(non_cash_percentage,',','')::DOUBLE PRECISION,
	currency,
	brand_non_brand
FROM staging.marketing_tv_cost
WHERE "date" IS NOT NULL;
