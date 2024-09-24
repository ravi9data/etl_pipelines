--ebay_kleinanzeigen cost
--we receive data from external partner ebay kleinenzeigen, 
--data is stored in a google sheet and from there imported in staging table created in dag_markeging_costs.py
BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_ebay_kleinanzeigen
USING staging.marketing_cost_ebay_kleinanzeigen cur
WHERE marketing_cost_ebay_kleinanzeigen."date"::DATE = cur."date"::DATE AND cur."date"::DATE >= CURRENT_DATE - 7;

INSERT INTO marketing.marketing_cost_ebay_kleinanzeigen
SELECT
    "date"::DATE,
    category,
    platform,
    ROUND(REPLACE(clicks,',',''),0)::BIGINT AS clicks,
    ROUND(REPLACE(impressions,',',''),0)::BIGINT AS impressions,
    REPLACE(cost,',','')::DOUBLE PRECISION AS cost
FROM staging.marketing_cost_ebay_kleinanzeigen
WHERE "date" IS NOT NULL AND "date"::DATE >= CURRENT_DATE - 7;

END TRANSACTION;
