--marktplaats_cost cost

DELETE FROM marketing.marketing_marktplaats_cost 
USING staging.marketing_cost_marktplaats cur
WHERE marketing_marktplaats_cost.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_marktplaats_cost
SELECT
    REPLACE(date, '/', '-')::DATE AS date,
    REPLACE(cost,',','')::DOUBLE PRECISION AS cost
FROM staging.marketing_cost_marktplaats;
