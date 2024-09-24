--milanuncios_cost cost

DELETE FROM marketing.marketing_milanuncios_cost 
USING staging.marketing_cost_milanuncios cur
WHERE marketing_milanuncios_cost.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_milanuncios_cost
SELECT
    REPLACE(date, '/', '-')::DATE AS date,
    REPLACE(cost,',','')::DOUBLE PRECISION AS cost
FROM staging.marketing_cost_milanuncios;
