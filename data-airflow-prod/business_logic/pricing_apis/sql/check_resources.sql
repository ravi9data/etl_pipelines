WITH 
last_mozenda_synced_date AS (
    SELECT MAX(SUBSTRING(crawled_at::date, 1, 10)::date) AS max_mozenda_synced_date
    FROM staging_price_collection.amazon amzn
    WHERE "price" IS NOT NULL
),
last_rainforest_synced_date AS (
    SELECT MAX(SUBSTRING(processed_at::date, 1, 10)::date) AS max_rainforest_synced_date
    FROM staging_price_collection.rainforest_pricing_data sae
)
SELECT last_mozenda_synced_date.max_mozenda_synced_date AS last_mozenda_synced_date, 
       last_rainforest_synced_date.max_rainforest_synced_date AS last_rainforest_synced_date
FROM last_mozenda_synced_date
CROSS JOIN last_rainforest_synced_date;