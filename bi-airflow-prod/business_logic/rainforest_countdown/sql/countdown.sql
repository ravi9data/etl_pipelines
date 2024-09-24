SELECT DATE(_airbyte_emitted_at) AS date_added,
count(id) AS rows_count FROM
staging_price_collection.countdown_pricing_data
GROUP BY date_added
ORDER BY date_added DESC
LIMIT 8
