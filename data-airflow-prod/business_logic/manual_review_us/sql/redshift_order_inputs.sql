SELECT
    DISTINCT order_id
FROM ods_data_sensitive.order_manual_review
WHERE store_country = 'United States'
AND created_date >= CURRENT_DATE - INTERVAL '{lookback_days} DAY';
