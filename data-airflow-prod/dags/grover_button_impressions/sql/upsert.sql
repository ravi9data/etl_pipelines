BEGIN TRANSACTION;

DELETE stg_events.grover_button_impressions
WHERE reporting_date IN (
        SELECT DISTINCT reporting_date
        FROM stg_events_dl.grover_button_impressions_agg);

INSERT INTO stg_events.grover_button_impressions(
    reporting_date,
    store_id,
    product_sku,
    button_state,
    impressions,
    unique_impressions
    )
SELECT
    reporting_date,
    store_id,
    product_sku,
    button_state,
    impressions,
    unique_impressions
FROM stg_events_dl.grover_button_impressions_agg;

END TRANSACTION;
