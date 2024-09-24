SELECT
    datum,
    subcategory AS subcategory_name,
    acquired_subscriptions AS n_subscriptions,
    acquired_subvalue_daily AS asv
FROM
    dm_commercial.v_target_data_cancellation_rate_forecast
WHERE
    country = '{country}'
    AND customer_type = '{customer_type}'
