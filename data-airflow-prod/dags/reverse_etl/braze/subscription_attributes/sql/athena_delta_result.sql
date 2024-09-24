WITH deduped_history AS (
SELECT
        external_id,
        almost_ending_subscription_names,
        almost_ending_subscription_ids,
        almost_ending_subscription_periods,
        almost_ending_subscription_variant_skus,
        same_day_ending_subs_count,
        closest_minimum_cancellation_date,
        active_subscriptions,
        ROW_NUMBER () OVER (PARTITION BY external_id ORDER BY extracted_at DESC) AS row_num
FROM braze_subscription_export_history)
SELECT
        external_id,
        almost_ending_subscription_names,
        almost_ending_subscription_ids,
        almost_ending_subscription_periods,
        almost_ending_subscription_variant_skus,
        same_day_ending_subs_count,
        closest_minimum_cancellation_date,
        active_subscriptions
FROM braze_staging_attributes
EXCEPT
SELECT
        external_id,
        almost_ending_subscription_names,
        almost_ending_subscription_ids,
        almost_ending_subscription_periods,
        almost_ending_subscription_variant_skus,
        same_day_ending_subs_count,
        closest_minimum_cancellation_date,
        active_subscriptions
FROM deduped_history
WHERE row_num = 1;
