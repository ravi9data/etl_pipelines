BEGIN;

WHERE date = current_date - 1;

SELECT 
    subscription_id,
    order_id,
    customer_id,
    subscription_start_date,
    order_submitted_date,
    rank_subscriptions,
    subscriptions_per_customer,
    subscription_status,
    subscription_value,
    country,
    customer_type,
    variant_sku,
    first_asset_delivery_date,
    subscription_plan,
    rental_period,
    committed_sub_value,
    new_recurring,
    minimum_cancellation_date,
    subscription_bo_id,
    current_date - 1 as date 
FROM 
    
COMMIT;