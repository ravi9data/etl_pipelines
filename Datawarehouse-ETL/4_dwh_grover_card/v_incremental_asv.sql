SELECT 
fact_Date,
is_activated,
--count(DISTINCT CASE WHEN active_subscription_value_before_card<>0 then customer_id_bc END ) AS customer_id_bc,
--count(DISTINCT CASE WHEN active_subscription_value<>0 THEN customer_id_ac END) AS customer_id_ac,
COALESCE(sum(active_subscriptions_before_card),0) AS active_subscriptions_before_card,
COALESCE(sum(active_subscription_value_before_card),0) AS active_subscription_value_before_card,
COALESCE(sum(active_subscriptions),0) AS active_subscriptions,
COALESCE(sum(active_subscription_value),0) AS active_subscription_value,
sum(incremental_active_subscriptions) AS incremental_active_subscriptions,
sum(incremental_active_subscription_value) AS incremental_active_subscription_value
GROUP BY 1,2
ORDER BY 1 desc
WITH NO SCHEMA BINDING;


