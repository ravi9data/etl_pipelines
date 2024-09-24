
CREATE OR REPLACE VIEW dm_bd.v_partner_conversions_impressions AS
SELECT
    date_trunc('week',reporting_date) AS reporting_date,
    store_id,
    store_name,
    store_label,
    product_sku,
    variant_sku,
    ean,
    product_name,
    brand,
    variant_color,
    category_name,
    subcategory_name,
    SUM(total_impressions) AS total_impressions,
    SUM(available_impressions) AS available_impressions,
    SUM(unavailable_impressions) AS unavailable_impressions,
    SUM(widget_impressions) AS widget_impressions,
    SUM(pageviews) AS pageviews,
    SUM(users) AS users,
    SUM(add_to_cart) AS add_to_cart,
    SUM(submitted_orders) AS submitted_orders,
    SUM(paid_orders) AS paid_orders,
    SUM(acquired_subscription) AS acquired_subscription,
    SUM(acquired_subscription_value) AS acquired_subscription_value
FROM dm_bd.partner_reporting
    WHERE reporting_date >= CURRENT_DATE - INTERVAL '6 month'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_bd.v_partner_conversions_impressions TO tableau;
