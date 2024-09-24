DROP VIEW IF EXISTS dm_finance.v_purchase_option_report;
CREATE VIEW dm_finance.v_purchase_option_report AS 
WITH subs_ AS (
    SELECT 
        s.*,
        ald.asset_status_detailed,
        CASE WHEN s.subscription_id ILIKE 'F%' THEN 'flex'
             WHEN s.subscription_id ILIKE 'M%' THEN 'mix'
             ELSE 'sf' 
        END AS subs_source,
        CASE WHEN months_required_to_own ILIKE '%.%' THEN TRUE 
             ELSE FALSE 
        END AS is_float,
        COALESCE(ROUND(LEFT(CASE WHEN months_required_to_own ILIKE '%.%' THEN months_required_to_own ELSE NULL END , 4)::DECIMAL(2,1))::INT, months_required_to_own::INT) AS final_months_to_own
        FROM master.subscription s 
        LEFT JOIN ods_production.allocation a 
            ON s.subscription_id=a.subscription_id
            AND a.is_last_allocation_per_asset=TRUE
        LEFT JOIN ods_production.asset_last_allocation_details ald
            ON a.allocation_id=ald.last_allocation_id
)
SELECT 
--subs info
    s.start_date::DATE,
    s.first_asset_delivery_date::DATE,
    CASE 
        WHEN s.subscription_id ILIKE 'F%' THEN 'flex'
        ELSE 'sf'
    END AS logic_subs,
    s.is_float, 
    s.customer_id,
    subscription_sf_id,
    s.subscription_id, 
    s.status, 
    s.cancellation_reason_new, 
    s.country_name,
    s.rental_period,
    --product info 
    s.category_name,
    s.product_name, 
    s.subcategory_name,
    s.brand,
    --payment info
    s.paid_subscriptions,
    --logig purchase
    months_required_to_own,
    final_months_to_own,
    final_months_to_own - s.paid_subscriptions::INT  AS months_until_purchase,
    --based on the months to own
    DATEADD('MONTH', final_months_to_own::INT, first_asset_delivery_date::DATE)::DATE AS predicted_purchase_date,
    --actual purchase date (for past cases)
    CASE 
        WHEN status = 'ACTIVE' THEN DATEADD('MONTH', final_months_to_own::INT, first_asset_delivery_date::DATE)::DATE 
        WHEN status = 'CANCELLED' AND cancellation_reason_new ILIKE 'SOLD%' THEN S.cancellation_date::DATE  
        WHEN status = 'CANCELLED' AND cancellation_reason_new NOT ILIKE 'SOLD%' THEN NULL 
    END AS actual_purchase_date,
    s.cancellation_date,
    CASE 
        WHEN cancellation_reason_new = 'SOLD EARLY' THEN DATEDIFF('DAY', predicted_purchase_date, actual_purchase_date) 
    END AS sold_early_diff,
    CASE 
        WHEN cancellation_reason_new = 'SOLD 1-EUR' THEN 'Sold 1 Eur'
        WHEN cancellation_reason_new = 'SOLD EARLY' THEN 'Sold Early'
        WHEN cancellation_reason_new NOT IN ('SOLD EARLY', 'SOLD 1-EUR') THEN 'Not Purchased'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase < 0  THEN 'Beyond Elegible'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase = 0  THEN  'Elegible - Less than 1 month'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 0 AND months_until_purchase <= 3 THEN  'Elegible - 1 to 3 months'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 3 AND months_until_purchase <= 6 THEN  'Elegible - 4 to 6 months'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 6 AND months_until_purchase <= 12 THEN  'Elegible - 7 to 12 months'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 13 THEN  'Elegible - 13+ months'
        ELSE NULL
    END AS purchase_type,
    asset_status_detailed,
    CASE 
        WHEN asset_status_detailed ='SOLD 1-euro' THEN 'Sold 1 Eur'
        WHEN asset_status_detailed ='SOLD to Customer' AND months_required_to_own IS NOT NULL AND s.cancellation_date<predicted_purchase_date THEN 'Sold Early'
        WHEN asset_status_detailed ='SOLD to Customer' AND months_required_to_own IS NOT NULL AND s.cancellation_date>predicted_purchase_date THEN 'SOLD Later'
        WHEN status = 'ACTIVE' AND  months_until_purchase < 0  THEN 'Beyond Elegible'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase = 0  THEN  'Elegible - Less than 1 month'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 0 AND months_until_purchase <= 3 THEN  'Elegible - 1 to 3 months'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 3 AND months_until_purchase <= 6 THEN  'Elegible - 4 to 6 months'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 6 AND months_until_purchase <= 12 THEN  'Elegible - 7 to 12 months'
        WHEN cancellation_reason_new IS NULL AND status = 'ACTIVE' AND  months_until_purchase > 13 THEN  'Elegible - 13+ months'
        ELSE NULL 
    END AS new_purchase_type,
    CASE 
        WHEN asset_status_detailed IN ('SOLD to Customer') AND s.cancellation_date< predicted_purchase_date THEN ' SOLD Early' 
        ELSE NULL 
    END AS sold_early,
    --in case of early purchase
    s.subscription_value,
    net_subscription_revenue_paid,
    (final_months_to_own * s.subscription_value) AS min_eligibility_amount,
    min_eligibility_amount - net_subscription_revenue_paid AS amount_to_pay
FROM subs_ s
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_finance.v_purchase_option_report TO tableau;