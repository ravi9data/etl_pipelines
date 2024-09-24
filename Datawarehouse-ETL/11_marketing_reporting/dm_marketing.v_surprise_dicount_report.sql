DROP VIEW IF EXISTS dm_marketing.v_surprise_dicount_report;
CREATE VIEW dm_marketing.v_surprise_dicount_report AS
WITH redemption_clean_up AS (
    SELECT a.customer_source_id,
           a.date,
           a.object,
           b.id AS customer_id,
           COALESCE(LEAD(object) OVER (PARTITION BY customer_source_id ORDER BY date),'n/a') AS prev_object,
           CASE WHEN b.id IS NULL THEN FALSE ELSE TRUE END AS is_available_email
    FROM marketing.voucherify_redemption a
             LEFT JOIN stg_api_production.spree_users b ON a.customer_source_id = b.email
    WHERE a.voucher_code = '4RFBfusWuaaj7uaJsV68H71bAE46lEPd'
      AND result = 'SUCCESS'
),

     customer_id_findings AS (
         SELECT a.customer_source_id,
                a.date,
                a.object,
                a.customer_id,
                a.prev_object,
                a.is_available_email,
                b.subscription_id,
                b.subscription_bo_id,
                ROUND(NULLIF(sub_value_after,0) / NULLIF(sub_value_before,0), 2) AS ratio,
                object || '-' ||  prev_object AS object_result,
                ROW_NUMBER() OVER (PARTITION BY a.customer_id, a.date ORDER BY ratio) AS rn
         FROM redemption_clean_up a
                  LEFT JOIN master.subscription b USING (customer_id)
                  INNER JOIN ods_production.subscription_plan_switching c USING (subscription_id)
         WHERE upgrade_action = 'amount-change'
           AND (a.date BETWEEN c.date - INTERVAL '5 minute' AND c.date + INTERVAL '5 minute')
           AND is_available_email = TRUE
           AND object_result IN ('redemption-redemption', 'redemption-n/a')
         ORDER BY a.customer_id, b.start_date
     ),

     date_findings AS (
         SELECT a.customer_source_id,
                a.date,
                a.object,
                b.customer_id,
                a.prev_object,
                a.is_available_email,
                b.subscription_id,
                b.subscription_bo_id,
                ROUND(NULLIF(sub_value_after,0) / nullif(sub_value_before,0), 2) AS ratio,
                object || '-' ||  prev_object AS object_result,
                ROW_NUMBER() OVER (PARTITION BY a.customer_source_id, a.date ORDER BY a.date) AS rn
         FROM redemption_clean_up a
                  INNER JOIN ods_production.subscription_plan_switching c ON a.date BETWEEN c.date - INTERVAL '5 minute' AND c.date + INTERVAL '5 minute'
                  LEFT JOIN master.subscription b ON c.subscription_id = b.subscription_id
         WHERE upgrade_action = 'amount-change'
           AND is_available_email = FALSE
           AND ROUND(NULLIF(c.sub_value_after,0) / NULLIF(c.sub_value_before,0), 2) = 0.9
           AND object_result IN ('redemption-redemption', 'redemption-n/a')
     ),

     discount_applied AS (
         SELECT DISTINCT
             subscription_id,
             subscription_bo_id,
             customer_source_id,
             customer_id,
             date AS discount_applied_date,
             'customer_id' AS source,
             1 AS is_discount_applied
         FROM customer_id_findings
         WHERE rn = 1

         UNION ALL

         SELECT DISTINCT
             subscription_id,
             subscription_bo_id,
             customer_source_id,
             customer_id,
             date AS discount_applied_date,
             'date' AS source,
             1 AS is_discount_applied
         FROM date_findings
         WHERE rn = 1
     ),

     discount_seen_prep AS (
         SELECT
             spv.customer_id,
             spv.entity_id AS subscription_bo_id,
             COALESCE(s.subscription_id,spv.entity_id) as subscription_id,
             spv.vote_slug,
             date(spv.created_at)::date AS discount_seen_date,
             ROW_NUMBER() OVER (PARTITION BY spv.entity_id, spv.poll_slug ORDER BY spv.created_at, spv.consumed_at desc) AS rowno
         FROM staging.spectrum_polls_votes spv
         LEFT JOIN ods_production.subscription s ON entity_id = s.subscription_bo_id
         LEFT JOIN master.order o ON o.order_id = s.order_id
         LEFT JOIN marketing.voucherify_voucher_transactions v on v.voucher_code = o.voucher_code
         WHERE poll_slug ILIKE '%return-flow%'
           AND vote_slug in ('monthly-price-is-too-high','i-d-rather-want-to-own-the-device','dont-want-to-be-tied-to-a-contract')
           AND coalesce(v.voucher_length,'n/a') != 'recurring'
     ),

     discount_seen AS (
         SELECT
             customer_id,
             subscription_bo_id,
             subscription_id,
             discount_seen_date,
             1 AS is_discount_seen
         FROM discount_seen_prep
         WHERE rowno = 1
     ),

     data_combined AS (
         SELECT
             a.customer_id,
             coalesce(a.subscription_bo_id,b.subscription_bo_id) AS sub_id,
             COALESCE(a.subscription_id,b.subscription_id) AS sub_id_master,
             a.discount_seen_date,
             b.discount_applied_date,
             b.is_discount_applied,
             a.is_discount_seen
         FROM discount_seen a
                  FULL JOIN discount_applied b
                            ON a.subscription_id = b.subscription_id
     )

SELECT
    s.*,
    os.rental_period,
    os.subscription_plan,
    os.order_id,
    os.country_name,
    os.subscription_value_euro,
    os.status,
    os.cancellation_date,
    os.subscription_duration AS subscription_duration_days,
    os.product_name,
    os.category_name,
    os.subcategory_name,
    os.brand,
    os.months_required_to_own,
    os.rank_subscriptions,
    os.subscriptions_per_customer,
    os.variant_sku,
    os.product_sku,
    o.order_rank,
    o.total_orders,
    o.order_value,
    o.store_type,
    o.new_recurring,
    o.retention_group,
    o.marketing_channel,
    o.store_name,
    o.store_label,
    o.store_country,
    o.store_commercial,
    o.basket_size,
    o.voucher_type,
    o.voucher_discount,
    o.voucher_value,
    ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY discount_seen_date DESC) AS customer_event,
    c.email_subscribe,
    c.subscription_revenue_paid,
    c.burgel_risk_category,
    c.clv,
    c.customer_acquisition_cohort,
    c.rfm_segment,
    c.crm_label,
    c."age" AS customer_age
FROM data_combined s
         LEFT JOIN ods_production.subscription os
                   ON s.sub_id_master = os.subscription_id
         LEFT JOIN master."order" o
                   ON o.order_id = os.order_id
         LEFT JOIN master.customer c
                   ON c.customer_id = o.customer_id
        WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_marketing.v_surprise_dicount_report TO tableau;