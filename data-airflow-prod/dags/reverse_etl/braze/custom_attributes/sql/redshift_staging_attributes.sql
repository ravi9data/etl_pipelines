WITH review_segment AS (
        SELECT DISTINCT s.customer_id,
                        current_timestamp AS updated_at,
                        'true'              AS qualified_for_review_invitation
        FROM master.subscription s
                 LEFT JOIN master.allocation a ON a.subscription_id = s.subscription_id
                 LEFT JOIN master.asset asset ON asset.asset_id = a.asset_id
        WHERE asset.total_allocations_per_asset < 3
          AND datediff('d', start_date, first_asset_delivery_date) <= 7
        GROUP BY 1
    ),

    sessions_consent AS (
        SELECT a.session_id,
               CASE WHEN a.marketing_consent = 'yes' THEN TRUE ELSE FALSE END AS ad_user_data,
               CASE WHEN a.marketing_consent = 'yes' THEN TRUE ELSE FALSE END AS ad_personalization,
               b.customer_id::INT AS customer_id,
               b.session_start AS consent_date,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY session_start DESC) AS rn
        FROM segment.cookie_consent a
            INNER JOIN traffic.sessions b ON a.session_id = b.session_id::BIGINT
        WHERE traffic_source = 'segment_web'
    ),

    grover_cash AS (
        SELECT customer_id, 
               SUM(CASE WHEN event_name = 'Earning' THEN grover_cash END) - SUM(CASE WHEN event_name = 'Redemption' THEN grover_cash END) AS grover_cash_balance,
               MAX(event_timestamp) AS updated_at
        FROM ods_grover_card.grover_cash
        WHERE transaction_type NOT IN ('canceled', 'over_limit') OR transaction_type IS NULL
        GROUP BY 1
    ),

    order_scoring AS (
        SELECT user_id,
               verification_state,
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_created_at DESC) AS row_n
        FROM ods_production.order_scoring
    ),

    total_subs AS (
        SELECT customer_id,
               COUNT(DISTINCT subscription_id) AS total_subscriptions
        FROM master.subscription
        GROUP BY 1
    ),

    one_product_in_order AS (
        SELECT order_id,
               COUNT(subscription_id) AS total_subs
        FROM master.subscription
        WHERE status = 'ACTIVE'
        GROUP BY 1
    ),

    first_subscription_date AS (
        SELECT customer_id,
               start_date AS date_first_active_subscription,
               variant_sku AS first_ever_rented_variant_sku,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date ASC) AS rn
        FROM ods_production.subscription
    ),

    active_subs AS (
        SELECT customer_id,
               start_date,
               product_name AS last_active_subscription_product_name,
               category_name AS last_active_subscription_category,
               subcategory_name AS last_active_subscription_subcategory,
               ROW_NUMBER()
               OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS rn
        FROM master.subscription
            INNER JOIN one_product_in_order USING (order_id)
        WHERE status = 'ACTIVE'
    ),

    churn_prevention AS (
        SELECT a.customer_id,
               MAX(a.created_at) AS created_at,
               COUNT(DISTINCT CASE WHEN b.status = 'ACTIVE' THEN a.subscription_id END) AS active_subscriptions
        FROM data_science.churn_prediction_v1 a
            LEFT JOIN master.subscription b USING (subscription_id)
        GROUP BY 1
    )

SELECT c.customer_id                                         AS external_id,
       'true'                                                AS _update_existing_only,
       c.crm_label_braze                                     AS customer_label,
       c.profile_status                                      AS profile_status,
       c.company_status,
       c.customer_type,
       TO_CHAR(c.company_created_at, 'yyyy-MM-dd HH:mm:ss')  AS company_created_at,
       c.trust_type,
       c.company_type_name                                   AS company_type,
       COALESCE(rs.qualified_for_review_invitation, 'false') AS qualified_for_review_invitation,
       c.ever_rented_products                                AS lifetime_rented_product,
       c.ever_rented_categories                              AS lifetime_rented_category,
       c.ever_rented_subcategories                           AS lifetime_rented_subcategory,
       c.ever_rented_brands                                  AS lifetime_rented_brand,
       c.ever_rented_sku                                     AS lifetime_rented_sku,
       c.subscription_durations                              AS lifetime_rental_plan,
       c.voucher_usage                                       AS lifetime_voucher_redeemed,
       c.rfm_segment,
       c.active_subscriptions,
       os.verification_state,
       ROUND(gc.grover_cash_balance,0) AS grover_cash_balance,
       a.last_active_subscription_product_name,
       a.last_active_subscription_category,
       a.last_active_subscription_subcategory,
       ts.total_subscriptions,
       sc.ad_user_data,
       sc.ad_personalization,
       c.ever_rented_variant_sku                             AS lifetime_rented_variant_sku,
       fs.date_first_active_subscription,
       fs.first_ever_rented_variant_sku,
       CASE WHEN cp.customer_id IS NOT NULL AND cp.active_subscriptions >= 1 THEN TRUE ELSE FALSE END AS potential_churner
FROM master.customer c
    LEFT JOIN review_segment rs ON rs.customer_id = c.customer_id
    LEFT JOIN order_scoring os ON os.user_id = c.customer_id AND row_n =1
    LEFT JOIN grover_cash gc ON gc.customer_id = c.customer_id
    LEFT JOIN active_subs a ON a.customer_id = c.customer_id AND a.rn = 1
    LEFT JOIN total_subs ts ON ts.customer_id = c.customer_id
    LEFT JOIN sessions_consent sc ON sc.customer_id = c.customer_id AND sc.rn = 1
    LEFT JOIN first_subscription_date fs ON fs.customer_id = c.customer_id AND fs.rn = 1
    LEFT JOIN churn_prevention cp ON cp.customer_id = c.customer_id
WHERE GREATEST(c.updated_at::TIMESTAMP, gc.updated_at::TIMESTAMP, a.start_date::TIMESTAMP, sc.consent_date::TIMESTAMP, cp.created_at::TIMESTAMP) >= CURRENT_TIMESTAMP - ('{interval_minutes}' ||' minutes')::INTERVAL;
