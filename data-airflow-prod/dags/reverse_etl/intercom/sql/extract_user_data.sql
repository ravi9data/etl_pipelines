WITH trustpilot_segment AS (
    SELECT DISTINCT customer_id,
                    current_date AS updated_at,
                    TRUE         AS trustpilot_segment
    FROM master.subscription s
             LEFT JOIN master.allocation a ON a.subscription_id = s.subscription_id
    WHERE s.status = 'ACTIVE'
      AND s.customer_type = 'normal_customer'
      AND a.is_recirculated = 'New'
      AND datediff('d', start_date, first_asset_delivery_date) <= 5
    GROUP BY 1
),
     active_escooter_segment AS (
         SELECT DISTINCT customer_id,
                         current_date AS updated_at,
                         'true'::text AS active_escooter
         FROM master.subscription s
                  LEFT JOIN master.allocation a ON a.subscription_id = s.subscription_id
         WHERE s.status = 'ACTIVE'
           AND s.subcategory_name = 'Scooters'
           AND a.delivered_at IS NOT NULL
           AND a.return_shipment_at IS NULL
           AND first_asset_delivery_date < '2020-03-01'
         GROUP BY 1
     ),
     blackcat_segment AS (
         SELECT DISTINCT s.customer_id,
                         current_date AS updated_at,
                         TRUE         AS blackcat_segment
         FROM master.subscription s
                  LEFT JOIN master."order" o
                            ON s.order_id = o.order_id
         WHERE TRUE
           AND s.status = 'ACTIVE'
           AND current_date >= minimum_cancellation_date - 7
           AND minimum_cancellation_date > current_date
           AND last_return_shipment_at IS NULL
         ORDER BY minimum_cancellation_date
     ),
     outstanding_payment AS (
         SELECT customer_id,
                COALESCE(SUM(COALESCE(sp.amount_due, 0)), 0) AS amount_due
         FROM ods_production.payment_subscription sp
         WHERE status IN ('FAILED', 'FAILED FULLY', 'NOT PROCESSED')
           AND subscription_id IS NOT NULL
         GROUP BY 1
     ),
     subs AS (
         SELECT s.customer_id, min(s.start_date) AS pending_allocation_delay_at
         FROM master.subscription s
         WHERE status = 'ACTIVE'
           AND allocation_status = 'PENDING ALLOCATION'
           AND allocated_assets IS NULL
         GROUP BY 1)
SELECT c.customer_id                                                AS user_id,
       c.customer_type,
       c.subscription_limit                                         AS subscription_limit,
       c.email_subscribe                                            AS email_subscribe,
       c.subscriptions                                              AS lifetime_subscriptions,
       c.active_subscriptions                                       AS active_subscriptions,
       EXTRACT(EPOCH FROM
               CASE
                   WHEN last_order_created_date > COALESCE(max_submitted_order_date, '1990-05-22')
                       THEN last_order_created_date
                   END)::BIGINT                                     AS last_abandoned_cart_at,
       CASE
           WHEN last_order_created_date > COALESCE(max_submitted_order_date, '1990-05-22')
               THEN last_cart_product_names
           END                                                      AS last_abandoned_cart_product,
       EXTRACT(EPOCH FROM
               c.minimum_cancellation_date
           )::BIGINT                                                AS next_min_cancellation_at,
       c.minimum_cancellation_product                               AS next_min_cancellation_product,
       COALESCE(t.trustpilot_segment, FALSE)                        AS trustpilot_segment,
       COALESCE(e.active_escooter, 'false'::TEXT)                   AS active_escooter,
       EXTRACT(EPOCH FROM c.max_asset_delivered_at)::BIGINT         AS last_asset_delivered_at,
       COALESCE(completed_orders, 0)                                AS submitted_orders,
       EXTRACT(EPOCH FROM subs.pending_allocation_delay_at)::BIGINT AS pending_allocation_delay_at,
       COALESCE(b.blackcat_segment, FALSE)                          AS blackcat_segment,
       COALESCE(ROUND(op.amount_due, 2), 0)                         AS outstanding_amount
FROM master.customer c
         LEFT JOIN trustpilot_segment t
                   ON t.customer_id = c.customer_id
         LEFT JOIN blackcat_segment b
                   ON b.customer_id = c.customer_id
         LEFT JOIN active_escooter_segment e
                   ON e.customer_id = c.customer_id
         LEFT JOIN subs ON subs.customer_id = c.customer_id
         LEFT JOIN outstanding_payment op
                   ON op.customer_id = c.customer_id
WHERE c.updated_at >= current_timestamp - INTERVAL '{interval_hour}'
ORDER BY random();
