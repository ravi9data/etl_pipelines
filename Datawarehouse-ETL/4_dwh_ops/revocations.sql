DROP TABLE IF EXISTS revoked_subs;

CREATE temp TABLE revoked_subs AS
SELECT
    s.subscription_id,
    s.cancellation_date
FROM ods_production.subscription_cancellation_reason s
WHERE
    s.cancellation_reason_new = 'REVOCATION'
    AND s.cancellation_date >= current_date - 360
    AND s.cancellation_date < current_date;

DROP TABLE IF EXISTS dm_operations.revocations_total_assets_revoked;

CREATE TABLE dm_operations.revocations_total_assets_revoked AS
SELECT
    a.allocation_id,
    a.is_recirculated,
    a.warehouse,
    t.category_name,
    t.product_sku,
    t.product_name,
    s.cancellation_date AS revocation_date,
    sp.rental_period,
    a.issue_reason,
    a.issue_comments,
    a.issue_date
FROM revoked_subs s
LEFT JOIN ods_production.allocation a
    ON a.subscription_id = s.subscription_id
LEFT JOIN ods_production.subscription sp
    ON s.subscription_id = sp.subscription_id
LEFT JOIN ods_production.asset t
    ON a.asset_id = t.asset_id;

DROP TABLE IF EXISTS dm_operations.revocations_total_revoked_delivered;

CREATE TABLE dm_operations.revocations_total_revoked_delivered AS WITH total_revoked AS (
    SELECT
        DATE_TRUNC('week', s.cancellation_date) AS revocation_date,
        a.is_recirculated,
        a.warehouse,
        t.product_sku,
        --a.delivered_at,
        COUNT(a.allocation_id) total_revoked
    FROM revoked_subs s
    LEFT JOIN ods_production.allocation a
        ON a.subscription_id = s.subscription_id
    LEFT JOIN ods_production.subscription sp
        ON s.subscription_id = sp.subscription_id
    LEFT JOIN ods_production.asset t
        ON a.asset_id = t.asset_id
    GROUP BY 1, 2, 3, 4
),
total_delivered AS (
    SELECT
        DATE_TRUNC('week', delivered_at) AS delivered_date,
        a.is_recirculated,
        a.warehouse,
        t.product_sku,
        COUNT(a.allocation_id) total_delivered
    FROM ods_production.allocation a
    LEFT JOIN ods_production.subscription s
        ON a.subscription_id = s.subscription_id
    LEFT JOIN MASTER.asset t
        ON a.asset_id = t.asset_id
    WHERE
        rank_allocations_per_subscription = 1
        AND s.store_short != 'Partners Offline'
        AND a.delivered_at >= current_date - 360
        AND a.delivered_at < current_date
    GROUP BY 1, 2, 3, 4
),
union_both AS (
    SELECT
        'revoked' AS metric_name,
        revocation_date AS fact_date,
        is_recirculated,
        warehouse,
        product_sku,
        total_revoked AS metric_value
    FROM total_revoked
    UNION ALL
    SELECT
        'delivered' AS metric_name,
        delivered_date,
        is_recirculated,
        warehouse,
        product_sku,
        total_delivered AS metric_value
    FROM total_delivered
    UNION ALL
    SELECT
        'delivered 2w' AS metric_name,
        dateadd('week', 2, delivered_date),
        is_recirculated,
        warehouse,
        product_sku,
        total_delivered AS metric_value
    FROM total_delivered
)
SELECT
    r.metric_name,
    r.fact_date,
    r.is_recirculated,
    r.warehouse,
    p.category_name,
    r.product_sku,
    p.product_name,
    r.metric_value
FROM union_both r
LEFT JOIN ods_production.product p
    ON r.product_sku = p.product_sku;


GRANT SELECT ON dm_operations.revocations_total_assets_revoked TO tableau;
GRANT SELECT ON dm_operations.revocations_total_revoked_delivered TO tableau;
