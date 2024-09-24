DROP TABLE IF EXISTS dm_b2b.gbp_created_orders;

CREATE TABLE dm_b2b.gbp_created_orders AS WITH companies AS (
    SELECT
        c.customer_id,
        c.company_name,
        c.is_gbp_enabled,
        c.is_freelancer,
        c.created_at
    FROM ods_production.companies c
    WHERE
        (
            c.is_gbp_enabled IS false
            AND c.is_freelancer = 0
        )
        OR c.is_gbp_enabled
),
orders AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        store_country,
        STATUS,
        is_pay_by_invoice,
        created_date,
        submitted_date,
        order_value,
        paid_date
    FROM ods_production.order o
    WHERE
        created_date :: DATE >= '2022-09-27'
        AND customer_id IN (
            SELECT
                customer_id
            FROM companies
        )
)
SELECT DISTINCT
    c.customer_id,
    c.company_name,
    c2.created_at AS customer_created_date,
    c.is_gbp_enabled,
    c.is_freelancer,
    CASE
        WHEN c.created_at :: DATE >= '2022-09-28'
            THEN TRUE
        ELSE false
    END AS company_created_after_launch,
    o.created_date,
    o.submitted_date,
    o.order_value,
    o.paid_date,
    o.store_id,
    s.store_name,
    s.store_label,
    o.store_country,
    o.order_id,
    o.status,
    o.is_pay_by_invoice,
    CASE
        WHEN oo.order_id IS NOT NULL
            THEN TRUE
        ELSE false
    END AS is_offer,
    oo.offer_status,
    CASE
        WHEN k.order_id IS NOT NULL
            THEN TRUE
        ELSE false
    END AS is_kits,
    ss.subscription_id,
    ss.status AS subs_status,
    ss.subscription_value,
    ss.category_name,
    ss.subcategory_name,
    ss.brand,
    ss.product_name
FROM orders o
INNER JOIN companies c
    ON c.customer_id = o.customer_id
LEFT JOIN ods_production.customer c2
    ON o.customer_id = c2.customer_id
LEFT JOIN ods_b2b.offers oo
    ON o.order_id = oo.order_id
LEFT JOIN ods_b2b.kits k
    ON k.order_id = o.order_id
LEFT JOIN ods_production.store s
    ON s.id = o.store_id
LEFT JOIN MASTER.subscription ss
    ON ss.order_id = o.order_id;

GRANT
SELECT
    ON dm_b2b.gbp_created_orders TO tableau;