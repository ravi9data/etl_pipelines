DROP TABLE IF EXISTS dm_b2b.gbp_overview;

CREATE TABLE dm_b2b.gbp_overview AS WITH datum AS (
    SELECT
        datum,
        CASE
            WHEN dd.day_is_last_of_month = 1
                THEN 1
            WHEN dd.datum = current_date
                THEN 1
            ELSE 0
        END AS day_is_last_of_month,
        CASE
            WHEN dd.day_name = 'Sunday'
                THEN 1
            WHEN datum = current_date
                THEN 1
            ELSE 0
        END AS day_is_last_of_week
    FROM public.dim_dates dd
    WHERE
        (
            dd.datum >= current_date - 360
            AND dd.datum <= current_date
            AND day_is_last_of_month = 1
        ) --months
        OR (
            dd.datum >= current_date - 60
            AND dd.datum <= current_date
            AND day_is_last_of_week = 1
        ) --weeks
        OR (
            dd.datum >= current_date - 14
            AND dd.datum <= current_date
        ) --recent dates 
        OR dd.datum = '2022-09-27' --baseline
),
pre_companies AS (
    SELECT
        c.customer_id,
        c.company_name,
        c.is_gbp_enabled,
        c.created_at,
        c.status,
        c.gpb_employees_submitted,
        COALESCE(c.is_freelancer, 0) AS is_freelancer
    FROM ods_production.companies c
    WHERE
        (
            (
                c.is_gbp_enabled IS false
                AND c.is_freelancer = 0
            )
            OR c.is_gbp_enabled
        )
),
companies_enriched AS (
    SELECT
        c.customer_id,
        c.company_name,
        c.is_gbp_enabled,
        c.created_at,
        c.status,
        c.gpb_employees_submitted,
        c.is_freelancer,
        cc.crm_label,
        cc.billing_country,
        cc.first_subscription_store,
        cc.signup_country,
        cc.paid_orders,
        cu.consolidation_day
    FROM pre_companies c
    LEFT JOIN MASTER.customer cc
        ON cc.customer_id = c.customer_id
    LEFT JOIN ods_production.customer cu
        ON cu.customer_id = c.customer_id
),
companies AS (
    SELECT DISTINCT
        dd.datum AS fact_date,
        dd.day_is_last_of_month,
        dd.day_is_last_of_week,
        CASE
            WHEN c.is_gbp_enabled = false
                THEN 'Non-GBP'
            WHEN c.is_gbp_enabled != false
                THEN 'GBP'
        END AS customer_logic,
        c.customer_id,
        c.company_name,
        c.is_gbp_enabled,
        c.created_at :: DATE,
        c.status,
        c.gpb_employees_submitted,
        c.crm_label,
        c.billing_country,
        c.first_subscription_store,
        c.signup_country,
        c.consolidation_day,
        CASE
            WHEN c.consolidation_day is not null
                THEN TRUE
            ELSE false
        END AS is_consolidated,
        c.is_freelancer,
        CASE
            WHEN c.paid_orders IS NULL
            OR c.paid_orders = 0
                THEN '0'
            WHEN c.paid_orders > 0
            AND c.paid_orders <= 10
                THEN '1-10'
            WHEN c.paid_orders > 10
            AND c.paid_orders <= 20
                THEN '11-20'
            WHEN c.paid_orders > 20
            AND c.paid_orders <= 30
                THEN '21-30'
            WHEN c.paid_orders > 30
            AND c.paid_orders <= 40
                THEN '31-40'
            WHEN c.paid_orders > 40
            AND c.paid_orders <= 50
                THEN '41-50'
            WHEN c.paid_orders > 50
            AND c.paid_orders <= 100
                THEN '51-100'
            WHEN c.paid_orders > 100
            AND c.paid_orders <= 150
                THEN '101-150'
            WHEN c.paid_orders > 150
            AND c.paid_orders <= 200
                THEN '151-200'
            WHEN c.paid_orders > 200
            AND c.paid_orders <= 250
                THEN '201-250'
            WHEN c.paid_orders > 250
            AND c.paid_orders <= 300
                THEN '251-300'
            WHEN c.paid_orders > 300
            AND c.paid_orders <= 350
                THEN '301-350'
            WHEN c.paid_orders > 350
            AND c.paid_orders <= 400
                THEN '351-400'
            WHEN c.paid_orders > 400
            AND c.paid_orders <= 450
                THEN '401-450'
            WHEN c.paid_orders > 450
            AND c.paid_orders <= 500
                THEN '451-500'
            WHEN c.paid_orders > 500
                THEN '500+'
        END AS paid_orders_range --, 
        --o.is_pay_by_invoice 
    FROM datum dd
    LEFT JOIN companies_enriched c
        ON dd.datum >= c.created_at :: DATE
),
asv AS (
    SELECT
        aso.customer_id,
        aso.fact_date,
        SUM(aso.active_subscriptions) AS active_subscriptions,
        SUM(aso.active_subscription_value) AS asv
    FROM ods_finance.active_subscriptions_overview aso
    WHERE
        aso.customer_id IN (
            SELECT
                customer_id
            FROM pre_companies
        )
        AND aso.fact_date IN (
            SELECT
                datum
            FROM datum
        )
    GROUP BY 1, 2
),
orders AS (
    SELECT
        o.customer_id,
        o.created_date :: DATE AS fact_date,
        COUNT (DISTINCT o.order_id) AS created_orders,
        COUNT (
            DISTINCT CASE
                WHEN o.is_pay_by_invoice
                    THEN o.order_id
            END
        ) AS created_pay_by_invoice_orders,
        COUNT (
            DISTINCT CASE
                WHEN o.submitted_date IS NOT NULL
                    THEN o.order_id
            END
        ) AS submitted_orders,
        COUNT (
            DISTINCT CASE
                WHEN o.submitted_date IS NOT NULL
                AND o.is_pay_by_invoice
                    THEN o.order_id
            END
        ) AS submitted_pay_by_invoice_orders,
        COUNT (
            DISTINCT CASE
                WHEN o.paid_date IS NOT NULL
                    THEN o.order_id
            END
        ) AS paid_orders,
        COUNT (
            DISTINCT CASE
                WHEN o.paid_date IS NOT NULL
                AND o.is_pay_by_invoice
                    THEN o.order_id
            END
        ) AS paid_by_invoice_orders
    FROM ods_production.order o
    WHERE
        o.customer_id IN (
            SELECT
                customer_id
            FROM pre_companies
        )
        AND o.created_date :: DATE IN (
            SELECT
                datum
            FROM datum
        )
    GROUP BY 1, 2
),
subs_pre AS (
    SELECT
        subscription_id,
        customer_id,
        start_date :: DATE,
        cancellation_date :: DATE,
        subscription_value,
        subscription_value_euro
    FROM ods_production.subscription
    WHERE
        customer_id IN (
            SELECT
                customer_id
            FROM pre_companies
        )
),
subscriptions AS (
    SELECT
        s.customer_id,
        s.start_date :: DATE AS fact_date,
        SUM(s.subscription_value) AS acquired_sub_value,
        SUM(s.subscription_value_euro) AS acquired_sub_value_eur,
        COUNT(DISTINCT s.subscription_id) AS acquired_subs
    FROM subs_pre s
    WHERE
        s.start_date :: DATE IN (
            SELECT
                datum
            FROM datum
        )
    GROUP BY 1, 2
),
cancellations AS (
    SELECT
        s.customer_id,
        s.cancellation_date :: DATE AS fact_date,
        SUM(s.subscription_value) AS cancelled_sub_value,
        SUM(s.subscription_value_euro) AS cancelled_sub_value_eur,
        COUNT(DISTINCT s.subscription_id) AS cancelled_subs
    FROM subs_pre s
    WHERE
        s.cancellation_date :: DATE IN (
            SELECT
                datum
            FROM datum
        )
    GROUP BY 1, 2
),
payments_pre AS (
    SELECT
        payment_id,
        customer_id,
        DATE,
        amount_due,
        amount_paid,
        STATUS
    FROM MASTER.subscription_payment_historical sp
    WHERE
        sp.status != 'PLANNED'
        AND sp.customer_id IN (
            SELECT
                customer_id
            FROM pre_companies
        )
        AND sp.date IN (
            SELECT
                datum
            FROM datum
        )
),
pending_payments AS (
    SELECT
        sp.customer_id,
        sp.date AS fact_date,
        SUM(sp.amount_due) AS amount_due,
        SUM(sp.amount_paid) AS amount_paid,
        SUM(
            CASE
                WHEN sp.status != 'PAID'
                    THEN sp.amount_due
            END
        ) AS amount_not_paid,
        COUNT(
            DISTINCT CASE
                WHEN sp.status != 'PAID'
                    THEN sp.payment_id
            END
        ) AS not_paid_ids
    FROM payments_pre sp
    WHERE
        sp.status != 'PLANNED'
        AND sp.customer_id IN (
            SELECT
                customer_id
            FROM pre_companies
        )
        AND sp.date IN (
            SELECT
                datum
            FROM datum
        )
    GROUP BY 1, 2
)
SELECT
    c.fact_date,
    c.day_is_last_of_month,
    c.day_is_last_of_week,
    c.customer_id,
    c.company_name,
    c.is_gbp_enabled,
    c.customer_logic,
    c.created_at,
    c.status,
    c.gpb_employees_submitted,
    c.crm_label,
    c.billing_country,
    c.first_subscription_store,
    c.signup_country,
    c.consolidation_day,
    c.is_consolidated,
    c.is_freelancer,
    c.paid_orders_range,
    a.active_subscriptions,
    a.asv,
    o.created_orders,
    o.created_pay_by_invoice_orders,
    o.submitted_orders,
    o.submitted_pay_by_invoice_orders,
    o.paid_orders,
    o.paid_by_invoice_orders,
    s.acquired_sub_value,
    s.acquired_sub_value_eur,
    s.acquired_subs,
    cc.cancelled_sub_value,
    cc.cancelled_sub_value_eur,
    cc.cancelled_subs,
    p.amount_due,
    p.amount_paid,
    p.amount_not_paid,
    p.not_paid_ids
FROM companies c
LEFT JOIN asv a
    ON c.fact_date = a.fact_date
    AND c.customer_id = a.customer_id
LEFT JOIN orders o
    ON c.fact_date = o.fact_date
    AND c.customer_id = o.customer_id
LEFT JOIN subscriptions s
    ON c.fact_date = s.fact_date
    AND c.customer_id = s.customer_id
LEFT JOIN cancellations cc
    ON c.fact_date = cc.fact_date
    AND c.customer_id = cc.customer_id
LEFT JOIN pending_payments p
    ON c.fact_date = p.fact_date
    AND c.customer_id = p.customer_id;

GRANT
SELECT
    ON dm_b2b.gbp_overview TO tableau;