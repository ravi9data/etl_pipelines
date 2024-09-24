WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(net_subscription_revenue_paid) AS total_customer_revenue
    FROM
        master.subscription s
    WHERE
        customer_type = 'normal_customer'
        AND cancellation_date > '2021-01-01'
    GROUP BY 1
),
subscription_resubscription AS (
    SELECT
        s1.customer_id,
        s1.subscription_id,
        s1.start_date,
        s1.cancellation_date,
        CASE
            WHEN EXISTS (
                SELECT
                    1
                FROM
                    master.subscription AS s2
                WHERE
                    s2.customer_id = s1.customer_id
                    AND s2.order_created_date >= s1.start_date
                    --checks that the order_created_date is less than or equal to 6 months after the cancellation_date.
                    AND s2.order_created_date <= (s1.cancellation_date + INTERVAL '1 day' * 30 * 6)
                    AND s2.subscription_id != s1.subscription_id
                    AND s2.customer_type = 'normal_customer'
                    AND s2.store_id = 1
            ) THEN 'Yes'
            ELSE 'No'
        END AS did_resubscribe
    FROM
        master.subscription AS s1
    WHERE
        s1.cancellation_reason_new IN (
            'RETURNED EARLY',
            'SOLD EARLY',
            'REVOCATION',
            'SOLD 1-EUR',
            'RETURNED ON TIME',
            'RETURNED LATER'
        )
        AND s1.customer_type = 'normal_customer'
       AND s1.store_id = 1
)
SELECT
    sr.customer_id,
    c.age,
    cr.total_customer_revenue,
    s.subscription_id,
    s.order_id,
    s.subscriptions_per_customer,
    s.product_name,
    s.category_name,
    s.subcategory_name,
    s.brand,
    s.rental_period,
    s.subscription_value,
    s.cancellation_reason_new,
    s.start_date,
    s.cancellation_date,
    EXTRACT(MONTH FROM s.start_date) AS start_month,
    EXTRACT(MONTH FROM s.cancellation_date) AS cancellation_month,
    datediff(day, s.start_date, s.cancellation_date) AS order_keeping_duration,
	CASE
    WHEN EXISTS (
      SELECT 1
      FROM master.subscription AS s2
      WHERE s2.customer_id = s.customer_id
        AND s2.subscription_id <> s.subscription_id
        AND customer_type ='normal_customer'
        --AND s1.start_date <= s2.cancellation_date + INTERVAL '1 day' * 30 * 6
        --AND s1.rental_period = s2.rental_period
        --AND s2.cancellation_date IS NULL
        and status = 'ACTIVE'
        AND store_id = 1
    ) THEN true
    ELSE false
  END AS has_other_rental
    ,sr.did_resubscribe
FROM
    subscription_resubscription AS sr
JOIN
    master.customer c ON c.customer_id = sr.customer_id
JOIN
    master.subscription s ON s.subscription_id = sr.subscription_id
JOIN
    customer_revenue cr ON cr.customer_id = sr.customer_id
