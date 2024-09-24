
--Test Query for customers whose subscription is ending soon "Customers_whose_subscription_ending_soon_2023_10_19.csv"


WITH customer_revenue AS (
SELECT
    customer_id,
    SUM(net_subscription_revenue_paid) AS total_customer_revenue
FROM
    master.subscription s
WHERE
    customer_type = 'normal_customer'
    AND cancellation_date > '2021-01-01'
GROUP BY 1)
select distinct s.subscription_id,
    s.customer_id,
    s.product_sku,
    c.age,
    cr.total_customer_revenue,
    s.order_id,
    s.subscriptions_per_customer,
    s.product_name,
    s.category_name,
    s.subcategory_name,
    s.brand,
    s.rental_period,
    s.subscription_value,
    s.cancellation_reason_new,
    cast(s.start_date as date),
    cast (s.minimum_cancellation_date as date) as cancellation_date,
    EXTRACT(MONTH FROM s.start_date) AS start_month,
    EXTRACT(MONTH FROM s.minimum_cancellation_date) AS cancellation_month,
    datediff(day, s.start_date, s.minimum_cancellation_date) AS order_keeping_duration,
      CASE
    WHEN EXISTS (
      SELECT 1
      FROM master.subscription AS s2
      WHERE s2.customer_id = s.customer_id
        AND s2.subscription_id <> s.subscription_id
        AND customer_type ='normal_customer'
        --AND s1.start_date <= s2.minimum_cancellation_date  + INTERVAL '1 day' * 30 * 6
        --AND s1.rental_period = s2.rental_period
        --AND s2.cancellation_date IS NULL
        and status = 'ACTIVE'
        AND store_id = 1
    ) THEN 'Yes'
    ELSE 'No'
  END AS has_other_rental
FROM
    master.subscription s
JOIN
    master.customer c ON c.customer_id = s.customer_id
JOIN
    customer_revenue cr ON cr.customer_id = s.customer_id
WHERE s.minimum_cancellation_date BETWEEN
    DATE_TRUNC('month', CURRENT_DATE) AND
    DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day'
and s.store_id = 1
--AND s.start_date <= s.minimum_cancellation_date  + INTERVAL '1 day' * 30 * 6
and s.customer_type = 'normal_customer'
and cancellation_reason_new isnull
and status = 'ACTIVE'
--Making sure to take out customers who already exceeded their cancellation date and still renting
AND DATE_PART('month', s.start_date) + s.rental_period = DATE_PART('month', s.minimum_cancellation_date)