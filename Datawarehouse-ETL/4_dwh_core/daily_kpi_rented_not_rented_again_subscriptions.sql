DROP TABLE IF EXISTS dwh.daily_kpi_rented_not_rented_again_subscriptions;
CREATE TABLE dwh.daily_kpi_rented_not_rented_again_subscriptions AS
WITH account_data AS (
    SELECT DISTINCT 
    		c.customer_id,
        ka.account_owner,
        COALESCE(segment,'Self Service') AS segment
    FROM master.customer c
      LEFT JOIN dm_b2b.v_key_accounts ka ON ka.customer_id = c.customer_id
    WHERE customer_type = 'business_customer'
)

-- connecting ALL the cancelled subs WITH ALL NEW possible subs FOR EACH customer AND IN the same subcategory
   , cancelled_subs_with_new_subs AS (
    SELECT
        s1.customer_id,
        s1.customer_type,
        ad.segment,
        s1.new_recurring,
        s1.subscription_id AS subscription_id_cancelled,
        s1.cancellation_date::date,
        s1.category_name,
        s1.subcategory_name,
        s1.country_name,
        case
            WHEN s1.cancellation_reason_new IN ('SOLD EARLY','SOLD 1-EUR') AND s1.cancellation_reason_churn = 'customer request'
                THEN 'customer request, sold'
            WHEN s1.cancellation_reason_churn IS NOT NULL THEN s1.cancellation_reason_churn
            ELSE 'other' END as grouped_cancellation_reason,
        s1.cancellation_reason_churn,
        s1.cancellation_reason_new,
        CASE WHEN s2.subscription_id IS NOT NULL THEN 1 ELSE 0 END AS is_rented_again,
        s1.store_commercial,
        s2.subscription_id AS subscription_id_started_30_days,
        s2.start_date::date AS start_date_of_new_subs,
        s1.subscription_value_euro AS subscription_value_cancelled_contract,
        s2.subscription_value_euro AS subscription_value_new_contract,
        ROW_NUMBER() OVER (PARTITION BY s1.subscription_id order by s1.cancellation_date desc) AS rn_for_cancellation_value --table have duplicated rows now, but we need only 1 row for cancellation value
    FROM master.subscription s1
      LEFT JOIN master.subscription s2
        ON s1.customer_id = s2.customer_id
          AND s1.category_name = s2.category_name
          AND s1.subcategory_name = s2.subcategory_name --CHECK IF the customer IS replacing an asset
          AND s2.start_date::date BETWEEN DATEADD('day',-30,s1.cancellation_date::date) AND DATEADD('day',30,s1.cancellation_date::date)
          AND s1.subscription_id != s2.subscription_id --make sure we ARE NOT considering the same subscription_id
          AND s1.order_created_date < s2.order_created_date
      LEFT JOIN account_data ad
          ON s1.customer_id = ad.customer_id
    WHERE s1.cancellation_date::date BETWEEN CURRENT_DATE - 65 AND CURRENT_DATE -1
)

-- aggregate the numbers for cancelled info to join in the end
   , cancelled_subs_agg AS (
    SELECT
        datepart(week, cancellation_date) AS week_number,
        date_trunc('week',cancellation_date)::date AS cancellation_week,
        cancellation_date::date,
        country_name,
        customer_type,
        segment,
        new_recurring,
        category_name,
        subcategory_name,
        cancellation_reason_churn,
        cancellation_reason_new,
        grouped_cancellation_reason,
        is_rented_again,
        store_commercial,
        COUNT(DISTINCT subscription_id_cancelled) AS subscriptions_cancelled,
        SUM(CASE WHEN rn_for_cancellation_value = 1 THEN subscription_value_cancelled_contract ELSE 0 END) AS subscription_value_cancelled,
        COUNT(DISTINCT customer_id) AS customers_cancelled
    FROM cancelled_subs_with_new_subs 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

-- in order to calculate the subs value and number os subscription proportionally, so in the end, when we sum the whole column
-- would give us the correct value
   , new_subs_prep AS (
    SELECT
        datepart(week, cancellation_date) AS week_number,
        date_trunc('week',cancellation_date)::date AS cancellation_week,
        cancellation_date::date,
        country_name,
        customer_type,
        segment,
        new_recurring,
        category_name,
        subcategory_name,
        cancellation_reason_churn,
        cancellation_reason_new,
        grouped_cancellation_reason,
        is_rented_again,
        store_commercial,
        subscription_id_started_30_days,
        customer_id,
        COUNT(*) OVER (PARTITION BY subscription_id_started_30_days) AS cancellations_related_to_new_subs,
        CASE WHEN subscription_id_started_30_days IS NOT NULL
            THEN 1 ELSE 0 END::FLOAT/cancellations_related_to_new_subs AS subscriptions_rented_again,
        subscription_value_new_contract::FLOAT/cancellations_related_to_new_subs AS subscription_value_rented_again
    FROM cancelled_subs_with_new_subs
)

-- aggregating the new subscription info
   , new_subs_distinct_agg AS (
    SELECT
        week_number,
        cancellation_week,
        cancellation_date,
        country_name,
        customer_type,
        segment,
        new_recurring,
        category_name,
        subcategory_name,
        cancellation_reason_churn,
        cancellation_reason_new,
        grouped_cancellation_reason,
        is_rented_again,
        store_commercial,
        SUM(subscriptions_rented_again) AS subscriptions_rented_again,
        SUM(subscription_value_rented_again) AS subscription_value_rented_again
    FROM new_subs_prep 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

SELECT
    c.week_number,
    c.cancellation_week,
    c.cancellation_date,
    c.country_name,
    c.customer_type,
    c.segment,
    c.new_recurring,
    c.category_name,
    c.subcategory_name,
    c.cancellation_reason_churn,
    c.cancellation_reason_new,
    c.grouped_cancellation_reason,
    c.is_rented_again,
    c.store_commercial,
    c.subscriptions_cancelled,
    c.subscription_value_cancelled,
    c.customers_cancelled,
    CASE
        WHEN c.country_name in ('Austria','Netherlands','Spain')
            THEN 'Rest of Europe'
        WHEN c.country_name = 'United States'
            THEN 'United States Region'
        WHEN c.country_name = 'Germany'
            THEN 'Germany Region'
        ELSE c.country_name END AS region,
    COALESCE(n.subscriptions_rented_again,0) AS new_subscriptions_rented_again,
    COALESCE(n.subscription_value_rented_again,0) AS new_subscription_value_rented_again,
    COALESCE(CASE WHEN c.is_rented_again = 0 THEN c.subscriptions_cancelled END,0 ) AS not_rented_again_subscriptions,
    COALESCE(CASE WHEN c.is_rented_again = 0 THEN c.subscription_value_cancelled END,0 ) AS not_rented_again_subscriptions_value,
    COALESCE(CASE WHEN c.is_rented_again = 1 THEN c.subscriptions_cancelled END,0 ) AS rented_again_subscriptions,
    COALESCE(CASE WHEN c.is_rented_again = 1 THEN c.subscription_value_cancelled END,0 ) AS rented_again_subscriptions_value
FROM cancelled_subs_agg c
   LEFT JOIN new_subs_distinct_agg n
      ON c.week_number = n.week_number
         AND c.cancellation_week = n.cancellation_week
         AND c.cancellation_date = n.cancellation_date
         AND c.country_name = n.country_name
         AND c.customer_type = n.customer_type
         AND c.category_name = n.category_name
         AND c.subcategory_name = n.subcategory_name
         AND c.cancellation_reason_churn = n.cancellation_reason_churn
         AND c.cancellation_reason_new = n.cancellation_reason_new
         AND c.grouped_cancellation_reason = n.grouped_cancellation_reason
         AND c.is_rented_again = n.is_rented_again
         AND c.store_commercial = n.store_commercial
         AND c.new_recurring = n.new_recurring
         AND c.segment = n.segment
ORDER BY 3,4,5,6,7,8,9,10,11,12,13,14;
