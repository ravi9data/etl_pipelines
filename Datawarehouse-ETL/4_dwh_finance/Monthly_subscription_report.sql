CREATE OR REPLACE VIEW dm_finance.v_subscription_monthly AS
WITH subs AS (
    SELECT 
    date_trunc('month',start_date )::Date AS Fact_month,
    store_label ,
    store_commercial ,
    subscription_plan ,
    cancellation_reason_churn , 
    count(subscription_id ) AS subscriptions,
    sum(subscription_value ) AS subscription_value,
    sum(committed_sub_value + s.additional_committed_sub_value) AS committed_sub_value ,
    avg(rental_period ) AS rental_period 
    FROM master.subscription s 
    WHERE date_trunc('month',start_date )::Date IS NOT NULL
    GROUP BY 1,2,3,4,5)
,cancelled AS (
    SELECT 
    date_trunc('month',cancellation_date )::Date AS Fact_month,
    store_label ,
    store_commercial ,
    subscription_plan ,
    cancellation_reason_churn , 
    count(subscription_id) AS cancelled_subscritpions,
    sum(subscription_value) AS cancelled_subscritpion_value
    FROM master.subscription s 
    WHERE date_trunc('month',cancellation_date )::Date IS NOT NULL 
    AND cancellation_date IS NOT NULL 
    GROUP BY 1,2,3,4,5)
SELECT 
    dd.datum AS fact_date,
    s.store_label ,
    s.store_commercial ,
    s.subscription_plan ,
    s.cancellation_reason_churn,
    subscriptions,
    subscription_value,
    committed_sub_value,
    rental_period,
    cancelled_subscritpions,
    cancelled_subscritpion_value
FROM public.dim_dates dd 
LEFT JOIN subs s 
    ON dd.datum =s.Fact_month 
LEFT OUTER JOIN cancelled c 
    ON dd.datum =c.fact_month
    AND s.store_label =c.store_label
    AND s.store_commercial = c.store_commercial
    AND s.subscription_plan =c.subscription_plan
    AND s.cancellation_reason_churn=c.cancellation_reason_churn
WHERE dd.day_is_first_of_month =1
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_finance.v_subscription_monthly to tableau;
