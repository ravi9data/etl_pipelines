DROP TABLE IF EXISTS finance.profit_and_loss_redshift; 

CREATE TABLE finance.profit_and_loss_redshift AS 
--gross_subscription_revenue
SELECT 
  'gross_subscription_revenue' AS dimension
  ,LAST_DAY(erh.fact_date) AS date_
  ,CASE WHEN country_name = 'Germany' AND customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN country_name = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany' 
        ELSE country_name END AS cost_entity
  ,CASE 
      WHEN erh.country_name = 'United States' THEN (SUM(erh.amount_subscription/(1+erh.vat_rate)) + 
         SUM(erh.amount_overdue_fee /(1+erh.vat_rate)) +
         SUM(erh.refund_sub_payment_at_fact_date  /(1+erh.vat_rate))) * exc.exchange_rate_eur
    ELSE SUM(erh.amount_subscription/(1+erh.vat_rate)) + 
         SUM(erh.amount_overdue_fee /(1+erh.vat_rate)) +
         SUM(erh.refund_sub_payment_at_fact_date  /(1+erh.vat_rate))
    END AS amount
FROM dwh.earned_revenue_historical erh
LEFT JOIN trans_dev.daily_exchange_rate exc
ON dateadd('month', 1,date_trunc('month', erh.fact_date) ) = exc.date_
AND erh.currency = exc.currency
WHERE TRUE 
      AND date_trunc('month', erh.fact_date) = dateadd('month',-1,date_trunc('month',report_date))
     AND erh.report_date >= '2023-01-01'
GROUP BY 1,2,3, erh.country_name, exc.exchange_rate_eur
UNION 
--voucher_discount
SELECT 
  'voucher_discount_amount' AS dimension
  ,LAST_DAY(erh.fact_date) AS date_
  ,CASE WHEN erh.country_name = 'Germany' AND erh.customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN erh.country_name = 'Germany' AND erh.customer_type = 'business_customer' THEN 'B2B-Germany' 
        ELSE erh.country_name END AS cost_entity
  ,CASE 
      WHEN erh.country_name = 'United States' THEN SUM(erh.amount_discount/(1+erh.vat_rate))  * exc.exchange_rate_eur
      ELSE sum(erh.amount_discount/(1+erh.vat_rate)) 
      END AS amount
FROM dwh.earned_revenue_historical erh
    LEFT JOIN trans_dev.daily_exchange_rate exc
        ON dateadd('month', 1,date_trunc('month', fact_date) ) = exc.date_
        AND erh.currency = exc.currency
WHERE TRUE 
      AND date_trunc('month', erh.fact_date) = dateadd('month',-1,date_trunc('month',report_date))
      AND erh.report_date  >= '2023-01-01'
GROUP BY 1,2,3, erh.country_name, exc.exchange_rate_eur
UNION 
--internal_customer_asset_sale
SELECT 
  'internal_customer_asset_sale' AS dimension
  ,LAST_DAY(erh.fact_date) AS date_
  --,erh.country_name AS cost_entity
  ,CASE WHEN erh.country_name = 'Germany' AND erh.customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN erh.country_name = 'Germany' AND erh.customer_type = 'business_customer' THEN 'B2B-Germany'
        ELSE erh.country_name 
     END AS cost_entity
  --,currency      
  ,CASE 
      WHEN erh.country_name = 'United States' 
        THEN (SUM(erh.asset_sales_customer_bought_earned_revenue /(1+erh.vat_rate)) 
            +SUM(erh.asset_sales_grover_sold_earned_revenue/(1+erh.vat_rate)) 
            +SUM(erh.refund_asset_payment_at_fact_date  /(1+erh.vat_rate)))  * exc.exchange_rate_eur
      ELSE (SUM(erh.asset_sales_customer_bought_earned_revenue /(1+erh.vat_rate))
           +SUM(erh.asset_sales_grover_sold_earned_revenue/(1+erh.vat_rate)) 
           +SUM(erh.refund_asset_payment_at_fact_date  /(1+erh.vat_rate))) 
    END AS amount
FROM dwh.earned_revenue_historical erh
LEFT JOIN trans_dev.daily_exchange_rate exc
ON dateadd('month', 1,date_trunc('month', fact_date) ) = exc.date_
AND erh.currency = exc.currency
WHERE TRUE 
      AND date_trunc('month', erh.fact_date)= dateadd('month',-1,date_trunc('month',report_date))
      AND erh.report_date >= '2023-01-01'
GROUP BY 1,2,3, erh.country_name, exc.exchange_rate_eur
UNION 
--shipping_revenue
SELECT 
  'shipping_revenue' AS dimension
  ,LAST_DAY(erh.fact_date) AS date_
  ,CASE WHEN country_name = 'Germany' AND customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN country_name = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany' 
        ELSE country_name END AS cost_entity
  ,CASE 
      WHEN erh.country_name = 'United States' 
        THEN (SUM(erh.amount_shipment /(1+erh.vat_rate)) 
             +SUM(erh.refund_shipment_payment_at_fact_date  /(1+erh.vat_rate)) * exc.exchange_rate_eur) 
      ELSE SUM(erh.amount_shipment /(1+erh.vat_rate)) 
          +SUM(erh.refund_shipment_payment_at_fact_date  /(1+erh.vat_rate))
    END AS amount
FROM dwh.earned_revenue_historical erh 
    LEFT JOIN trans_dev.daily_exchange_rate exc
        ON dateadd('month', 1,date_trunc('month', fact_date) ) = exc.date_
        AND erh.currency = exc.currency
WHERE TRUE 
      AND date_trunc('month', erh.fact_date) = dateadd('month',-1,date_trunc('month',report_date))
      AND erh.report_date >= '2023-01-01'
GROUP BY 1,2,3, erh.country_name, exc.exchange_rate_eur
UNION 
--nr_of_standard_shipments
SELECT 
  'nr_of_standard_shipments' AS dimension
  ,LAST_DAY(fact_date) AS date_
  --,shipping_country AS cost_entity
  ,CASE WHEN shipping_country = 'Germany' AND customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN shipping_country = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany' 
        ELSE shipping_country END AS cost_entity
  ,sum(total_shipments) AS amount
FROM dm_operations.shipments_daily sd 
WHERE shipment_type = 'outbound'
  AND carrier IN ('DHL', 'UPS')
  AND date_trunc('month', fact_date) >= '2023-01-01' 
GROUP BY 1,2,3
UNION 
--bulky_shipments
SELECT 
  'nr_of_bulky_shipments' AS dimension
  ,LAST_DAY(DATE_TRUNC('month', fact_date)) AS "date"
  --,shipping_country AS cost_entity
  ,CASE WHEN shipping_country = 'Germany' AND customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN shipping_country = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany' 
        ELSE shipping_country END AS cost_entity
  ,SUM(total_shipments) AS amount 
FROM dm_operations.shipments_daily sd 
WHERE shipment_type = 'outbound'
  AND carrier = 'HERMES'
  AND date_trunc('month', fact_date) >= '2023-01-01' 
GROUP BY 1,2,3
UNION 
--new_subscriptions
SELECT 
  'new_subscriptions' AS dimension
  ,LAST_DAY(DATE_TRUNC('month', start_date)) AS "date"
  --,country_name AS cost_entity
  ,CASE WHEN country_name = 'Germany' AND customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN country_name = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany'
        ELSE country_name END AS cost_entity
  ,COUNT(DISTINCT subscription_id) AS amount
FROM master.subscription s 
WHERE  date_trunc('month', start_date) >= '2023-01-01'
--DATE_TRUNC('month',start_date) = DATEADD('month',-1, DATE_TRUNC('month',CURRENT_DATE))
GROUP BY 1,2,3
UNION 
--nr_of_subscriptions
SELECT 
  'nr_of_subscriptions' AS dimension
  ,LAST_DAY(DATE_TRUNC('month', fact_date)) AS "date"
  --,country AS cost_entity
  ,CASE 
    WHEN country = 'Germany' AND store_label IN ('Grover-DE', 'Partnerships-Total') 
       THEN 'B2C-Germany' 
    WHEN country = 'Germany' AND store_label IN ('B2B-Non Freelancers', 'B2B-Freelancers') 
       THEN 'B2B-Germany' 
    ELSE country END AS cost_entity
  ,SUM(active_subscriptions) AS amount
FROM dwh.reporting_churn_store_commercial rcsc
WHERE (CASE WHEN fact_date = DATEADD('day', -1,CURRENT_DATE) THEN 1 
            WHEN  month_eom = CURRENT_DATE THEN 0
            WHEN  month_eom = fact_date THEN 1
            ELSE 0
       END) = 1
       AND date_trunc('month', fact_date) >= '2023-01-01'
   -- AND DATE_TRUNC('month',"date") = DATEADD('month',-1, DATE_TRUNC('month',CURRENT_DATE))
GROUP BY 1,2,3
UNION 
--RETURNS 
SELECT 
  'returns' AS dimension
  ,LAST_DAY(fact_date) AS date_
  --,shipping_country AS cost_entity
  ,CASE WHEN shipping_country = 'Germany' AND customer_type ='normal_customer' THEN 'B2C-Germany'
        WHEN shipping_country = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany' 
       ELSE shipping_country END AS cost_entity 
  ,SUM(total_shipments) AS amount 
FROM dm_operations.shipments_daily sd 
WHERE shipment_type = 'return'
AND date_trunc('month', fact_date) >= '2023-01-01'
    --AND DATE_TRUNC('month', fact_date) = DATEADD('month',-1, DATE_TRUNC('month',CURRENT_DATE))
GROUP BY 1,2,3
;

GRANT SELECT ON finance.profit_and_loss_redshift TO tableau;
; 