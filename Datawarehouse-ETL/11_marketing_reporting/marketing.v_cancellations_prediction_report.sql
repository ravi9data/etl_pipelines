DROP VIEW IF EXISTS marketing.v_cancellations_prediction_report;
CREATE VIEW marketing.v_cancellations_prediction_report AS
WITH dates as (
select datum, product_sku, country_name from ods_production.product p
    , 
      (select datum from public.dim_dates where day_name = 'Monday') ,
      (select distinct country_name from ods_production.store)
where datum > p.created_at) 
,
	expected_cancellations AS (    
SELECT 
  DATE_TRUNC('week',minimum_cancellation_date)::DATE AS fact_date
 ,s.product_sku
 ,s.country_name
 ,COUNT(DISTINCT 
   CASE 
    WHEN status ='ACTIVE' 
     THEN s.subscription_id 
   END) AS active_subscriptions_scheduled_for_cancellation
 ,COUNT(DISTINCT 
   CASE 
    WHEN status ='CANCELLED' 
      AND cancellation_date > minimum_cancellation_date 
     THEN s.subscription_id 
   END) AS cancelled_later
 ,COUNT(DISTINCT 
   CASE 
    WHEN status ='CANCELLED' 
      AND cancellation_date <= minimum_cancellation_date 
     THEN s.subscription_id 
   END) AS cancelled_before
FROM master.subscription s
GROUP BY 1, 2, 3
)  
,cancelled AS (
 SELECT 
  DATE_TRUNC('week',cancellation_date)::DATE AS fact_date
 ,s.product_sku
 ,s.country_name
 ,COUNT(DISTINCT s.subscription_id) AS subs_cancelled
FROM master.subscription s
WHERE TRUE
  AND cancellation_date IS NOT NULL
  AND cancellation_reason_churn = 'customer request' 
GROUP BY 1, 2, 3
)
,ship_label AS (
SELECT 
  DATE_TRUNC('week',a.return_shipment_label_created_at)::DATE AS fact_date
 ,s.product_sku
 ,s.country_name
 ,COUNT(DISTINCT s.subscription_id) AS subscriptions_w_shipping_lable
FROM master.subscription s
  LEFT JOIN master.allocation a 
    ON a.subscription_id = s.subscription_id
WHERE return_shipment_label_created_at IS NOT NULL 
GROUP BY 1, 2, 3
)
,shipped AS ( 
SELECT 
  DATE_TRUNC('week',a.return_shipment_at)::DATE AS fact_date
 ,s.product_sku 
 ,s.country_name
 ,COUNT(DISTINCT s.subscription_id) AS subscriptions_return_shipment
FROM master.subscription s
  LEFT JOIN master.allocation a 
    ON a.subscription_id=s.subscription_id
WHERE return_shipment_at IS NOT NULL
GROUP BY 1, 2, 3
)
,returned AS (
SELECT 
  DATE_TRUNC('week',a.return_delivery_date)::DATE AS fact_date
 ,s.product_sku 
 ,s.country_name
 ,COUNT(DISTINCT s.subscription_id) AS subscriptions_returned
FROM master.subscription s
  LEFT JOIN master.allocation a 
    ON a.subscription_id = s.subscription_id
WHERE return_delivery_date IS NOT NULL 
GROUP BY 1, 2, 3
)
,ab AS (
SELECT 
  DISTINCT d.datum AS fact_date,
  d.country_name
 ,d.product_sku AS product_sku
 ,coalesce(sum(active_subscriptions_scheduled_for_cancellation),0) AS active_subscriptions_scheduled_for_cancellation
 ,coalesce(sum(cancelled_later),0) AS cancelled_subscripions_after_exp
 ,coalesce(sum(cancelled_before),0) AS cancelled_subscripions_before_exp
 ,coalesce(sum(subs_cancelled),0) AS subs_cancelled
 ,coalesce(sum(subscriptions_w_shipping_lable),0) AS subscriptions_w_shipping_lable
 ,coalesce(sum(subscriptions_return_shipment),0) AS subscriptions_return_shipment
 ,coalesce(sum(subscriptions_returned),0) AS subscriptions_returned
FROM dates d 
 left join  expected_cancellations e 
 on d.product_sku = e.product_sku 
   and d.datum =  e.fact_date
   and d.country_name = e.country_name
  left JOIN cancelled c 
    ON  d.datum =c.fact_date 
    AND d.product_sku = c.product_sku
    and d.country_name = c.country_name
  left JOIN ship_label s 
    ON  d.datum =s.fact_date 
    AND d.product_sku = s.product_sku
    and d.country_name = s.country_name 
  left JOIN shipped sh 
    ON  d.datum =sh.fact_date 
    AND d.product_sku = sh.product_sku
    and d.country_name = sh.country_name
  left JOIN returned r 
    ON  d.datum =r.fact_date 
    AND  d.product_sku = r.product_sku 
    and d.country_name = r.country_name
GROUP BY 1, 2,3
)
,final_ as (
SELECT ab.*, 
p.category_name,
p.subcategory_name,
p.product_name
FROM ab
left join ods_production.product p on ab.product_sku = p.product_sku
where (active_subscriptions_scheduled_for_cancellation +
cancelled_subscripions_after_exp +
cancelled_subscripions_before_exp +
subs_cancelled +
subscriptions_w_shipping_lable +
subscriptions_return_shipment +
subscriptions_returned ) > 0 )
select * from final_  
WITH NO SCHEMA BINDING
;
             
             
             