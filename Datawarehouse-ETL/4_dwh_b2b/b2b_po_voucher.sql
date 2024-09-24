CREATE OR REPLACE VIEW dm_b2b.v_po_voucher AS 
WITH 
allocations AS (
   SELECT 
      a.subscription_id ,
      a.asset_id,
      a1.asset_name,
      a.allocated_at
   FROM master.allocation AS a 
   LEFT JOIN master.asset a1 
   ON a.asset_id=a1.asset_id
   WHERE TRUE 
   and a.customer_type ='business_customer'
   QUALIFY ROW_NUMBER()Over(PARTITION BY a.subscription_id ORDER BY a.allocated_at DESC)=1
)
SELECT 
   a.asset_id,
   s.subscription_id ,
   s.subscription_bo_id,
   ac.account_name,
   a.asset_name,
   s.start_date,
   s.order_id ,
   s.customer_id,
   s.customer_type,
   s.status,
   s.subscription_value ,
   s.rental_period ,
   s.months_required_to_own,
   s.effective_duration::int,
   s.months_required_to_own::int - (datediff('month',start_date,dateadd('month',-1,date_trunc('month',current_date)))) AS months_required_to_own_from_cm,
   CASE 
      WHEN s.months_required_to_own::int - effective_duration::int<0 THEN 0 
      ELSE s.months_required_to_own::int - effective_duration::int 
   END AS months_to_1_euro,
   CASE 
      WHEN rental_period = 1 THEN 0 
      WHEN effective_duration <= s.rental_period::int::int + 3 THEN 0 
      WHEN months_required_to_own_from_cm::int > 12 THEN 0 
      ELSE greatest(0.15,0.6-(months_to_1_euro*0.01::decimal(30,2)))*100
   END AS voucher_amount_percent,
   
   CASE 
      WHEN rental_period = 1 THEN FALSE
      WHEN effective_duration <= s.rental_period::int::int + 3 THEN FALSE
      WHEN months_required_to_own_from_cm::int > 12 THEN FALSE 
      ELSE TRUE
   END AS is_eligbile_voucher,
   voucher_amount_percent*0.01*s.subscription_value AS voucher_amount,
   subscription_value*0.8 AS voucher_amount_upsell,
    CASE 
      WHEN rental_period = 1 THEN 'Rental period 1 month' 
      WHEN effective_duration <= s.rental_period::int + 3 THEN 'rented less than 3 months beyond min rental period' 
      WHEN months_required_to_own_from_cm::int > 12 THEN 'more than a year for purchase option' 
      ELSE 'Eligible for voucher'
   END AS voucher_reason,
   voucher_amount*1.2 AS min_basket_size
FROM master.subscription s
LEFT JOIN allocations a
ON s.subscription_id =a.subscription_id 
left join ods_b2b.account ac
   on ac.customer_id = s.customer_id 
WHERE true
AND s.customer_type ='business_customer'
AND s.status<>'CANCELLED'
AND s.customer_id <> '29216'---Grover GROUP account
WITH NO SCHEMA binding
;
