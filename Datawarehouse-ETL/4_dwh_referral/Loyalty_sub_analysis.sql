CREATE OR REPLACE VIEW pawan_pai.v_loyalty_sub_analysis AS (
with l_cust as (	
SELECT 
	DISTINCT customer_id
	FROM
		master_referral.loyalty_customer
	WHERE
		 first_event_name = 'referral_guest'
		)
, subs as (
	Select 
		subscription_id ,
		customer_id ,
		order_id ,
		Start_date,
		subscription_plan ,
		subscription_value ,
		rental_period,
		s.payment_count ,
		minimum_term_months,
		(committed_sub_value + s.additional_committed_sub_value) as committed_sub_value,
		country_name,
		status ,
		cancellation_date ,
		minimum_cancellation_date ,
		cancellation_note ,
		cancellation_reason ,
		cancellation_reason_new ,
		cancellation_reason_churn
	from master.subscription s 
Select customer_id,
	WHERE user_classification<>'Both' and earning_type ='referral_guest' 
	GROUP BY 1)
	Select customer_id,
	WHERE event_name='Redemption' 
	GROUP BY 1)
,cash AS (
	SELECT 
		ge.customer_id,
	ON ge.customer_id=gr.customer_id)
,sub_payments AS (
	SELECT 
		subscription_id ,
		sum(CASE WHEN amount_voucher<0 THEN  amount_voucher*-1 ELSE amount_voucher END) AS amount_voucher,
		sum(CASE WHEN amount_discount<0 THEN  amount_discount*-1 ELSE amount_discount END) AS amount_discount
	FROM master.subscription_payment sp 
	GROUP BY 1)
,orders AS (
	SELECT 
		customer_id,
		order_id,
		voucher_discount,
		is_voucher_recurring
	FROM ods_production.ORDER 
	WHERE is_special_voucher <> 'no voucher')
SELECT 
	s.subscription_id ,
	s.customer_id ,
	s.order_id ,
	s.Start_date,
	s.subscription_plan ,
	s.subscription_value ,
	s.rental_period,
	s.payment_count,
	s.minimum_term_months,
	s.committed_sub_value,
	s.country_name,
	s.status ,
	s.cancellation_date ,
	s.minimum_cancellation_date ,
	s.cancellation_note ,
	s.cancellation_reason ,
	s.cancellation_reason_new ,
	s.cancellation_reason_churn,
	o.voucher_discount,
	is_voucher_recurring,
	COALESCE(amount_voucher,0) AS amount_voucher,
	COALESCE(amount_discount,0) AS amount_discount,
	redemption,
	COALESCE(amount_discount,0)/committed_sub_value AS discount_availed_prcnt,
   CASE WHEN rental_period<payment_count AND rental_period=minimum_term_months THEN true ELSE false END AS exceeding_min_terms,
   CASE WHEN exceeding_min_terms =TRUE THEN s.subscription_value*payment_count ELSE committed_sub_value END AS effect_csv,
   COALESCE(amount_discount,0)/effect_csv AS discount_availed_prcnt_new,
	CASE
		WHEN discount_availed_prcnt_new='0' THEN '0'
		WHEN discount_availed_prcnt_new<='0.25' THEN 'less than 25'
		WHEN discount_availed_prcnt_new>'0.25' AND discount_availed_prcnt_new<='0.50' THEN 'BETWEEN 25-50'
		WHEN discount_availed_prcnt_new>'0.50' AND discount_availed_prcnt_new<='0.65' THEN 'BETWEEN 50-65'
		WHEN discount_availed_prcnt_new>'0.65' AND discount_availed_prcnt_new<='0.80' THEN 'BETWEEN 65-80'
		WHEN discount_availed_prcnt_new>'0.80' AND discount_availed_prcnt_new<='0.90' THEN 'BETWEEN 80-90'
		WHEN discount_availed_prcnt_new>'0.90' AND discount_availed_prcnt_new<='1' THEN 'BETWEEN 90-100'
		ELSE 'others'
	END AS discount_availed_label
FROM subs s 
INNER JOIN l_cust lc ON lc.customer_id= s.customer_id 
LEFT JOIN orders o on o.order_id=s.order_id
LEFT JOIN sub_payments sp on sp.subscription_id =s.subscription_id  
LEFT JOIN cash c ON c.customer_id=s.customer_id 
ORDER BY exceeding_min_terms DESC
)
WITH NO SCHEMA binding;