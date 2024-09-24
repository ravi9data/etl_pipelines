-- dm_risk.v_subscription_data_cancellation_rate_forecast source

CREATE OR REPLACE VIEW dm_risk.v_subscription_data_cancellation_rate_forecast AS
SELECT DISTINCT
	s.subscription_id,
	s.customer_id,
	s.start_date AS subscription_start_date,
	s.order_id,
	s.country_name,
	s.cancellation_date,
	minimum_cancellation_date,
	s.cancellation_reason_churn,
	s.cancellation_reason_new,
	s.cancellation_reason,
	s.category_name,
	s.subcategory_name ,
	s.brand,
	s.store_type,
	s.customer_type,
	s.minimum_term_months, -- Is the current minimum subscription duration. For example, after the subscription time expires, the customer wants to subscribe for a while longer, this value updates for the duration of the current subscription.
	s.rental_period, -- It is the plan that the customer subscribed to when they started the subscription
	o.new_recurring AS order_new_recurring,
	o.marketing_channel AS order_marketing_channel,
	o.payment_method AS order_payment_method,
	s.subscription_value ,
	s.effective_duration ,
	(s.committed_sub_value + s.additional_committed_sub_value) as committed_sub_value,
	s.product_sku,
	o.voucher_code,
	s.store_commercial
FROM master.subscription s 
INNER JOIN master."order" o 
  ON s.order_id = o.order_id
WHERE s.country_name IN ('Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
AND s.rental_period IS NOT NULL
WITH NO SCHEMA BINDING;
