select
	subscription_id,
	customer_id,
	subscription_start_date,
	order_id,
	country_name,
	cancellation_date,
	minimum_cancellation_date,
	cancellation_reason_churn,
	cancellation_reason_new,
	cancellation_reason,
	category_name,
	subcategory_name,
	brand,
	store_type,
	customer_type,
	minimum_term_months,
	rental_period,
	order_new_recurring,
	order_marketing_channel,
	order_payment_method,
	subscription_value,
	effective_duration,
	committed_sub_value,
	product_sku,
	voucher_code,
	store_commercial
from
	dm_risk.v_subscription_data_cancellation_rate_forecast
where
	country_name = '{country_name}'
	and customer_type = '{customer_type}'
	and store_commercial = '{store}'
