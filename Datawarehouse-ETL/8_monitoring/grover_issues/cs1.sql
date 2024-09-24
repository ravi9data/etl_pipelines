DROP TABLE IF EXISTS monitoring.cs1;

CREATE TABLE monitoring.cs1 AS

select s.customer_id,
	s.customer_type,
	subscription_sf_id,
	s.status as subscription_status,
	cancellation_date,
	cancellation_note,
	allocation_status,
	s.product_name,
	s.product_sku,
	s.variant_sku,
	s.store_label,
	email,
	phone_number,
	order_id,
	order_created_date,
	CURRENT_DATE::Date - order_created_date::date as days_since_order,
	net_subscription_revenue_paid,
	first_asset_delivery_date,
	delivered_assets,
	returned_packages,
	case when days_since_order <= 7 then 'Less than 7 days'
		 when (days_since_order > 7 and days_since_order <=14) then '8-14 days late'
		 else 'More than 14 days'
		 end as days_bucket,
		 case when (cancellation_note  is not null  and net_subscription_revenue_paid <= 0) then 'Cancellation Date Missing'
			  when net_subscription_revenue_paid <= 0 then 'Payment already Refunded, Subscription to be Cancelled'
			  when days_since_order > 14 then 'To be refunded, Subscription to be Cancelled'
			  else days_bucket
			  end as action_to_be_taken
	from master.subscription s
    left join ods_data_sensitive.customer_pii cp on s.customer_id = cp.customer_id
	where s.status ='ACTIVE'
	and s.allocation_status ='PENDING ALLOCATION'
	and s.store_short ='Partners Online'
	order by days_since_order DESC ;

DROP TABLE IF EXISTS skyvia.monitoring_cs1;

CREATE TABLE skyvia.monitoring_cs1 AS
SELECT * FROM monitoring.cs1;

GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO skyvia;
