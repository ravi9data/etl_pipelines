--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_order_info;

INSERT INTO hightouch_sources.gdpr_order_info
	(customer_id
	,order_id
	,rental_period
	,subscription_value
	,start_date
	,cancellation_date
	,product_name
	,serial_number
	,order_status
	)
SELECT 
	s.customer_id,
	o.order_id,
	s.rental_period,
	s.subscription_value,
	s.start_date::date AS start_date,
	s.cancellation_date::date AS cancellation_date,
	s.product_name,
	a2.serial_number,
	o.status AS order_status
FROM master.subscription s 
INNER JOIN master."order" o
  ON s.order_id = o.order_id
LEFT JOIN master.allocation a 
  ON s.subscription_id = a.subscription_id 
LEFT JOIN master.asset a2 
  ON a.asset_id = a2.asset_id 
WHERE s.customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
;