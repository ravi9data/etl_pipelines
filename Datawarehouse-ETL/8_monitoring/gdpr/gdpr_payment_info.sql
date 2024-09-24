--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_payment_info;

INSERT INTO hightouch_sources.gdpr_payment_info 
	(customer_id
	,order_id
	,payment_type
	,payment_id
	,payment_date
	,payment_amount
	,payment_status
	,payment_method_used
	,expiring_date
	)
SELECT 
	p.customer_id, 
	p.order_id,
	payment_type, 
	payment_id, 
	due_date::date as payment_date, 
	amount_due as payment_amount, 
	p.status as payment_status,
	CASE 
		WHEN pm.payment_method IN ('Paypal', 'paypal-gateway', 'PayPal')
			THEN 'PayPal:' + ' ' + pm.paypal_email
		WHEN pm.payment_method IN ('AdyenContract', 'bankcard-gateway', 'dalenys-bankcard-gateway')
			THEN pm.card_type + ': ' + '****' + pm.card_number
		WHEN pm.payment_method = 'sepa-gateway'
			THEN 'IBAN:' + ' ' + pm.iban
	END AS payment_method_used,
	b.expiry_month + '/' + b.expiry_year AS expiring_date
FROM ods_production.payment_all p
LEFT JOIN ods_data_sensitive.order_payment_method pm 
  ON pm.order_id = p.order_id
LEFT JOIN ods_data_sensitive.ixopay_transactions b 
  ON p.customer_id = b.customer_id 
  AND p.order_id = b.order_reference
WHERE p.customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
;
