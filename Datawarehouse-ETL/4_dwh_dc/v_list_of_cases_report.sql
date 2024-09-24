CREATE OR REPLACE VIEW dm_debt_collection.v_list_of_cases_report AS
WITH market_valuation AS (
SELECT 
  a.subscription_id
 ,SUM(aa.residual_value_market_price) AS finco_market_valuation
FROM ods_production.allocation a
  LEFT JOIN master.asset aa
    ON a.asset_id = aa.asset_id 
GROUP BY 1
)
,cancellation_letters AS (
SELECT
  CASE 
   WHEN s.subscription_id LIKE 'S-%'
    THEN s.subscription_id
   ELSE  'S-' || s.subscription_id 
  END AS subscription_sf_id
 ,tk.accountid
 ,MAX(tk.activitydate::DATE) AS last_cancellation_letter_date
FROM stg_salesforce.vw_task_subscriptions s
  LEFT JOIN stg_salesforce.task tk 
    ON s.task_id = tk.id
WHERE tk.category__c = 'Debt Collection - Cancellation'
GROUP BY 1, 2
)
,billing_addresses AS (
SELECT DISTINCT 
  customer_id
 ,LAST_VALUE(billingstreet) OVER (PARTITION BY customer_id ORDER BY created_date ASC 
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS billingstreet
 ,LAST_VALUE(billingcity) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS billingcity
 ,LAST_VALUE(billingpostalcode) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS billingpostalcode
 ,LAST_VALUE(billingcountry) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS billingcountry
FROM ods_production.order
WHERE TRUE 
  AND billingstreet IS NOT NULL
  AND billingcity IS NOT NULL
  AND billingpostalcode IS NOT NULL
  AND billingcountry IS NOT NULL
  AND billingcountry <>'United States'
),
shipping_addresses AS (
SELECT DISTINCT 
  customer_id
 ,LAST_VALUE(shippingstreet) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS shippingstreet
 ,LAST_VALUE(shippingcity) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS shippingcity
 ,LAST_VALUE(shippingpostalcode) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS shippingpostalcode
 ,LAST_VALUE(shippingcountry) OVER (PARTITION BY customer_id ORDER BY created_date ASC
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS shippingcountry
FROM ods_production.order
WHERE TRUE 
   AND shippingstreet IS NOT NULL 
   AND shippingcity IS NOT NULL 
   AND shippingpostalcode IS NOT NULL 
   AND shippingcountry IS NOT NULL
   AND shippingcountry <> 'United States'
)
,customer_identification AS (
SELECT 
  user_id AS customer_id
 ,identification_number
 ,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY extracted_at DESC, created_at DESC) AS rowno
FROM s3_spectrum_rds_dwh_api_production_sensitive.personal_identifications
)
,paid_subscriptions_excluding_refunds AS (
SELECT 
  subscription_id
 ,COUNT(DISTINCT CASE
   WHEN paid_date IS NOT NULL 
    THEN payment_id 
   ELSE NULL::CHARACTER VARYING
  END) AS payment_paid
FROM master.subscription_payment
WHERE currency <> 'USD'
GROUP BY 1
)
,failed_payment_invoice_details AS (
SELECT 
  subscription_id
 ,CASE  
   WHEN (status IN ('FAILED','FAILED FULLY')
      AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN payment_number 
  END AS failed_payment_numbers
 ,COALESCE(CASE  
   WHEN (status IN ('FAILED','FAILED FULLY') AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN amount_due::DECIMAL(10,2)
   END, 0) AS failed_payment_amount_due
 ,COALESCE(CASE  
   WHEN (status IN ('FAILED','FAILED FULLY') AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN amount_paid::DECIMAL(10,2)   
   END, 0) AS failed_payment_amount_paid
 ,failed_payment_amount_due - failed_payment_amount_paid AS failed_payment_amount_over_due
 ,CASE  
   WHEN (status IN ('FAILED','FAILED FULLY') AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN invoice_date  
  END AS failed_payment_invoice_dates
 ,CASE  
   WHEN (status IN ('FAILED','FAILED FULLY') AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN billing_period_start::DATE|| ' - ' || billing_period_end::DATE 
  END AS failed_payment_billing_dates
 ,CASE  
   WHEN (status IN ('FAILED','FAILED FULLY') AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN invoice_number
  END AS failed_payment_invoice_numbers   
 ,CASE  
   WHEN (status IN ('FAILED','FAILED FULLY') AND failed_date IS NOT NULL) 
     OR (status = 'PAID' AND amount_due::DECIMAL(10,2) > amount_paid::DECIMAL(10,2))
     OR (status = 'PENDING' AND payment_method = 'pay-by-invoice' AND due_date <= CURRENT_DATE)
    THEN invoice_url   
  END AS failed_payment_invoice_url
FROM master.subscription_payment
WHERE TRUE 
  AND failed_payment_numbers IS NOT NULL 
  AND currency <> 'USD'
)
,written_off_assets AS (
SELECT 
  s.subscription_id
 ,s.order_id 
 ,s.status
 ,ah.asset_id
 ,ah.asset_status_original
 ,ah.residual_value_market_price AS finco_market_value_at_cancellation
 ,ah.date
FROM master.subscription s
  LEFT JOIN master.asset_historical ah
    ON s.subscription_id = ah.subscription_id
   AND s.cancellation_date::DATE = ah.date 
WHERE ah.asset_status_original LIKE 'WRITTEN OFF%'       
)
SELECT DISTINCT  
  a.order_id
 ,a.subscription_id 
 ,a.created_date
 ,a.subscription_bo_id
 ,COALESCE(a.subscription_sf_id, a.subscription_id) AS subscription_sf_id
 ,w.finco_market_value_at_cancellation
 ,a.subscription_plan
 ,c.customer_id
 ,sp.outstanding_allocation_ids AS allocation_sf_id
 ,c.first_name::TEXT
 ,c.last_name::TEXT
 ,(c.first_name::TEXT || ' '::CHARACTER VARYING::TEXT) || c.last_name::TEXT AS full_name
 ,c.customer_type
 ,COALESCE(c.company_name,'') AS company_name
 ,CASE
   WHEN c.customer_type = 'normal_customer' 
    THEN ci.identification_number
   WHEN c.customer_type = 'business_customer'
    THEN c.tax_id 
  END AS identification_number
 ,cs.id_check_state
 ,c.email
 ,c.phone_number
 ,c.signup_language
 ,sp.outstanding_product_names AS outstanding_products
 ,sp.outstanding_purchase_price AS outstanding_asset_value
 ,sp.outstanding_assets AS outstanding_assets
 ,CASE 
   WHEN sp.outstanding_assets > 0 
     AND w.asset_status_original ILIKE 'WRITTEN%' 
    THEN 'Written Off' 
   WHEN sp.outstanding_assets > 0 
    THEN 'Outstanding'
   ELSE 'Returned'
  END AS asset_status
 ,sp.outstanding_serial_numbers AS outstanding_serial_numbers
 ,sp.outstanding_rrp AS outstanding_rrp
 ,sp.outstanding_residual_asset_value
 ,mv.finco_market_valuation
 ,a.start_date::DATE AS subscription_start_date
 ,a.cancellation_date AS susbcription_cancellation_date
 ,a.dc_status
 ,sc.outstanding_subscription_revenue
 ,a.payment_method
 ,ps.payment_processor_message
 ,CASE 
   WHEN ps.payment_processor_message ILIKE '%Agreement was canceled%'
    THEN 'Agreement was canceled'
   WHEN ps.payment_processor_message ILIKE '%Blocked Card%'
    THEN 'Blocked Card'
   WHEN ps.payment_processor_message ILIKE '%Instruct the customer to retry the transaction using an alternative payment%'
    THEN 'Instruct the customer to retry the transaction using an alternative payment'
   WHEN ps.payment_processor_message ILIKE '%Invalid Card Number%'
    THEN 'Invalid Card Number'
   WHEN ps.payment_processor_message ILIKE '%Not enough balance%'
    THEN 'Not enough balance'
   WHEN ps.payment_processor_message ILIKE '%Not supported%'
    THEN 'Not supported'
   WHEN ps.payment_processor_message ILIKE '%referenced transaction is already de-registered%'
    THEN 'referenced transaction is already de-registered'
   WHEN ps.payment_processor_message ILIKE '%Refused%'
    THEN 'Refused'
   WHEN ps.payment_processor_message ILIKE '%Users account is closed or restricted%'
    THEN 'Users account is closed or restricted'
   WHEN ps.payment_processor_message ILIKE '%Withdrawal amount exceeded%'
    THEN 'Withdrawal amount exceeded'
   ELSE 'Others'
  END AS failed_reason_filter
 ,sc.payment_history
 ,a.payment_count
 ,a.paid_subscriptions AS paid_subscriptions_excluding_paid_refunds_and_chargebacks
 ,sc.last_valid_payment_category
 ,a.status
 ,a.subscription_value
 ,a.first_asset_delivery_date
 ,sc.last_warning_date
 ,sc.default_date
 ,sc.warning_sent
 ,a.result_debt_collection_contact
 ,a.cancellation_reason_new
 ,COALESCE(ba.billingstreet, CASE
   WHEN c.billing_country IN ('United States', 'United Kingdom') 
    THEN c.house_number || ' ' || c.street
   ELSE c.street || ' ' || c.house_number
  END) AS billing_street
 ,COALESCE(ba.billingcity, c.billing_city) AS billing_city
 ,COALESCE(ba.billingpostalcode, c.billing_zip) AS billing_postalcode
 ,COALESCE(ba.billingcountry, c.billing_country) AS billing_country
 ,sa.shippingstreet AS shipping_street
 ,c.house_number AS shipping_house_number
 ,COALESCE(sa.shippingcity, c.shipping_city) AS shipping_city
 ,COALESCE(sa.shippingpostalcode, c.shipping_zip) AS shipping_postalcode
 ,COALESCE(sa.shippingcountry, c.shipping_country) AS shipping_country
 ,sc.outstanding_sub_revenue_excl_overdue
 ,sc.outstanding_sub_overdue
 ,c.birthdate
 ,cn.last_cancellation_letter_date
 ,CASE
   WHEN mcd.contract_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END AS is_migrated
 ,ps2.payment_paid
 ,fpi.failed_payment_numbers
 ,fpi.failed_payment_amount_due
 ,fpi.failed_payment_amount_paid
 ,fpi.failed_payment_amount_over_due
 ,fpi.failed_payment_invoice_dates
 ,fpi.failed_payment_invoice_numbers
 ,fpi.failed_payment_invoice_url
 ,fpi.failed_payment_billing_dates
 ,o.is_pay_by_invoice
FROM master.subscription a
  LEFT JOIN ods_production.subscription_assets AS sp 
    ON a.subscription_id = sp.subscription_id
  LEFT JOIN ods_data_sensitive.customer_pii AS c 
    ON c.customer_id = a.customer_id
  LEFT JOIN ods_production.customer_scoring AS cs 
    ON a.customer_id = cs.customer_id
  LEFT JOIN ods_production.subscription_cashflow AS sc
    ON sc.subscription_id=a.subscription_id
  LEFT JOIN ods_production.payment_subscription AS ps
    ON a.subscription_id =ps.subscription_id
  LEFT JOIN market_valuation AS mv
    ON mv.subscription_id = a.subscription_id
  LEFT JOIN cancellation_letters AS cn 
    ON cn.subscription_sf_id = a.subscription_sf_id
  LEFT JOIN billing_addresses AS ba
    ON ba.customer_id = a.customer_id
  LEFT JOIN shipping_addresses AS sa
    ON sa.customer_id = a.customer_id
  LEFT JOIN customer_identification AS ci
    ON ci.customer_id = a.customer_id 
    AND ci.rowno = 1
  LEFT JOIN ods_production.ORDER AS o
    ON a.order_id = o.order_id
  LEFT JOIN ods_production.migrated_contracts_dates AS mcd
    ON mcd.contract_id = COALESCE(a.subscription_bo_id, a.subscription_id)
  LEFT JOIN paid_subscriptions_excluding_refunds AS ps2
    ON ps2.subscription_id = a.subscription_id
  LEFT JOIN failed_payment_invoice_details AS fpi
    ON a.subscription_id = fpi.subscription_id
  LEFT JOIN written_off_assets AS w
    ON a.subscription_id = w.subscription_id
WHERE TRUE  
  AND (sp.outstanding_assets > 0 OR sc.outstanding_subscription_revenue > 0::NUMERIC)
  AND sc.default_date = ps.due_date
  AND a.currency <> 'USD'
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_debt_collection.v_list_of_cases_report TO tableau;