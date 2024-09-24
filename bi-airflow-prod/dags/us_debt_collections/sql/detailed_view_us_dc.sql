DROP TABLE IF EXISTS ods_production.detailed_view_us_dc;
CREATE TABLE ods_production.detailed_view_us_dc AS
WITH list_of_cases AS (
SELECT DISTINCT
 subscription_id
FROM ods_production.detailed_view_us_dc_tmp
)
,max_payment_date AS (
SELECT
 sp.subscription_id,
 MAX(sp.paid_date) last_payment_date
FROM master.subscription_payment sp
  INNER JOIN list_of_cases lt
    ON sp.subscription_id = lt.subscription_id
WHERE sp.status = 'PAID'
GROUP BY 1
)
SELECT
 t.order_id,
 t.subscription_id,
 t.customer_id,
 t.due_date,
 t.due_date_new,
 t.dpd,
 t.dpd_new,
 t.subscription_value,
 t.tax_amount,
 t.outstanding_subscription_revenue,
 t.remaining_purchase_price_excl_sales_tax,
 t.remaining_purchase_price_incl_sales_tax,
 t.first_name,
 t.last_name,
 t.email,
 t.phone_number,
 t.billing_city,
 t.billing_zip,
 t.billing_street,
 t.billing_state,
 t.shipping_city,
 t.shipping_zip,
 t.shipping_street,
 t.shipping_state,
 t.order_status,
 t.contract_status,
 t.subscription_plan,
 t.outstanding_products,
 t.serial_number,
 t.total_nr_payments,
 t.paid_payments,
 t.customer_type,
 t.subscription_start_date,
 t.payment_method,
 t."asset_value_linear_depr (3% discount)",
 t.person_business,
 t.last_failed_reason,
 t.last_failed_date,
 c.birthdate ,
 p.last_payment_date,
 sb.first_asset_delivery_date ,
 DATE_TRUNC('DAY', sb.next_due_date) AS day_default_date,
 sb.outstanding_rrp ,
 sb.outstanding_asset_value ,
 sb.outstanding_residual_asset_value,
 t.total_paid_amount_excl_sales_tax,
 t.total_paid_amount_incl_sales_tax,
 t.start_purchase_price_excl_sales_tax,
 t.start_purchase_price_incl_sales_tax
FROM ods_production.detailed_view_us_dc_tmp  t
  LEFT JOIN master.subscription sb
    ON t.subscription_id = sb.subscription_id
  LEFT JOIN ods_data_sensitive.customer_pii c
    ON t.customer_id = c.customer_id
  LEFT JOIN max_payment_date p
    ON t.subscription_id = p.subscription_id
WHERE TRUE
  AND t.show_dc = 1
  AND ((COALESCE(t.dpd_new,1) > 0)
        AND t.contract_status NOT IN ('ended','cancelled','completion_completed','purchase_completed')
        AND NOT (t.contract_status in ('completion_started')
        AND t.order_status = 'CANCELLED')
      )
  AND t.outstanding_subscription_revenue > 0
ORDER BY t.dpd_new DESC
;