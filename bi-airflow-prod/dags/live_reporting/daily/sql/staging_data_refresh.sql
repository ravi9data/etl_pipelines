-- capturing the latest data for orders 
DROP TABLE IF EXISTS order_refresh;
CREATE TEMP TABLE order_refresh AS 
SELECT *  FROM stg_api_production2.spree_orders
WHERE updated_at >=(SELECT MAX(updated_at) FROM stg_api_production.spree_orders);

-- deleting the records from orders final table which exists with different id 
DELETE FROM stg_api_production.spree_orders WHERE "number" IN (
SELECT "number"  FROM order_refresh)
AND id NOT IN (SELECT id  FROM order_refresh);

-- extracting the latest record by making sure id doesn't exist 
DROP TABLE IF EXISTS tmp_spree_orders;
CREATE TEMP TABLE tmp_spree_orders AS
SELECT id,"number",item_total,total,state,adjustment_total,user_id,completed_at,bill_address_id,ship_address_id,payment_total,shipping_method_id,shipment_state,payment_state,email,special_instructions,created_at,updated_at,currency,last_ip_address,created_by_id,shipment_total,additional_tax_total,promo_total,channel,included_tax_total,item_count,approver_id,approved_at,confirmation_delivered,considered_risky,guest_token,canceled_at,canceler_id,store_id,state_lock_version,payone_hash,invoice_number,invoice_date,active,stripe_subscription_id,stripe_plan_id,decision_result,decision_confidence,sf_id,sf_picked,sent_shipedge_at,order_type,current_payment_source_id,current_payment_source_type,coupon,"external",us_sales_tax,ship_started_at,ship_finished_at,burgel_risk_score,burgel_risk_text,burgel_risk_request,burgel_risk_response,requested_via_secret_link,waiting_list_share_id,utm_source,utm_campaign,via_grover_pays,via_store,utm_medium,voucherify_coupon_code,voucherify_tracking_code,voucherify_discount_total,voucherify_coupon_type,voucherify_coupon_value,sf_order_sync_log,step,shipment_tracking_url,declined_at,decline_reason,approval_request_log,sf_sync_code,subscription_limit_exceeded,meta,adyen_first_payment_sf_log,first_payment_request_id,first_payment_result_code,offline_store_unit_code,employee_name,deleted_at,coupon_recurrent,voucherify_response,voucherify_response_at
FROM order_refresh so
WHERE so.id NOT IN (SELECT so2.id FROM stg_api_production.spree_orders so2);

-- refreshing the orders which has same id and number but different status and updated_at
DROP TABLE IF EXISTS order_refresh_same_id;
CREATE temp TABLE order_refresh_same_id AS 
SELECT a.id,a."number",a.item_total,a.total,a.state,a.adjustment_total,a.user_id,a.completed_at,a.bill_address_id,a.ship_address_id,a.payment_total,a.shipping_method_id,a.shipment_state,a.payment_state,a.email,a.special_instructions,a.created_at,a.updated_at,a.currency,a.last_ip_address,a.created_by_id,a.shipment_total,a.additional_tax_total,a.promo_total,a.channel,a.included_tax_total,a.item_count,a.approver_id,a.approved_at,a.confirmation_delivered,a.considered_risky,a.guest_token,a.canceled_at,a.canceler_id,a.store_id,a.state_lock_version,a.payone_hash,a.invoice_number,a.invoice_date,a.active,a.stripe_subscription_id,a.stripe_plan_id,a.decision_result,a.decision_confidence,a.sf_id,a.sf_picked,a.sent_shipedge_at,a.order_type,a.current_payment_source_id,a.current_payment_source_type,a.coupon,a."external",a.us_sales_tax,a.ship_started_at,a.ship_finished_at,a.burgel_risk_score,a.burgel_risk_text,a.burgel_risk_request,a.burgel_risk_response,a.requested_via_secret_link,a.waiting_list_share_id,a.utm_source,a.utm_campaign,a.via_grover_pays,a.via_store,a.utm_medium,a.voucherify_coupon_code,a.voucherify_tracking_code,a.voucherify_discount_total,a.voucherify_coupon_type,a.voucherify_coupon_value,a.sf_order_sync_log,a.step,a.shipment_tracking_url,a.declined_at,a.decline_reason,a.approval_request_log,a.sf_sync_code,a.subscription_limit_exceeded,a.meta,a.adyen_first_payment_sf_log,a.first_payment_request_id,a.first_payment_result_code,a.offline_store_unit_code,a.employee_name,a.deleted_at,a.coupon_recurrent,a.voucherify_response,a.voucherify_response_at 
FROM order_refresh a LEFT JOIN 
stg_api_production.spree_orders b
ON a.id = b.id 
AND a.updated_at > b.updated_at ;

-- merging new and old orders together for refreshing 
DROP TABLE IF EXISTS tmp_spree_orders_final_pre ;
CREATE TABLE tmp_spree_orders_final_pre AS
SELECT * FROM tmp_spree_orders
UNION 
SELECT * FROM order_refresh_same_id;

-- extracting the latest orders against each order number by using updateed_at
DROP TABLE IF EXISTS tmp_spree_orders_final ;
CREATE TABLE tmp_spree_orders_final AS
WITH unique_orders AS (SELECT *, ROW_NUMBER() OVER (
PARTITION BY "number" 
ORDER BY updated_at DESC) AS rr 
FROM tmp_spree_orders_final_pre)
SELECT * FROM unique_orders WHERE rr=1;

ALTER TABLE tmp_spree_orders_final DROP column rr; 

-- deleting the already exists records from orders data
DELETE FROM stg_api_production.spree_orders
USING tmp_spree_orders_final WHERE spree_orders.id = tmp_spree_orders_final.id;

-- appending latest records for orders data
INSERT INTO stg_api_production.spree_orders
SELECT *
FROM tmp_spree_orders_final;

-- making sure nothing is missing

DROP TABLE IF EXISTS missing_order;
CREATE temp TABLE missing_order AS 
SELECT "number" 
FROM stg_api_production2.spree_orders 
EXCEPT 
SELECT "number" 
FROM stg_api_production.spree_orders;

INSERT INTO stg_api_production.spree_orders 
SELECT 
id,"number",item_total,total,state,adjustment_total,user_id,completed_at,bill_address_id,ship_address_id,payment_total,shipping_method_id,shipment_state,payment_state,email,special_instructions,created_at,updated_at,currency,last_ip_address,created_by_id,shipment_total,additional_tax_total,promo_total,channel,included_tax_total,item_count,approver_id,approved_at,confirmation_delivered,considered_risky,guest_token,canceled_at,canceler_id,store_id,state_lock_version,payone_hash,invoice_number,invoice_date,active,stripe_subscription_id,stripe_plan_id,decision_result,decision_confidence,sf_id,sf_picked,sent_shipedge_at,order_type,current_payment_source_id,current_payment_source_type,coupon,"external",us_sales_tax,ship_started_at,ship_finished_at,burgel_risk_score,burgel_risk_text,burgel_risk_request,burgel_risk_response,requested_via_secret_link,waiting_list_share_id,utm_source,utm_campaign,via_grover_pays,via_store,utm_medium,voucherify_coupon_code,voucherify_tracking_code,voucherify_discount_total,voucherify_coupon_type,voucherify_coupon_value,sf_order_sync_log,step,shipment_tracking_url,declined_at,decline_reason,approval_request_log,sf_sync_code,subscription_limit_exceeded,meta,adyen_first_payment_sf_log,first_payment_request_id,first_payment_result_code,
offline_store_unit_code,employee_name,deleted_at,coupon_recurrent,voucherify_response,voucherify_response_at 
FROM stg_api_production2.spree_orders
WHERE  DATE(created_at)=current_date 
AND "number" IN (SELECT "number" FROM missing_order);

-- =================================spree line items process====================================================
DROP TABLE IF EXISTS tmp_spree_line_items;
CREATE TEMP TABLE tmp_spree_line_items AS
SELECT id,variant_id,order_id,quantity,price,created_at,updated_at,currency,cost_price,tax_category_id,adjustment_total,additional_tax_total,promo_total,included_tax_total,pre_tax_amount,cancel_requested_at,cancel_approved_at,specs,purchased_at,purchaser_id,line_item_id,resubscribed_at,asset_allocation_id,redeemed_at,stock_type,total,serial_numbers,asset_prices,any_variant,rental_plan_id,condition_name,rental_plan_price,minimum_term_months,first_month_discount_amount,trial_days,campaign_id,market_price,buyout
FROM stg_api_production2.spree_line_items so
WHERE so.updated_at >= (SELECT MAX(updated_at) FROM stg_api_production.spree_line_items so2)
OR so.id NOT IN (SELECT so2.id FROM stg_api_production.spree_line_items so2);


DELETE FROM stg_api_production.spree_line_items
USING tmp_spree_line_items WHERE spree_line_items.id = tmp_spree_line_items.id;

INSERT INTO stg_api_production.spree_line_items
SELECT *
FROM tmp_spree_line_items;
