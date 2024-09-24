-- identifying the records where prod table has old data as compared to the staging table
DROP TABLE IF EXISTS asset_update; 
CREATE TEMP TABLE asset_update AS 
SELECT a.id
FROM
stg_salesforce.asset a
INNER JOIN staging.asset b 
ON a.id = b.id
WHERE TRUE
	AND date(a.lastmodifieddate)<current_date -  1
	AND b.lastmodifieddate > a.lastmodifieddate
  AND a.status <> b.status 
union
SELECT distinct a.id
FROM
 staging.asset a
left join stg_salesforce.asset b
ON a.id = b.id
where b.id is null;
   
-- deleting the records which requires the refresh
DELETE FROM stg_salesforce.asset
USING asset_update
WHERE stg_salesforce.asset.id = asset_update.id;


-- inserting the latest records in to the final table

INSERT INTO stg_salesforce.asset
SELECT distinct id, contactid, accountid, parentid, rootassetid, product2id, iscompetitorproduct, createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, systemmodstamp, isdeleted, "name", serialnumber, 
installdate, purchasedate, usageenddate, status, price, quantity, description, ownerid, lastvieweddate, lastreferenceddate, sku__c, asset_id__c, invoice_number__c, proposed_to_customer__c, condition__c, 
order_number__c, sell_price__c, shop__c, cost_price__c, coeo_claim_closed_date__c, supplier__c, asset_allocation__c, test__c, purchased__c, height__c, length__c, width__c, weight__c, shipedge__c, 
purchased_request__c, in_repair_details__c, last_usage_end_date__c, customer_request__c, invoice_date__c, invoice_url__c, brand__c, device_type__c, amount_rrp__c, incomplete_details__c, 
locked_details__c, comtech_article_number__c, comtech_price__c, sold__c, sell_price_with_fees__c, note__c, days__c, days_in_stock__c, offered_customer_name__c, leasing__c, leasing_start__c, 
leasing_amount__c, leasing_term__c, warehouse__c, availability_status__c, procurement_status__c, asset_quality__c, device_type_desc__c, warehouse_shipping_id__c, warehouse_target__c, 
repair_amount__c, repair_registered_date__c, actual_asset_value__c, lost_reason__c, condition_note__c, sum_of_rents__c, potential_sum_of_rents__c, potential_asset_value__c, number_of_rents__c,
currency_new__c, selling_order_number__c, components_cost__c, capital_source__c, sync_payments_debt__c, sync_payments_expected__c, sync_payments_profit__c, sync_payments_refunded__c, sync_payments_tax__c, 
sync_payments_paid__c, revenue_share__c, theoretical_purchase_price__c, dc_claim_closed_date__c, spree_product_variant_id__c, coeo_claim_last_update_date__c, final_condition__c, cross_sale_attempt_date__c, 
purchase_request__c, wh_goods_receipt__c, product_ean__c, upc__c, currency__c, cost_price_calculated__c, exchange_rate__c, currency_capital_source__c, cost_price_capital_source__c, sync_capital_source_payments_debt__c, 
sync_capital_source_payments_expected__c, sync_capital_source_payments_paid__c, sync_capital_source_payments_profit__c, sync_capital_source_payments_refunded__c, to_update__c, purchase_payment_method__c, 
payment_due_date__c, invoice_total__c, debt_collection_not_recoverable__c, rrp_diff__c, sell_voucher__c, wh_goods_receipt_id__c, number__c, eta_shipping_note__c, eta__c, eta_asap__c, wh_goods_receipt_tries__c,
supplier_name__c, wh_goods_receipt_delivery_number__c, external_condition__c, functional_condition__c, package_condition__c, f_product_sku_product__c, f_product_sku_variant__c, 
wh_goods_receipt_transfer_type__c, purchase_dropship__c, purchase_shipment_eta__c, purchase_shipment_type__c, warehouse_refurbishment__c, external_condition_note__c,  functional_condition_note__c, 
package_condition_note__c, coeo_claim_date__c, coeo_claim_error__c, coeo_claim_id__c, coeo_claim_response__c, coeo_claim_status__c, purchase_request_item__c, dc_claim_id__c, in_stock_date__c, days_in_warehouse__c, 
manual_allocation__c, capital_source_name__c, sap_database_name__c, category__c, date_of_sale__c, sale_amount__c, selling_agent__c, stock_id__c, 
do_not_touch_belongs_to_mix__c, lost_reason_note__c
FROM staging.asset WHERE id IN (SELECT id FROM asset_update ) ;

-- -------------------customer_asset_allocation__c ---------------------------------------------

DROP TABLE IF EXISTS cus_asset_update; 
CREATE TEMP TABLE cus_asset_update AS 
SELECT a.id
FROM
stg_salesforce.customer_asset_allocation__c a
INNER JOIN staging.customer_asset_allocation__c b 
ON a.id = b.id
WHERE TRUE
	AND date(a.lastmodifieddate)<current_date -  1
	AND b.lastmodifieddate > a.lastmodifieddate
    AND a.status__c  <> b.status__c  ;

   
DELETE FROM stg_salesforce.customer_asset_allocation__c
USING cus_asset_update
WHERE stg_salesforce.customer_asset_allocation__c.id = cus_asset_update.id;


INSERT INTO stg_salesforce.customer_asset_allocation__c
SELECT id, isdeleted, "name", createddate, createdbyid, lastmodifieddate,
lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate,
customer__c, asset__c, order__c, shipment_date__c, usage_end_date__c, is_cross_sale__c,
to_purchase__c, allocated__c, amount__c, debt_collection_requested__c, 
shipment_tracking_number__c, shipping_provider__c, cancelltion_requested__c, 
return_tracking_number__c, asset_name__c, cancelltion_approved__c, weight__c, 
length__c, width__c, height__c, shipping_label_created__c, picked_by_carrier__c,
shipedge__c, shipcloud_shipment_id__c, tracking_url__c, label_url__c, shipment_price__c, 
delivered__c, shipping_service_type__c, return_label_created__c, 
return_shipment_provider__c, shipcloud_return_shipment_id__c, return_shipment_price__c,
return_picked_by_carrier__c, return_delivered__c, return_tracking_url__c, 
return_package_height__c, return_package_length__c, return_package_weight__c, 
return_package_width__c, return_label_url__c, return_label_error__c, shipment_error__c, 
order_product__c, package_lost__c, asset_length__c, asset_width__c, asset_height__c,
asset_weight__c, time_to_ship__c, time_to_deliver__c, insurance_contract_number__c, 
insurance_pdf_token__c, insurance_rental_period__c, insurance_cancelled__c,
insurance_error__c, automatically_allocated__c, ready_to_ship__c, 
shipedge_shipment_tracking_number__c, shipedge_shipment_tracking_url__c,
shipedge_reple_id__c, asset_status__c, renewal__c, renewal_sent__c, return_package_lost__c,
cancelltion_in_transit__c, asset_cost_price__c, spree_order_line_id__c, 
sell_notification_final_re__c, renewal_resent__c, device_type__c, device_type_desc__c, 
months_rented__c, wh_goods_order_id__c, currency__c, insurance_cancelled_check__c, 
debt_collection_handover__c, insurance_start__c, subscription_cancellation__c, 
subscription_cancellation_reason__c, debt_collection_not_recoverable__c, 
sell_notification_1__c, sell_notification_2__c, sell_notification_3__c, 
sell_notification_final__c, sell_notification_next_payment__c, sell_status__c, 
amount_rrp__c, customer_email__c, subscription__c, wh_goods_order_tries__c, 
asset_serial_number__c, initial_condition__c, initial_external_condition__c, 
initial_final_condition__c, initial_functional_condition__c, initial_package_condition__c, 
returned_condition__c, returned_external_condition__c, returned_final_condition__c, 
returned_functional_condition__c, returned_package_condition__c, wh_goods_order__c,
wh_return_announce__c, wh_return_announce_id__c, wh_return_announce_tries__c, 
asset_sku_variant__c, asset_purchase_shipment_type__c, asset_not_recoverable__c, 
cancelltion_returned__c, initial_external_condition_note__c, initial_functional_condition_note__c,
initial_package_condition_note__c, returned_external_condition_note__c, 
returned_functional_condition_note__c, returned_package_condition_note__c, status__c,
subscription_status__c, subscription_allocation_status__c, returned_condition_reason__c, 
returned_condition_reported__c, replaced_by__c, replacement_for__c, replacement_date__c, 
replacement_reason__c, minimum_cancellation_date__c, current_allocation_asset__c, can_create_insurance__c,
accessories_cost__c, refurbishment_charge_customer__c, repair_cost__c, unlocking_cost__c, 
shipcloud_profile__c, in_repair_details__c, incomplete_details__c, 
locked_details__c, sent_for_unlocking_date__c, failed_delivery__c, widerruf_claim_date__c, 
reported_issue_reason__c, issue_report_date__c,
wh_feedback__c, issue_report_comments__c, widerruf_validity_date__c, shipping_profile__c
FROM staging.customer_asset_allocation__c WHERE id IN (SELECT id FROM cus_asset_update);

