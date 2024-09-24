BEGIN;

TRUNCATE TABLE master.allocation;

INSERT INTO master.allocation
with revocation as(
select 
 a.allocation_id,
 a.delivered_at,
 s.cancellation_date as revocation_date,
 s.updated_at
from ods_production.allocation a 
left join ods_production.subscription_cancellation_reason s 
 on a.subscription_id = s.subscription_id
where s.cancellation_reason_new = 'REVOCATION')
select distinct
 aa.allocation_id,
 aa.asset_id,
 aa.allocation_sf_id,
 aa.subscription_id,
 oa.customer_type,
 st.store_type,
 st.store_short,
 s.subcategory_name,
 s.product_sku,
 aa.allocation_status_original,
 aa.is_manual_allocation,
 aa.is_recirculated,
 aa.is_last_allocation_per_asset,
 aa.rank_allocations_per_subscription,
 aa.order_approved_at,
 aa.order_completed_at,
 s.created_date as subscription_created_at,
 aa.allocated_at,
 aa.wh_goods_order__c AS push_to_wh_at,
 aa.ready_to_ship_at,
 aa.shipment_label_created_at,
 oa.carrier as shipment_provider,
 aa.picked_by_carrier_at,
 aa.shipment_at,
 aa.failed_delivery_at,
 oa.failed_reason,
 aa.is_package_lost,
 aa.delivered_at,
 aa.return_shipment_label_created_at,
 aa.return_shipment_at,
 aa.return_delivery_date,
 aa.cancellation_returned_at,
 aa.refurbishment_start_at,
 aa.refurbishment_end_at,
 revocation_date,
 aa.created_at,
 max(greatest(aa.updated_at,s.updated_date,st.updated_date,r.updated_at)) as updated_at,
 aa.shipment_tracking_number,
 aa.return_shipment_tracking_number,
 oa.receiver_country as shipping_country, 
 oa.receiver_city as city, 
 oa.receiver_state_name as state_name,
 aa.replacement_date,
 aa.replaced_by,
 aa.replacement_for,
 aa.replacement_reason,
 aa.returned_final_condition
from ods_production.allocation aa 
left join ods_production.subscription s on aa.subscription_id=s.subscription_id
left join ods_production.store st on st.id=s.store_id 
left join revocation r on r.allocation_id=aa.allocation_id
left join ods_operations.allocation_shipment oa on aa.allocation_id = oa.allocation_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,38,39,40,41,42,43,44,45,46,47
;

COMMIT;
