drop table if exists ods_production.debt_collection;
create table ods_production.debt_collection as
with asset_details as (
	select s.subscription_id,
	s.customer_id,
	 CASE 
		           when coeo_claim_id__c is not null or coeo_claim_date__c is not null THEN 'COEO'
		           when agency_for_dc_processing__c is not null or dc_agency_case_id__c is not null THEN 'Pair Finance'
		           else 'Not Available'
			       END as agency_dca,
    sa.outstanding_product_names,
	sa.allocated_assets,
	sa.delivered_assets,
	sa.debt_collection_assets,
	sa.returned_packages,
	sa.outstanding_assets,
	listagg(allocation_sf_id, ' | ' ) as allocation_sf_ids,
	listagg(asset_id, ' | ' ) as asset_ids,
	listagg(serial_number, ' | ' ) as serial_numbers,
	max(return_delivery_date) as max_return_date
	from ods_production.subscription s 
	left join ods_production.subscription_cancellation_reason cr on cr.subscription_id = s.subscription_id
	left join ods_production.allocation a on a.subscription_id = cr.subscription_id
	left join ods_production.subscription_assets sa on sa.subscription_id = cr.subscription_id
	where cr.cancellation_reason_new = 'DEBT COLLECTION'
group by 1,2,3,4,5,6,7,8,9)
 ,asset_payment as (
	 select aa.subscription__c as subscription_id,
	 count(*) as dc_payments,
	 count(case when pa.status__c = 'CANCELLED' then pa.id end) as cancelled_dc_payments,
	 sum(pa.amount_f_due__c) as dc_amount_due,
	 sum(case when pa.status__c = 'CANCELLED' then pa.amount_f_due__c end) as recovered_value,
	 sum(COALESCE(pa.amount_paid__c,0)-coalesce(pa.amount_refunded__c,0)) as net_dc_amount_paid
	 from stg_salesforce.asset_payment__c pa
	 left join stg_salesforce.customer_asset_allocation__c aa on pa.allocation__c=aa.id 
	 where pa.type__c = 'DEBT COLLECTION'
	  group by 1 ) 
 ,sub_payment as (
	  select ps.subscription_id,
	  count(ps.*) as sub_payments,
	  count(CASE when (sd.subscription_payment_category like '%PAID%') then ps.subscription_id end) as paid_sub_payments,
	  count(CASE when (sd.subscription_payment_category like '%RECOVERY%') then ps.subscription_id end) as recovered_sub_payments,
	  sum(CASE when (sd.subscription_payment_category like '%RECOVERY%') then ps.amount_paid else 0 end) as recovered_sub_value,
	  sum(ps.amount_due) as sub_amount_due,
	  sum(COALESCE(ps.amount_paid,0)-coalesce(ps.refund_amount,0)-coalesce(ps.chargeback_amount,0)) as net_sub_revenue_paid,
	  avg(DPD)as avg_dpd
	  from ods_production.payment_subscription ps 
	  left join ods_production.payment_subscription_details sd on sd.subscription_payment_id = ps.subscription_payment_id
	  group by 1)
 Select 
	 	ad.*,
	 	s.order_id,
	 	s.debt_collection_handover_date,
		s.cancellation_date,
	 	s.product_name,
	 	coalesce(ap.dc_payments,0) as dc_payments,
	 	coalesce(ap.cancelled_dc_payments,0) as cancelled_dc_payments,
 		ap.dc_amount_due,
 		coalesce(ap.recovered_value,0) as recovered_value,
 		ap.net_dc_amount_paid,
 		sp.sub_payments,
		sp.paid_sub_payments, 
		sp.recovered_sub_payments,
		sp.recovered_sub_value,
 		sp.sub_amount_due,
 		sp.net_sub_revenue_paid,
 		sp.avg_dpd,
 		cp.first_name,
 		cp.last_name,
 		cp.company_name
 from  asset_details ad 
 left join ods_production.subscription s on ad.subscription_id = s.subscription_id
 left join asset_payment ap on ap.subscription_id = ad.subscription_id
 left join sub_payment sp on sp.subscription_id = ad.subscription_id
 left join ods_data_sensitive.customer_pii cp on cp.customer_id = ad.customer_id;

 GRANT SELECT ON ods_production.debt_collection TO tableau;
 