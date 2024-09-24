drop table if exists dm_operations.lost_assets_and_compensation;
create table dm_operations.lost_assets_and_compensation as

with 
pre_asset_history as 
	(select 
		assetid as asset_id, 
		createddate , 
		field , 
		oldvalue , 
		newvalue , 
		case when field = 'Lost_Reason__c' then row_number() over (partition by assetid , field order by createddate desc)
		     when oldvalue = 'LOST' then row_number() over (partition by assetid , oldvalue order by createddate desc)
		     when newvalue = 'LOST' then row_number() over (partition by assetid , newvalue order by createddate desc) end as rn
	    from stg_salesforce.asset_history
		where (newvalue = 'LOST' or oldvalue = 'LOST' or field = 'Lost_Reason__c')),
---------
---------
pvt_asset_history as (
	select 
		asset_id,
		max(case when field = 'Lost_Reason__c' then newvalue end) as lost_reason,
		coalesce(
			max(case when newvalue = 'LOST' then createddate end), 
			max(case when field = 'Lost_Reason__c' then createddate end)) as lost_date, 
		max(case when oldvalue = 'LOST' then createddate end) as found_date
	from pre_asset_history
	where rn = 1
	group by 1
),
---------exclude manually booked asset, 
---------in order to create asset manually, you have to mark item as lost at the beginning
asset_history as (
	select asset_id, lost_reason, lost_date, found_date
	from pvt_asset_history
	where lost_date is not null
),
---------
---------
assets as (
	select
		ma.asset_id ,
		ma.serial_number ,
		ma.asset_status_original,
  		ma.asset_name,
  		ma.variant_sku,
		ma.category_name ,
		ma.subcategory_name ,
		ma.initial_price ,
		coalesce (ma.lost_reason , ah.lost_reason) as lost_reason,
		coalesce (ah.lost_date, ma.lost_date, ma.created_at ) as lost_date,
		ah.found_date
	from master.asset ma
	left join asset_history ah on ah.asset_id = ma.asset_id 
	where ma.asset_status_original = 'LOST'
		or ma.asset_id in (select asset_id from asset_history where found_date is not null)
	),
---------
---------	
residual_value_market_price as (
	select asset_id , 
 		   residual_value_market_price , 
 		   max(date) over (partition by asset_id) as max_date,
	 	   date 
	  from master.asset_historical 
	 where asset_id in (select asset_id from assets)
	   and residual_value_market_price > 0 ),
-------
-------
last_residual_value as (
	select asset_id ,
		   residual_value_market_price
	  from residual_value_market_price where max_date = date ),
-------
-------
compensation as (
	select asset_id , 
	       min(paid_date) as refund_date ,
		   count(1) as number_of_payments, 
		   sum(amount_paid) as refund_amount 
	  from master.asset_payment 
	 where payment_type ='COMPENSATION'
	   and asset_id in (select asset_id from assets)
	 group by 1 )
---------
---------
, allocation as (
	select 
		allocation_id ,
		shipping_country ,
		customer_type ,
		carrier ,
		shipment_service,
		return_carrier ,
		asset_id , 
		shipment_at ,
		failed_delivery_at ,
		failed_delivery_candidate ,
		delivered_at ,
		return_shipment_label_created_at ,
		return_shipment_at ,
		row_number () over (partition by asset_id order by allocated_at desc) idx
	from ods_operations.allocation_shipment
)
, last_allocation as (
	select 
		allocation_id ,
		shipping_country,
		customer_type,
		carrier ,
		shipment_service,
		return_carrier,
		asset_id , 
		shipment_at ,
		failed_delivery_at ,
		failed_delivery_candidate ,
		delivered_at ,
		return_shipment_label_created_at ,
		return_shipment_at
	from allocation 
	where idx = 1 
)	
select 
	a.asset_id,
	a.serial_number,
    a.asset_name,
    a.variant_sku,
	a.category_name,
	a.subcategory_name,
	a.asset_status_original,
	a.lost_date,
	a.lost_reason,
	case 
	  when a.lost_reason in ('DHL', 'UPS', 'Hermes') 
	  	   and a.found_date is null	
	  then
		case 
		  when coalesce(al.return_shipment_label_created_at, 
					    al.return_shipment_at) is not null
		 	  --or al.ib_shipment_unique is not null 
		 	then 'Return-With Label'
		  when al.delivered_at is not null 
		 	then 'Not Arrived Claim' 
		  when coalesce(al.failed_delivery_at, al.failed_delivery_candidate) is not null 
		 	then 'Failed Delivery'
		 	else 'Outbound' end
	  else a.lost_reason end as lost_reason_bucket,
	case when a.lost_reason <> lost_reason_bucket 
		   then al.shipping_country end as country_name,
	--no service provided in returns
	case when lost_reason_bucket = 'Return-With Label'	
		   then al.return_carrier
		 when a.lost_reason <> lost_reason_bucket		
		   then al.shipment_service
		 end as shipment_service,
	case when lost_reason_bucket = 'Return-With Label'	
		   then al.return_carrier
		 when a.lost_reason <> lost_reason_bucket		
		   then al.carrier
		 end as carrier,
	a.found_date,
	a.initial_price,
	rv.residual_value_market_price,
	co.refund_amount,
	co.refund_date,
	co.number_of_payments,
	al.customer_type ,
	d.longest_dim 
from assets a
left join last_residual_value rv 		on rv.asset_id = a.asset_id
left join compensation co 				on co.asset_id = a.asset_id
left join last_allocation al 			on al.asset_id = a.asset_id
left join dm_operations.dimensions d	on a.variant_sku = d.variant_sku 
;

GRANT SELECT ON dm_operations.lost_assets_and_compensation TO tableau;
