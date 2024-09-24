/*Objective - To monitor the subscriptions with replacements if the initial replaced allocation is returned*/

   drop table if exists monitoring.bo4;
   create table monitoring.bo4 as 
	with a as (
		select * from ods_production.subscription_payment_label
		where payment_label = 'Replacement Subscription - 1+ Outstanding'
		)
		, b as (
		select *
		from ods_production.allocation where replacement_date is not null
		)
		select 
			a.subscription_id, 
			a.subscription_name,
			a.customer_id,
			a.start_date,
			cp.email,
			b.allocation_status_original,
			b.delivered_at,
			b.asset_id,
			b.serial_number,
			b.replacement_date,
			b.replacement_reason,
			a.days_on_rent,
			b.replaced_by,
			allo.delivered_at as replacement_asset_delivery_date,
			b.return_shipment_at,
			concat('https://track.shipcloud.io/',b.return_shipment_id) as return_tracking_url,
			b.return_delivery_date,
			b.refurbishment_start_at
			from a 
			left join b on a.subscription_id = b.subscription_id 
			left join ods_data_sensitive.customer_pii cp on cp.customer_id = a.customer_id
			left join ods_production.allocation allo on allo.allocation_id = b.replaced_by;

GRANT SELECT ON monitoring.bo4 TO tableau;
