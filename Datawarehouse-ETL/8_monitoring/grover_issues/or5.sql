drop table if exists monitoring.or5;
create table monitoring.or5 as 
with rd as (
	select 
		a.allocation_id ,
		a.allocation_status_original,
		coalesce (a.return_delivery_date , a.return_delivery_date_old ) as return_date,
		coalesce(a.return_warehouse, 'Unknown') as return_warehouse,
		a.asset_id ,
		case when
		(select count(1) from ods_production.allocation alt 
			where alt.asset_id = a.asset_id  
			and alt.allocated_at > a.allocated_at::timestamp
            and a.allocation_id <> alt.allocation_id) = 0 then true else false end is_last_allocation_per_asset, --exclude itself
		t.asset_status_original ,
        a.subscription_id,
		a.serial_number ,
        a.customer_type ,
		a.return_shipment_uid ,
	    a.return_tracking_number ,
		a.return_tracking_id ,
		a.refurbishment_start_at ,
		case when (select count(1)
		 		   from ods_operations.ingram_inventory i 
				   where i.serial_number = a.serial_number 
			    	 	 and reporting_date between dateadd('day', -1, return_date) 
					 	     						and dateadd('day',  7, return_date) ) > 0 
			  then true else false end as has_reported_by_ingram_micro,
		false as has_reported_by_synerlogis,
		-- check if there is another allocation with customer for that subscription 
		-- for example, replacements... customer may return initial asset since it is wrong asset 
		-- then we send correct asset with same subs.id , so we should not cancel the sub.
		(select count(1) from ods_production.allocation aso
		 where s.subscription_id = aso.subscription_id 
		   and a.allocation_id <> aso.allocation_id 
		   and aso.allocation_status_original not in ('RETURNED', 'CANCELLED', 'FAILED DELIVERY', 'SOLD')) as oth_allocations,
		s.status as subs_status,
		s.minimum_cancellation_date,
		case when coalesce(coalesce (a.return_delivery_date , a.return_delivery_date_old), a.refurbishment_start_at) < s.minimum_cancellation_date 
			 then true else false end as is_return_before_min_cancel_date,
		a.infra 
	from ods_operations.allocation_shipment a 
	left join ods_production.asset t 
		on a.asset_id = t.asset_id 
	left join ods_production.subscription s 
		on a.subscription_id = s.subscription_id
	where 
	 	--exclude recent returns and refurbishments
		coalesce (a.return_delivery_date , a.return_delivery_date_old ) < dateadd('day', -1, current_date)
		and (a.refurbishment_start_at is null or a.refurbishment_start_at < dateadd('day', -1, current_date))
),
payments as (
	select 
		subscription_id ,
		due_date,
		subscription_payment_category,
		row_number () over (partition by subscription_id order by payment_number::int desc) idx
	from master.subscription_payment  
	where subscription_id in (select distinct subscription_id from rd)
),
payments_last as (
	select 
		subscription_id ,
		due_date ,
		subscription_payment_category 
	from payments 
	where idx = 1
),
---
evaluate as (
	select 
		rd.* , 
		pl.due_date as payment_due_date,
		pl.subscription_payment_category,
		case when (has_reported_by_ingram_micro and return_warehouse = 'Ingram Micro')
				  or
				  (has_reported_by_synerlogis and return_warehouse = 'Kiel')
				  or 
				  (return_warehouse is null and (has_reported_by_synerlogis or has_reported_by_ingram_micro))
			 then true else false end as has_reported,
		case when allocation_status_original in ('RETURNED', 'CANCELLED', 'FAILED DELIVERY', 'SOLD') then true else false end as is_allo_status_correct,
		case when (refurbishment_start_at is not null and infra = 'Legacy')
				  or
				  (refurbishment_start_at is null and infra = 'New Infra')
				  then true else false end as refurb_date_exists,
		case when asset_status_original not in ('ON LOAN', 'RESERVED')
				  or not is_last_allocation_per_asset
				  then true else false end as is_asset_status_correct ,
	    case when subs_status = 'CANCELLED'
	    		  or
	    		  -- no cancellation the subsc. before min.cancellation date
	    		  (subs_status = 'ACTIVE' and minimum_cancellation_date > current_date)
	    		  or 
	    		  -- no cancellation if subsc. has other active allocations
	    		  (subs_status = 'ACTIVE' and oth_allocations > 0)
	    		  then true else false end has_subs_cancelled
	from rd 
	left join payments_last pl 
	on rd.subscription_id = pl.subscription_id)
select 
	case -- everything as it should be
		 when is_allo_status_correct and has_subs_cancelled and refurb_date_exists and is_asset_status_correct 
		 then 'ALL GOOD'
		 -- UC5 : allocation and asset status are correct, subscription should be cancelled
		 when is_allo_status_correct and is_asset_status_correct and not has_subs_cancelled
		 then 'UC5'
		 -- UC6 : allocation and subscription status are correct, asset status should be corrected
		 when is_allo_status_correct and not is_asset_status_correct and has_subs_cancelled
		 then 'UC6'
		 when not is_allo_status_correct and is_asset_status_correct and has_subs_cancelled 
		 then 
		 -- UC7 : only allocation status is not correct
		 	case when infra = 'Legacy' --a fix needed on allocation table for new infra
		 		 then 'UC7'
		 		 else 'ALL GOOD' end 
		 -- UC1 : asset returned, but wh did not start refurbishment
		 when not has_reported and not refurb_date_exists 
		 then 'UC1'
		 when has_reported and not is_allo_status_correct and not is_asset_status_correct and not has_subs_cancelled 
		 then 
		 	case when refurb_date_exists
		 -- UC3 : asset returned, reported in WH reports, and refurbished, but all status are wrong
		 		 then 'UC3' 
		 -- UC2 : asset returned, reported in WH reports, but refubishment not started
		 		 else 'UC2' 
		 		 end 
		 -- UC4 : asset returned, reported in WH reports, and refurbished, but at least 2 status are wrong
		 else 'UC4'
	end as use_case,
	allocation_id ,
	allocation_status_original ,
	asset_id ,
	asset_status_original ,
	is_last_allocation_per_asset,
    subscription_id ,
	serial_number ,
	return_date ,
	return_warehouse ,
	return_shipment_uid,
    return_tracking_number,
	return_tracking_id ,
	refurbishment_start_at ,
	has_reported_by_ingram_micro ,
	has_reported_by_synerlogis ,
	subs_status ,
	minimum_cancellation_date ,
	is_return_before_min_cancel_date,
	payment_due_date,
	subscription_payment_category,
	infra,
    customer_type,
	has_reported,
	has_subs_cancelled,
	is_asset_status_correct,
	is_allo_status_correct
from evaluate 
where use_case <> 'ALL GOOD' 
order by use_case asc, return_date desc ;

GRANT SELECT ON monitoring.or5 TO tableau;
