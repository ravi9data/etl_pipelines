DROP TABLE IF EXISTS dwh.shipment_reporting;
CREATE TABLE dwh.shipment_reporting as

with allocations as (
select 
	a.allocation_id,
	case when a.store_short = 'Grover International' then s.country_name 
		 --when a.store_short = 'Partners Online' then a.store_short || '-' || s.country_name 
		 else a.store_short end as store_short ,
	case when s.country_name = 'United States' then 'US' else 'EU' end as store_region,
	a.customer_type,
	is_recirculated,
	allocated_at,
	ready_to_ship_at,
	shipment_label_created_at,
	picked_by_carrier_at,
	sm.failed_delivery_at as failed_delivery_at,
	revocation_date,
	delivered_at,
	first_delivery_attempt,
	return_shipment_label_created_at,
	return_shipment_at,
	return_delivery_date,
	refurbishment_start_at,
	refurbishment_end_at,
	DATEDIFF ('hour',subscription_created_at::timestamp,allocated_at::timestamp ) as time_to_allocate,
	DATEDIFF ( 'hour', allocated_at::timestamp, ready_to_ship_at::timestamp ) as time_to_prepare_to_ship,
	DATEDIFF ( 'hour', ready_to_ship_at::timestamp, picked_by_carrier_at::timestamp ) as time_to_pickup,
	DATEDIFF ( 'hour', picked_by_carrier_at::timestamp, sm.failed_delivery_at::timestamp )as time_to_fd,
	DATEDIFF ( 'hour', picked_by_carrier_at::timestamp, revocation_date::timestamp ) as time_to_revoke,
	case 
  	when (DATEDIFF('day', order_completed_at::timestamp, coalesce(first_delivery_attempt::timestamp, delivered_at::timestamp)) - datediff('week', order_completed_at::timestamp, coalesce(first_delivery_attempt::timestamp, delivered_at::timestamp) ))::decimal<=5 
   then true end as assets_delivered_on_time,
    sm.transit_time as time_to_deliver_excl_weekend,
    sm.delivery_time as total_delivery_time_excl_weekend,
    sm.net_ops_cycle as net_ops_delivery_time_excl_weekend,
    DATEDIFF ( 'hour', delivered_at::timestamp, return_shipment_label_created_at::timestamp ) as time_to_use_asset,
    DATEDIFF ( 'hour', return_shipment_label_created_at::timestamp, return_shipment_at::timestamp ) as time_to_retern_pack,
    DATEDIFF ( 'hour', return_shipment_at::timestamp, return_delivery_date::timestamp ) as time_to_retern_shipment,
    DATEDIFF ( 'hour', return_delivery_date::timestamp, refurbishment_start_at::timestamp ) as time_to_start_refurbishment,
     case 
  when DATEDIFF ( 'day', a.refurbishment_start_at::timestamp, a.refurbishment_end_at::timestamp )::Decimal<=3 
   then true  
  end as refurbished_on_time,
   case 
  when a.returned_final_condition in ('DAMAGED','WARRANTY','IRREPARABLE/DISPOSAL','LOCKED','INCOMPLETE') 
   then true 
  end as assets_refurbished_damaged,
  DATEDIFF ( 'hour', a.refurbishment_start_at::timestamp, a.refurbishment_end_at::timestamp ) as time_to_finish_refurbishment
	from master.allocation a
	 left join (select allocation_id,first_delivery_attempt, failed_delivery_at, transit_time, delivery_time , net_ops_cycle from ods_operations.allocation_shipment) sm 
	    on sm.allocation_id = a.allocation_id
	left join master.subscription s on a.subscription_id = s.subscription_id  
	    where rank_allocations_per_subscription = 1 
	    and a.store_short NOT IN ('Partners Offline', 'Patners Online')
	)
	, allocated_detailed as (
	select 
		DATE_TRUNC('week',allocated_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate
		--,percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80
		--,percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)
	, allocated_store as (
	select 
		DATE_TRUNC('week',allocated_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate
		--,percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80
		--,percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)
	, allocated_region as (
	select 
		DATE_TRUNC('week',allocated_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate
		--,percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80
		--,percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)
	, allocated_total as (
	select 
		DATE_TRUNC('week',allocated_at) as fact_date,
		'Total' AS store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate
		--,percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80
		--,percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)	
	, allocated as (
	select * from allocated_detailed 
	union all 
    select * from allocated_store
	union all 
    select * from allocated_region
    UNION ALL 
	select * from allocated_total     
	)
	, ready_to_ship_detailed as (
	select 
		DATE_TRUNC('week',ready_to_ship_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship
		--,percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80
		--,percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
	, ready_to_ship_store as (
	select 
		DATE_TRUNC('week',ready_to_ship_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship
		--,percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80
		--,percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
	, ready_to_ship_region as (
	select 
		DATE_TRUNC('week',ready_to_ship_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship
		--,percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80
		--,percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
	, ready_to_ship_total as (
	select 
		DATE_TRUNC('week',ready_to_ship_at) as fact_date,
		'Total' as  store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship
		--,percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80
		--,percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
	, ready_to_ship as (
	select * from ready_to_ship_detailed 
	union all 
    select * from ready_to_ship_store
	union all 
    select * from ready_to_ship_region
    UNION ALL 
	select * from ready_to_ship_total    
	)
	, shipped_detailed as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup
		--,percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80
		--,percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)
	, shipped_store as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup
		--,percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80
		--,percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)	
	, shipped_region as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup
		--,percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80
		--,percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)	
	, shipped_total as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup
		--,percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80
		--,percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)
	, shipped as (
	select * from shipped_detailed 
	union all 
    select * from shipped_store
	union all 
    select * from shipped_region
    UNION ALL 
	select * from shipped_total    
	)
	, shipped_store_b2b as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped_b2b
		from allocations
		where customer_type = 'business_customer'
		group by 1,2,3
		)	
	, shipped_region_b2b as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped_b2b
		from allocations
		where customer_type = 'business_customer'
		group by 1,2,3
		)	
	, shipped_total_b2b as (
	select 
		DATE_TRUNC('week',shipment_label_created_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped_b2b
		from allocations
		where customer_type = 'business_customer'
		group by 1,2,3
		)
	, shipped_b2b as (
    select * from shipped_store_b2b
	union all 
    select * from shipped_region_b2b
    UNION ALL 
	select * from shipped_total_b2b    
	)	
	, fd_detailed as (
	select 
		DATE_TRUNC('week',failed_delivery_at) as fact_date,
		 store_short,
		is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd
		--,percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80
		--,percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd_store as (
	select 
		DATE_TRUNC('week',failed_delivery_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd
		--,percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80
		--,percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd_region as (
	select 
		DATE_TRUNC('week',failed_delivery_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd
		--,percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80
		--,percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd_total as (
	select 
		DATE_TRUNC('week',failed_delivery_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd
		--,percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80
		--,percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd as (
	select * from fd_detailed 
	union all 
    select * from fd_store
	union all 
    select * from fd_region
    UNION ALL 
	select * from fd_total    
	)		
	, revocations_detailed as (
	select 
		DATE_TRUNC('week',revocation_date) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke
		--,percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80
		--,percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations_store as (
	select 
		DATE_TRUNC('week',revocation_date) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke
		--,percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80
		--,percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations_region as (
	select 
		DATE_TRUNC('week',revocation_date) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke
		--,percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80
		--,percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations_total as (
	select 
		DATE_TRUNC('week',revocation_date) as fact_date,
		 'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke
		--,percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80
		--,percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations as (
	select * from revocations_detailed 
	union all 
    select * from revocations_store
	union all 
    select * from revocations_region
    UNION ALL 
	select * from revocations_total    
	)	
	, delivered_detailed as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend
		--,percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)	
	, delivered_store as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend
		--,percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_region as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend
		--,percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_all_total as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		 'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend
		--,percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)	
	, delivered as (
	select * from delivered_detailed 
	union all 
    select * from delivered_store
	union all 
    select * from delivered_region
    UNION ALL 
	select * from delivered_all_total    
	)		
	, delivered_detailed_total as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		store_short,
		is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend
		--,percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_store_total as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		store_short,
		'Total' as  is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as net_ops_delivery_time_excl_weekend
		--,percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_region_total as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		store_region,
		'Total' as  is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as net_ops_delivery_time_excl_weekend
		--,percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_total_total as (
	select 
		DATE_TRUNC('week',delivered_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as net_ops_delivery_time_excl_weekend
		--,percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80
		--,percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
		, delivered_total as (
	select * from delivered_detailed_total 
	union all 
    select * from delivered_store_total
	union all 
    select * from delivered_region_total
    UNION ALL 
	select * from delivered_total_total  
	)	
    , return_shipping_label_created_detailed as (
	select 
		DATE_TRUNC('week',return_shipment_label_created_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset
		--,percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80
		--,percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	  , return_shipping_label_created_store as (
	select 
		DATE_TRUNC('week',return_shipment_label_created_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset
		--,percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80
		--,percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	  , return_shipping_label_created_region as (
	select 
		DATE_TRUNC('week',return_shipment_label_created_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset
		--,percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80
		--,percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	  , return_shipping_label_created_total as (
	select 
		DATE_TRUNC('week',return_shipment_label_created_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset
		--,percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80
		--,percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	 , return_shipping_label_created as (
	select * from return_shipping_label_created_detailed 
	union all 
    select * from return_shipping_label_created_store
	union all 
    select * from return_shipping_label_created_region
    UNION ALL 
	select * from return_shipping_label_created_total    
	)		
    , return_ship_detailed as (
	select 
		DATE_TRUNC('week',return_shipment_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack
		--,percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship_store as (
	select 
		DATE_TRUNC('week',return_shipment_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack
		--,percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship_region as (
	select 
		DATE_TRUNC('week',return_shipment_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack
		--,percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship_total as (
	select 
		DATE_TRUNC('week',return_shipment_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack
		--,percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship as (
	select * from return_ship_detailed 
	union all 
    select * from return_ship_store
	union all 
    select * from return_ship_region
    UNION ALL 
	select * from return_ship_total    
	)	
	, return_deliver_detailed as (
	select 
		DATE_TRUNC('week',return_delivery_date) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment
		--,percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver_store as (
	select 
		DATE_TRUNC('week',return_delivery_date) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment
		--,percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver_region as (
	select 
		DATE_TRUNC('week',return_delivery_date) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment
		--,percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver_total as (
	select 
		DATE_TRUNC('week',return_delivery_date) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment
		--,percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80
		--,percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver as (
	select * from return_deliver_detailed 
	union all 
    select * from return_deliver_store
	union all 
    select * from return_deliver_region
    UNION ALL 
	select * from return_deliver_total    
	)		
	, refurbished_detailed as (
	select 
		DATE_TRUNC('week',refurbishment_start_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
	, refurbished_store as (
	select 
		DATE_TRUNC('week',refurbishment_start_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
	, refurbished_region as (
	select 
		DATE_TRUNC('week',refurbishment_start_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
	, refurbished_total as (
	select 
		DATE_TRUNC('week',refurbishment_start_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished as (
	select * from refurbished_detailed 
	union all 
    select * from refurbished_store
	union all 
    select * from refurbished_region
    UNION ALL 
	select * from refurbished_total    
	)	
		, refurbished_end_detailed as (
	select 
		DATE_TRUNC('week',refurbishment_end_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end_store as (
	select 
		DATE_TRUNC('week',refurbishment_end_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end_region as (
	select 
		DATE_TRUNC('week',refurbishment_end_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end_total as (
	select 
		DATE_TRUNC('week',refurbishment_end_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment
		--,percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80
		--,percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end as (
	select * from refurbished_end_detailed 
	union all 
    select * from refurbished_end_store
	union all 
    select * from refurbished_end_region
    UNION ALL 
	select * from refurbished_end_total    
	)	
	, inbound as (
		select 
			date_trunc('week', created_at) as fact_date,
			region  as store_short,
			cast('Total' as text) as is_recirculated, 
			sum(booked_asset) as booked_asset,
			sum(case when new_bstock = 'new' then booked_asset end) as booked_asset_new,
			sum(case when new_bstock = 'b-stock' then booked_asset end) as booked_asset_bstock
		from dm_operations.assets_booked 
		group by 1, 2
		)
	, return_received_assets as(
	select 
	date_trunc('week', source_timestamp) as fact_date,
	'Total' as is_recirculated,
	'EU' as store_region,
	count(distinct serial_number) as return_received_assets
	from recommerce.ingram_micro_send_order_grading_status imsogs 
	where disposition_code ='RETURNED' and status_code ='RETURN RECEIVED'
	group by 1,2,3
	)
select 
	a.fact_date, 
	a.store_short,
	a.is_recirculated,
	assets_allocated,
	assets_recirculated,
	time_to_allocate,
	--time_to_allocate_median_80,
	--time_to_allocate_median_95,
	assets_ready_to_ship,
	assets_shipped,
	assets_shipped_b2b,
    time_to_deliver_excl_weekend,
    --time_to_deliver_excl_weekend_median_80,
    --time_to_deliver_excl_weekend_median_95,
    avg_net_ops_delivery_time_excl_weekend,
	total_delivery_time_excl_weekend,
	--total_delivery_time_excl_weekend_median_80,
	--total_delivery_time_excl_weekend_median_95,
	assets_delivered_on_time,
	time_to_prepare_to_ship,
	--time_to_prepare_to_ship_median_80,
	--time_to_prepare_to_ship_median_95,
	time_to_pickup,
	--time_to_pickup_median_80,
	--time_to_pickup_median_95,
	assets_failed_delivery,
	time_to_fd,
	--time_to_fd_median_80,
	--time_to_fd_median_95,
	revocations.assets_revoked,
	time_to_revoke,
	--time_to_revoke_median_80,
	--time_to_revoke_median_95,
	assets_delivered,
	return_shipping_label_created,
	time_to_use_asset,
	--time_to_use_asset_median_80,
	--time_to_use_asset_median_95,
	return_shipping,
	time_to_retern_pack,
	--time_to_retern_pack_median_80,
	--time_to_retern_pack_median_95,
	return_delivered,
	time_to_retern_shipment,
	--time_to_retern_shipment_median_80,
	--time_to_retern_shipment_median_95,
	assets_refurbished,
	assets_refurbished_end,
	time_to_start_refurbishment,
	--time_to_start_refurbishment_median_80,
	--time_to_start_refurbishment_median_95,
	time_to_finish_refurbishment,
	--time_to_finish_refurbishment_median_80,
	--time_to_finish_refurbishment_median_95,
	assets_refurbished_damaged,
	inb.booked_asset,
	inb.booked_asset_new,
	inb.booked_asset_bstock,
	rre.return_received_assets
		from allocated a 
 --
 --
 --
left join ready_to_ship r 
 on a.fact_date=r.fact_date
 and a.store_short=r.store_short
 and a.is_recirculated=r.is_recirculated
  --
 --
 --
left join shipped s 
on a.fact_date=s.fact_Date
 and a.store_short=s.store_short
 and a.is_recirculated=s.is_recirculated
  --
 --
 --
left join shipped_b2b sb
on a.fact_date=sb.fact_date
 and a.store_short=sb.store_short
 and a.is_recirculated=sb.is_recirculated
  --
 --
 --
left join fd
on a.fact_date=fd.fact_date
 and a.store_short=fd.store_short
 and a.is_recirculated=fd.is_recirculated
  --
 --
 --
left join revocations
on a.fact_date=revocations.fact_date
 and a.store_short=revocations.store_short
 and a.is_recirculated=revocations.is_recirculated
  --
 --
 --
left join delivered d
on a.fact_date=d.fact_date
 and a.store_short=d.store_short
 and a.is_recirculated=d.is_recirculated
  --
 --
 --
 left join delivered_total dt
on a.fact_date=dt.fact_date
 and a.store_short=dt.store_short
 and a.is_recirculated=dt.is_recirculated
  --
 --
 --
left join return_ship rs
on a.fact_date=rs.fact_Date
 and a.store_short=rs.store_short
 and a.is_recirculated=rs.is_recirculated
 --
 --
left join return_shipping_label_created rsl
on a.fact_date=rsl.fact_Date
 and a.store_short=rsl.store_short
 and a.is_recirculated=rsl.is_recirculated 
 --
 --
 --
left join return_deliver rd 
on a.fact_date=rd.fact_date
 and a.store_short=rd.store_short
 and a.is_recirculated=rd.is_recirculated
 --
 --
 --
left join refurbished refu 
 on a.fact_Date=refu.fact_Date
  and a.store_short=refu.store_short
 and a.is_recirculated=refu.is_recirculated
  --
 --
 --
left join refurbished_end refue
 on a.fact_Date=refue.fact_Date
  and a.store_short=refue.store_short
 and a.is_recirculated=refue.is_recirculated
--
--
--
left join inbound inb 
on a.fact_date = inb.fact_date
and a.store_short = inb.store_short
and a.is_recirculated = inb.is_recirculated
--
--
--
left join return_received_assets as rre
on a.fact_date = rre.fact_date 
and a.is_recirculated = rre.is_recirculated
and a.store_short = rre.store_region
--
--
--
order by 1 desc;




------------------------------------
-- Shipment reporting overview table --
-------------------------------------

drop table if exists dwh.shipment_reporting_overview;
create table dwh.shipment_reporting_overview as
with allocations as (
select 
	a.allocation_id,
     case when s.store_short = 'Grover International' then s.country_name 
		 -- when s.store_short = 'Partners Online' then s.store_short || '-' || s.country_name 
		 else s.store_short end as store_short,   
     case when s.country_name = 'United States' then 'US' else 'EU' end as store_region,
     is_recirculated,
     issue_date,
	issue_reason
	from ods_production.allocation a 
	 left join (select subscription_id, store_short, country_name from ods_production.subscription ) s
	    on s.subscription_id = a.subscription_id 
     where rank_allocations_per_subscription = 1 
	 and s.store_short NOT IN ('Partners Offline', 'Partners Online')
	)
,doa_detailed as (
	select 
		DATE_TRUNC('week',issue_date) as fact_date,
		store_short,
		is_recirculated,
		count(case when issue_reason = 'DOA' then allocation_id end) as doa
		from allocations
		group by 1,2,3
		)
	, doa_store as (
	select 
		DATE_TRUNC('week',issue_date) as fact_date,	
		store_short,
		'Total' as is_recirculated,
		count(case when issue_reason = 'DOA' then allocation_id end) as doa
		from allocations
		group by 1,2,3
		)
	, doa_region as (
	select 
		DATE_TRUNC('week',issue_date) as fact_date,			
		store_region,
		'Total' as is_recirculated,
		count(case when issue_reason = 'DOA' then allocation_id end) as doa
		from allocations
		group by 1,2,3
		)
	, doa_total as (
	select 
		DATE_TRUNC('week',issue_date) as fact_date,			
		'Total' as store_short,
		'Total' as is_recirculated,
		count(case when issue_reason = 'DOA' then allocation_id end) as doa
		from allocations
		group by 1,2,3
		)
	, doa as (
	select * from doa_detailed 
	union all 
    select * from doa_store
	union all 
    select * from doa_region
    UNION ALL 
	select * from doa_total    
	)
select 
sr.fact_date,
sr.store_short,
sr.is_recirculated,
round(sum(sr.assets_recirculated::numeric)/nullif(sum(sr.assets_allocated::numeric),0) * 100,2) as recirculation_rate,
coalesce(sr.assets_allocated,0) as assets_allocated,
coalesce(sr.assets_ready_to_ship,0) as assets_ready_to_ship,
coalesce(sr.assets_shipped,0) as assets_shipped,
coalesce(sr.assets_delivered,0) as assets_delivered,
coalesce(sr.assets_failed_delivery,0) as assets_failed_delivery,
round(sum(sr.assets_failed_delivery::numeric)/nullif(sum(sr.assets_delivered::numeric),0) * 100,1) as failed_delivery_rate,
coalesce(sr.assets_revoked,0) as assets_revoked,
round(sum(sr.assets_revoked::numeric)/nullif(sum(sr.assets_delivered::numeric),0) * 100,1) as revocation_rate,
coalesce(doa,0) as assets_doa
from dwh.shipment_reporting sr
left join doa as doa
on sr.fact_date = doa.fact_date 
and sr.is_recirculated = doa.is_recirculated
and sr.store_short = doa.store_short
GROUP BY 1,2,3,5,6,7,8,9,11,13;

GRANT SELECT ON dwh.shipment_reporting TO tableau;
GRANT SELECT ON dwh.shipment_reporting_overview TO tableau;




-- new ceo sc reporting
-- after confirming the script below
-- upper section will be deleted
-- along with 
--      ******* shipment_reporting_monthly.sql
--      ******* shipment_reporting_daily.sql



--
--
-- Source
--
drop table if exists rawd;
create temp table rawd as 
	select 
		sa.allocation_id,
		sa.tracking_number,
		COALESCE(sa.shipping_country,sa.receiver_country) AS country,
		a.is_recirculated,
		sa.is_delivered_on_time,
		sa.customer_type,
		--we start counting the days from the order completion as confirmed with Mike (OPS)
		--BI-8431: the intervals and the shipping services are provided by OPS
		CASE 
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)) <= 4)
						AND country = 'Germany'
						AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date 
						AND d.week_day_number IN (6, 7)) <= 3)
						AND country = 'Netherlands'
						AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)) <= 4)
						AND country = 'Austria'
						AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE	
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)) <= 5)
						AND country = 'Spain'
						AND shipment_service <> 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)) <= 10)
						AND country = 'Germany'
						AND shipment_service = 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)) <= 12)
						AND country = 'Netherlands'
						AND shipment_service = 'Hermes 2MH Standard'
				THEN TRUE
			WHEN (datediff('day', sa.order_completed_at, sa.first_delivery_attempt ) 
        		- (SELECT count(1) FROM public.dim_dates d
					WHERE d.datum >= sa.order_completed_at::date 
						AND d.datum < sa.first_delivery_attempt::date
						AND d.week_day_number IN (6, 7)) <= 10)
						AND country = 'Austria'
						AND shipment_service = 'Hermes 2MH Standard'
				THEN TRUE
			--No bulky/2 man handling for Spain
			ELSE FALSE
		END AS is_promised_delivery_time_fullfillment,
		sa.allocated_at,
		sa.ready_to_ship_at ,
		sa.shipment_label_created_at ,
		sa.shipment_at,
		sa.failed_delivery_at ,
		a.revocation_date,
		COALESCE(sa.first_delivery_attempt , sa.delivered_at) AS first_delivery_attempt,
		sa.delivered_at,
		DATEDIFF('hour', a.subscription_created_at, sa.allocated_at::timestamp ) as time_to_allocate,
		sa.warehouse_lt as time_to_pickup,
		sa.transit_time as time_to_deliver,
		sa.net_ops_cycle,
		least(
			sa.allocated_at,
			sa.ready_to_ship_at ,
			sa.shipment_label_created_at ,
			sa.failed_delivery_at ,
			a.revocation_date,
			sa.delivered_at) min_date,
		sa.failed_reason
	from ods_operations.allocation_shipment sa
	left join master.allocation a 
	on sa.allocation_id = a.allocation_id 
	WHERE a.store_short NOT IN ('Partners Offline', 'Partners Online')
	AND min_date > date_add('month', -18, date_trunc('month', current_date))
	-- AND country in ('Germany', 'Austria', 'Netherlands', 'Spain', 'United States', 'Partners Online')
;


--
--
-- 1. Totals (Assets Allocated, Delivered, etc.)
--
DROP TABLE IF EXISTS rawd_totals; -- change to dm_operations
CREATE temp TABLE rawd_totals as -- change to dm_operations
select 
		'Assets Allocated' as kpi_name,
		date_trunc('day', allocated_at) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	group by 1, 2, 3, 4
	union all 
	select 
		'Assets Recirculated' as kpi_name,
		date_trunc('day', allocated_at) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	where is_recirculated = 'Re-circulated'
	group by 1, 2, 3, 4
	union all
	select 
		'Assets Ready to Ship' as kpi_name,
		date_trunc('day', ready_to_ship_at) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	group by 1, 2, 3, 4
	union all 
	select 
		'Assets Shipped' as kpi_name,
		date_trunc('day', shipment_label_created_at) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	group by 1, 2, 3, 4
	union all 
	select 
		'Assets Shipped B2B' as kpi_name,
		date_trunc('day', shipment_label_created_at) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	where customer_type = 'business_customer'
	group by 1, 2, 3, 4
	union all 
	select 
		'Assets Failed Delivery' as kpi_name,
		date_trunc('day', failed_delivery_at) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	where COALESCE(failed_reason, ' ') <> 'Customer Refused'
	group by 1, 2, 3, 4
	union all 
	select 
		'Assets Revoked' as kpi_name,
		date_trunc('day', revocation_date) as fact_date,
		country,
		'Total' as is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	group by 1, 2, 3, 4
	union all
	--delivered on time
	select 
		'Assets Delivered' as kpi_name,
		date_trunc('day', delivered_at) as fact_date,
		country,
		is_recirculated ,
		count(allocation_id) as kpi_value
	from rawd 
	group by 1, 2, 3, 4
	union all
	select 
		'Assets Delivered On Time' as kpi_name ,
		date_trunc('day', delivered_at) as fact_date,
		country,
		is_recirculated ,
		count(case when is_delivered_on_time then allocation_id end) as kpi_value
	from rawd 
	group by 1, 2, 3, 4
	union all
	select 
		'Delivery Promise Allocation Level - Fullfilled' as kpi_name ,
		date_trunc('day', first_delivery_attempt) as fact_date,
		country,
		is_recirculated ,
		count(distinct case when is_promised_delivery_time_fullfillment is true then allocation_id end) as kpi_value
	from rawd
	group by 1, 2, 3, 4
	union all
	select 
		'Delivery Promise Allocation Level - Not Fullfilled' as kpi_name ,
		date_trunc('day', first_delivery_attempt) as fact_date,
		country,
		is_recirculated ,
		count(distinct case when is_promised_delivery_time_fullfillment is false then allocation_id end) as kpi_value
	from rawd
	group by 1, 2, 3, 4
	union all
	select 
		'Delivery Promise - Fullfilled' as kpi_name ,
		date_trunc('day', first_delivery_attempt) as fact_date,
		country,
		null AS is_recirculated ,
		count(distinct case when is_promised_delivery_time_fullfillment is true then tracking_number end) as kpi_value
	from rawd
	group by 1, 2, 3, 4
	union all
	select 
		'Delivery Promise - Not Fullfilled' as kpi_name ,
		date_trunc('day', first_delivery_attempt) as fact_date,
		country,
		null AS is_recirculated ,
		count(distinct case when is_promised_delivery_time_fullfillment is false then tracking_number end) as kpi_value
	from rawd
	group by 1, 2, 3, 4;



DROP TABLE IF EXISTS final_totals; -- change to dm_operations
CREATE temp TABLE final_totals as 
select 
	'Daily' as report_name ,
	kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value 
from rawd_totals 
where fact_date > current_date - 14
union all 
select 
	'Weekly' as report_name ,
	kpi_name ,
	date_trunc('week', fact_date) as fact_date,
	country ,
	is_recirculated ,
	sum(kpi_value) as kpi_value  
from rawd_totals 
where fact_date > current_date - 12 * 7
group by 1,2,3,4,5
union all 
select 
	'Monthly' as report_name ,
	kpi_name ,
	date_trunc('month', fact_date) as fact_date,
	country ,
	is_recirculated ,
	sum(kpi_value) as kpi_value  
from rawd_totals 
group by 1,2,3,4,5;
	



--
--
-- 2. Lead Times (Time to Deliver, Net OPS Cycle, etc.)
--


drop table if exists final_lead_times;
create temp table final_lead_times as 
with
rawd_daily as (
	select * from rawd 
	where min_date > current_date - 14
),
rawd_weekly as (
	select * from rawd 
	where min_date > current_date - 12 * 7
),
time_to_allocate (report_name , fact_date,  country, is_recirculated , kpi_value) as (
	select 'Daily', date_trunc('day', allocated_at), 'Total', 'Total' , median(time_to_allocate) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', allocated_at),  country, 'Total' ,  median(time_to_allocate) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', allocated_at), country, is_recirculated , median(time_to_allocate) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', allocated_at), 'Total', 'Total' , median(time_to_allocate) 
	from rawd_weekly  group by 1 , 2, 3 , 4
	union all
	select 'Weekly', date_trunc('week', allocated_at), country, 'Total' ,  median(time_to_allocate) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', allocated_at), country, is_recirculated , median(time_to_allocate) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all	
	select 'Monthly', date_trunc('month', allocated_at), 'Total', 'Total' , median(time_to_allocate) 
	from rawd  group by 1 , 2, 3 , 4
	union all
	select 'Monthly', date_trunc('month', allocated_at), country, 'Total' ,  median(time_to_allocate) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', allocated_at),  country, is_recirculated , median(time_to_allocate) 
	from rawd  group by 1 , 2, 3, 4
),
time_to_pickup  (report_name ,  fact_date, country, is_recirculated , kpi_value) as (
	select 'Daily', date_trunc('day', shipment_at), 'Total', 'Total' , median(time_to_pickup) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', shipment_at), country, 'Total' ,  median(time_to_pickup) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', shipment_at), country, is_recirculated , median(time_to_pickup) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	--week
	select 'Weekly', date_trunc('week', shipment_at), 'Total', 'Total' , median(time_to_pickup) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', shipment_at), country, 'Total' ,  median(time_to_pickup) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', shipment_at),country, is_recirculated , median(time_to_pickup) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	--month
	select 'Monthly', date_trunc('month', shipment_at), 'Total', 'Total' , median(time_to_pickup) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', shipment_at), country, 'Total' ,  median(time_to_pickup) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', shipment_at), country, is_recirculated , median(time_to_pickup) 
	from rawd  group by 1 , 2, 3, 4
	
),
time_to_deliver  (report_name , fact_date, country, is_recirculated , kpi_value) as (
	select 'Daily', date_trunc('day', first_delivery_attempt),  'Total', 'Total' , median(time_to_deliver) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', first_delivery_attempt),  country, 'Total' ,  median(time_to_deliver) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', first_delivery_attempt), country, is_recirculated , median(time_to_deliver) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	--week
	select 'Weekly', date_trunc('week', first_delivery_attempt), 'Total', 'Total' , median(time_to_deliver) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', first_delivery_attempt), country, 'Total' ,  median(time_to_deliver) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', first_delivery_attempt), country, is_recirculated , median(time_to_deliver) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all 
	--month 
	select 'Monthly', date_trunc('month', first_delivery_attempt), 'Total', 'Total' , median(time_to_deliver) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', first_delivery_attempt), country, 'Total' ,  median(time_to_deliver) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', first_delivery_attempt), country, is_recirculated , median(time_to_deliver) 
	from rawd  group by 1 , 2, 3, 4
)	
, time_to_deliver_avg (report_name , fact_date, country, is_recirculated , kpi_value) AS (
	select 'Daily', date_trunc('day', first_delivery_attempt),  'Total', 'Total' , avg(time_to_deliver) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', first_delivery_attempt),  country, 'Total' ,  avg(time_to_deliver) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', first_delivery_attempt), country, is_recirculated , avg(time_to_deliver) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	--week
	select 'Weekly', date_trunc('week', first_delivery_attempt), 'Total', 'Total' , avg(time_to_deliver) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', first_delivery_attempt), country, 'Total' ,  avg(time_to_deliver) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', first_delivery_attempt), country, is_recirculated , avg(time_to_deliver) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all 
	--month 
	select 'Monthly', date_trunc('month', first_delivery_attempt), 'Total', 'Total' , avg(time_to_deliver) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', first_delivery_attempt), country, 'Total' ,  avg(time_to_deliver) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', first_delivery_attempt), country, is_recirculated , avg(time_to_deliver) 
	from rawd  group by 1 , 2, 3, 4
),
net_ops_cycle  (report_name ,  fact_date, country, is_recirculated , median, average) as (
	select 'Daily', date_trunc('day', first_delivery_attempt), 'Total', 'Total' , median(net_ops_cycle), avg(net_ops_cycle::float) 
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', first_delivery_attempt), country, 'Total' ,  median(net_ops_cycle), avg(net_ops_cycle::float)
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	select 'Daily', date_trunc('day', first_delivery_attempt), country, is_recirculated , median(net_ops_cycle), avg(net_ops_cycle::float)
	from rawd_daily  group by 1 , 2, 3, 4
	union all
	--week
	select 'Weekly', date_trunc('week', first_delivery_attempt), 'Total', 'Total' , median(net_ops_cycle), avg(net_ops_cycle::float) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', first_delivery_attempt), country, 'Total' ,  median(net_ops_cycle), avg(net_ops_cycle::float) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all
	select 'Weekly', date_trunc('week', first_delivery_attempt), country, is_recirculated , median(net_ops_cycle), avg(net_ops_cycle::float) 
	from rawd_weekly  group by 1 , 2, 3, 4
	union all 
	--month 
	select 'Monthly', date_trunc('month', first_delivery_attempt), 'Total', 'Total' , median(net_ops_cycle), avg(net_ops_cycle::float)
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', first_delivery_attempt), country, 'Total' ,  median(net_ops_cycle), avg(net_ops_cycle::float) 
	from rawd  group by 1 , 2, 3, 4
	union all
	select 'Monthly', date_trunc('month', first_delivery_attempt), country, is_recirculated , median(net_ops_cycle), avg(net_ops_cycle::float) 
	from rawd  group by 1 , 2, 3, 4
)
select 
	report_name ,
	'Time To Allocate, hours (Median)' kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value
from time_to_allocate
union all 
select 
	report_name ,
	'Time To Pickup, hours (Median)' kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value
from time_to_pickup
union all 
select 
	report_name ,
	'Time To Deliver, hours (Median)' kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value
from time_to_deliver
union all 
select 
	report_name ,
	'Time To Deliver, hours (AVG)' kpi_name , 
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value
from time_to_deliver_avg
union all 
select 
	report_name ,
	'Net Ops Cycle Time, days (Average)' kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	average as kpi_value
from net_ops_cycle
union all 
select 
	report_name ,
	'Net Ops Cycle Time, days (Median)' kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	median as kpi_value
from net_ops_cycle;



--
--
-- 3. Overview
--
drop table if exists final_overview;
create temp table final_overview as 
with weeks (fact_week) as (
	select date_add('week', -1, date_trunc('week', current_date))
	union 
	select date_add('week', -2, date_trunc('week', current_date))
),
months (fact_month) as (
	select date_add('month', -1, date_trunc('month', current_date))
	union 
	select date_add('month', -2, date_trunc('month', current_date))
),
--
-- Net Ops 
pre_net_ops (report_name, fact_date, country, median, average) as (
	select 
		'Weekly', 
		date_trunc('week', first_delivery_attempt) as fact_date , 
		case when country = 'United States'
		 	then 'US' else 'EU' end ,
		median(net_ops_cycle), 
		avg(net_ops_cycle::float) 
	from rawd
	where fact_date in (select fact_week  from weeks)
	group by 1,2,3
	union all
	select 
		'Monthly', 
		date_trunc('month', first_delivery_attempt)  as fact_date , 
		case when country = 'United States'
		 	then 'US' else 'EU' end ,
		median(net_ops_cycle), 
		avg(net_ops_cycle::float)
	from rawd
	where fact_date in (select fact_month from months)
	group by 1,2,3
),
--
--Assets Booked
--
--
pre_assets_booked as (
	select 
		created_at ,
		region,
		booked_asset 
	from dm_operations.assets_booked 
	where (date_trunc('week', created_at) in (select fact_week from weeks)
		  or 
		   date_trunc('month', created_at) in (select fact_month from months))
),
assets_booked as (
	select 
		'Weekly' as report_name ,
		region as country,
		date_trunc('week', created_at) as fact_date,
		sum(booked_asset) kpi_value
	from pre_assets_booked 
	where date_trunc('week', created_at) in (select fact_week from weeks)
	group by 1,2,3
	union all
	select 
		'Monthly' as report_name ,
		region as country,
		date_trunc('month', created_at) as fact_date,
		sum(booked_asset) kpi_value
	from pre_assets_booked 
	where date_trunc('month', created_at) in (select fact_month from months)
	group by 1,2,3
),
--Return Received
pre_returns_received as (
	select 
		serial_number ,
		source_timestamp
	from recommerce.ingram_micro_send_order_grading_status 
	where disposition_code ='RETURNED' and status_code ='RETURN RECEIVED'
	and (date_trunc('week', source_timestamp) in (select fact_week from weeks)
		  or 
		 date_trunc('month', source_timestamp) in (select fact_month from months))
),
returns_received as (
	select 
		'Weekly' as report_name ,
		date_trunc('week', source_timestamp) as fact_date,
		count(distinct serial_number) as kpi_value
	from pre_returns_received 
	where fact_date in (select fact_week from weeks)
	group by 1, 2
	union all
	select 
		'Monthly' as report_name ,
		date_trunc('month', source_timestamp) as fact_date,
		count(distinct serial_number) as kpi_value
	from recommerce.ingram_micro_send_order_grading_status imsogs 
	where fact_date in (select fact_month from months)
	group by 1, 2
)
select 
	report_name ,
	fact_date ,
	case when country = 'United States'
		 then 'US' else 'EU' end country ,
	kpi_name,
	sum(kpi_value) as kpi_value 
from final_totals 
where kpi_name in ('Assets Shipped', 'Assets Shipped B2B')
and (fact_date in (select fact_week from weeks) and report_name = 'Weekly'
	or 
	 fact_date in (select fact_month from months) and report_name = 'Monthly')
group by 1,2,3,4
---
--- Delivered on Time
---
union all
select 
	report_name ,
	fact_date ,
	case when country = 'United States'
		 then 'US' else 'EU' end country ,
	'Delivered on Time' as kpi_name ,
	round(
		sum(case when kpi_name = 'Assets Delivered On Time' then kpi_value::float end) / 
		sum(case when kpi_name = 'Assets Delivered' then kpi_value::float end) * 100, 1) as kpi_value 
from final_totals 
where kpi_name in ('Assets Delivered', 'Assets Delivered On Time')
and (fact_date in (select fact_week from weeks) and report_name = 'Weekly'
	or 
	 fact_date in (select fact_month from months) and report_name = 'Monthly')
group by 1,2,3,4
---
--- Promised Time is fullfilled
---
---
union all
select 
	report_name ,
	fact_date ,
	case when country = 'United States'
		 then 'US' else 'EU' end country ,
	'Delivery Promise Success Rate - Allocation Level' as kpi_name ,
	round(
		sum(case when kpi_name = 'Delivery Promise Allocation Level - Fullfilled' then kpi_value::float end) / 
		(sum(case when kpi_name = 'Delivery Promise Allocation Level - Not Fullfilled' then kpi_value::float end) 
			+ sum(case when kpi_name = 'Delivery Promise Allocation Level - Fullfilled' then kpi_value::float end)) * 100, 1) as kpi_value --computing as ratio among fullfilled and total sum   
from final_totals 
where kpi_name in ('Delivery Promise Allocation Level - Fullfilled', 'Delivery Promise Allocation Level - Not Fullfilled')
and (fact_date in (select fact_week from weeks) and report_name = 'Weekly'
	or 
	 fact_date in (select fact_month from months) and report_name = 'Monthly')
group by 1,2,3,4

union all
select 
	report_name ,
	fact_date ,
	case when country = 'United States'
		 then 'US' else 'EU' end country ,
	'Delivery Promise Success Rate' as kpi_name ,
	round(
		sum(case when kpi_name = 'Delivery Promise - Fullfilled' then kpi_value::float end) / 
		(sum(case when kpi_name = 'Delivery Promise - Not Fullfilled' then kpi_value::float end) 
			+ sum(case when kpi_name = 'Delivery Promise - Fullfilled' then kpi_value::float end)) * 100, 1) as kpi_value --computing as ratio among fullfilled and total sum   
from final_totals 
where kpi_name in ('Delivery Promise - Fullfilled', 'Delivery Promise - Not Fullfilled')
and (fact_date in (select fact_week from weeks) and report_name = 'Weekly'
	or 
	 fact_date in (select fact_month from months) and report_name = 'Monthly')
group by 1,2,3,4
----
--- Net OPS Cycle
---
----
union all 
select 
	report_name , 
	fact_date , 
	country, 
	'Med. Net OPS Cycle Time' as kpi_name ,
	median as kpi_value 
from pre_net_ops
union all 
select 
	report_name , 
	fact_date , 
	country, 
	'Avg. Net OPS Cycle Time' as kpi_name ,
	round(average::float, 2)  as kpi_value 
from pre_net_ops  
----
---- Assets booked
----
union all 
select 
	report_name ,
	fact_date,
	country,
	'Assets Booked' as kpi_name ,
	kpi_value
from assets_booked
---
--- Returns
---
union all
select 
	report_name ,
	fact_date,
	'EU' as country,
	'Return Received' as kpi_name ,
	kpi_value
from returns_received;

--
--
-- 4. Overview 2024 Update (weekly level only)
--
drop table if exists overview_202405_update;
create temp table overview_202405_update as 
with weeks (fact_week) as (
	select date_add('week', -1, date_trunc('week', current_date))
	union 
	select date_add('week', -2, date_trunc('week', current_date))
)
, months (fact_month) as (
	select date_add('month', -1, date_trunc('month', current_date))
	union 
	select date_add('month', -2, date_trunc('month', current_date))
)
--
-- Net Ops 
, pre_net_ops (report_name, fact_date, country, median, average) as (
	select 
		'Weekly', 
		date_trunc('week', first_delivery_attempt) as fact_date , 
		case when country = 'United States'
		 	then 'US' else 'EU' end ,
		median(net_ops_cycle), 
		avg(net_ops_cycle::float) 
	from rawd
	where fact_date in (select fact_week  from weeks)
	group by 1,2,3
	union all
	select 
		'Monthly', 
		date_trunc('month', first_delivery_attempt)  as fact_date , 
		case when country = 'United States'
		 	then 'US' else 'EU' end ,
		median(net_ops_cycle), 
		avg(net_ops_cycle::float)
	from rawd
	where fact_date in (select fact_month from months)
	group by 1,2,3
),
--
--Assets Booked
--
--
pre_assets_booked as (
	select 
		created_at ,
		region,
		booked_asset 
	from dm_operations.assets_booked 
	where (date_trunc('week', created_at) in (select fact_week from weeks)
		  or 
		   date_trunc('month', created_at) in (select fact_month from months))
),
assets_booked as (
	select 
		'Weekly' as report_name ,
		region as country,
		date_trunc('week', created_at) as fact_date,
		sum(booked_asset) kpi_value
	from pre_assets_booked 
	where date_trunc('week', created_at) in (select fact_week from weeks)
	group by 1,2,3
	union all
	select 
		'Monthly' as report_name ,
		region as country,
		date_trunc('month', created_at) as fact_date,
		sum(booked_asset) kpi_value
	from pre_assets_booked 
	where date_trunc('month', created_at) in (select fact_month from months)
	group by 1,2,3
),
--Return Received
pre_returns_received as (
	select 
		serial_number ,
		source_timestamp
	from recommerce.ingram_micro_send_order_grading_status 
	where disposition_code ='RETURNED' and status_code ='RETURN RECEIVED'
	and (date_trunc('week', source_timestamp) in (select fact_week from weeks)
		  or 
		 date_trunc('month', source_timestamp) in (select fact_month from months))
), 
returns_received as (
	select 
		'Weekly' as report_name ,
		date_trunc('week', source_timestamp) as fact_date,
		count(distinct serial_number) as kpi_value
	from pre_returns_received 
	where fact_date in (select fact_week from weeks)
	group by 1, 2
	union all
	select 
		'Monthly' as report_name ,
		date_trunc('month', source_timestamp) as fact_date,
		count(distinct serial_number) as kpi_value
	from recommerce.ingram_micro_send_order_grading_status imsogs 
	where fact_date in (select fact_month from months)
	group by 1, 2
)
, supply_chain_weekly_kpis AS 
(
SELECT
	reporting_date::DATE AS fact_date,
--	section_name,
	REPLACE(REPLACE(INITCAP(CASE
		WHEN kpi_name = 'IN STOCK' AND section_name = 'INVENTORY'
			THEN 'STOCK LEVEL (UPS)'
		WHEN kpi_name = 'IN STOCK' AND section_name = 'REVERSE'
			THEN 'STOCK LEVEL (IM)'
		WHEN kpi_name = 'KLÄRFÄLLE MISSING QUANTITIES' AND section_name = 'DELIVERY MANAGEMENT'
			THEN 'MISSING QUANTITITES'
		ELSE kpi_name
	END), 'Ups', 'UPS'), 'Im', 'IM') AS kpi_name_new,
	kpi_value
FROM dm_operations.supply_chain_weekly scw 
WHERE kpi_name IN 
	('SHIPPED ASSETS (IM)', 'SHIPPED ASSETS (UPS)', 'IN STOCK', 'KLÄRFÄLLE MISSING QUANTITIES') 
	AND (reporting_date  = date_add('week', -1, date_trunc('week', current_date)) OR   -- LAST 2 complete weeks
		reporting_date  = date_add('week', -2, date_trunc('week', current_date)) )
	AND region = 'EU'
--ORDER BY 3,1
)
, reverse_happy_path_median AS 
(
SELECT
	DATE_TRUNC('week', end_reverse_process_at) fact_date,
--	shipping_country,
	'Reverse Happy Path (Med.)' AS kpi_name,
	ROUND(MEDIAN(CASE WHEN is_asset_in_repair = 0 AND kpi = 'Reverse Net Ops Cycle' THEN Reverse_Net_Ops_Cycle END ), 1) kpi_value
FROM dm_operations.v_reverse_times vrt 
WHERE shipping_country IN ('Germany', 'Austria', 'Netherlands', 'Spain')
--	AND end_reverse_process_at IS NOT NULL
GROUP BY 1
--ORDER BY 1 DESC 
HAVING (fact_date  = date_add('week', -1, date_trunc('week', current_date)) OR   -- LAST 2 complete weeks
		fact_date  = date_add('week', -2, date_trunc('week', current_date)) )
)
, reverse_net_ops_median AS 
(
SELECT
	DATE_TRUNC('week', end_reverse_process_at) fact_date,
--	shipping_country,
	'Reverse Net Ops Cycle (Med.)' AS kpi_name,
	ROUND(MEDIAN(CASE WHEN kpi = 'Reverse Net Ops Cycle' THEN Reverse_Net_Ops_Cycle END ), 1) kpi_value
FROM dm_operations.v_reverse_times vrt 
WHERE shipping_country IN ('Germany', 'Austria', 'Netherlands', 'Spain')
--	AND end_reverse_process_at IS NOT NULL
GROUP BY 1
--ORDER BY 1 DESC 
HAVING (fact_date  = date_add('week', -1, date_trunc('week', current_date)) OR   -- LAST 2 complete weeks
		fact_date  = date_add('week', -2, date_trunc('week', current_date)) )
)
---
--- Promised Time is fullfilled (for on time delivery rate)
---
select 
	report_name ,
	fact_date ,
	'EU' as country ,
	'Delivery Promise Success Rate' as kpi_name ,
	round(
		sum(case when kpi_name = 'Delivery Promise - Fullfilled' then kpi_value::float end) / 
		(sum(case when kpi_name = 'Delivery Promise - Not Fullfilled' then kpi_value::float end) 
			+ sum(case when kpi_name = 'Delivery Promise - Fullfilled' then kpi_value::float end)) * 100, 1) as kpi_value --computing as ratio among fullfilled and total sum   
from final_totals 
where kpi_name in ('Delivery Promise - Fullfilled', 'Delivery Promise - Not Fullfilled')
	AND country IN ('Germany', 'Netherlands', 'Spain', 'Austria')
		and (fact_date in (select fact_week from weeks) and report_name = 'Weekly')
--	or 
--	 fact_date in (select fact_month from months) and report_name = 'Monthly')
group by 1,2,3,4
----
--- Avg Net OPS Cycle
---
----
union all 
select 
	report_name , 
	fact_date , 
	country, 
	'Avg. Net OPS Cycle Time' as kpi_name ,
	round(average::float, 2)  as kpi_value 
from pre_net_ops  
WHERE report_name = 'Weekly'
---
--- Returns
---
union all
select 
	report_name ,
	fact_date,
	'EU' as country,
	'Return Received' as kpi_name ,
	kpi_value
from returns_received
WHERE report_name = 'Weekly'
UNION ALL 
SELECT
	'Weekly' AS report_name,
	fact_date,
	'EU' as country,
	kpi_name_new AS kpi_name,
	kpi_value
FROM supply_chain_weekly_kpis
UNION ALL 
SELECT
	'Weekly' AS report_name,
	fact_date,
	'EU' as country,
	'Shipped Assets (Total)' AS kpi_name,
	SUM(kpi_value) 
FROM supply_chain_weekly_kpis
WHERE kpi_name_new IN ('Shipped Assets (IM)', 'Shipped Assets (UPS)')
GROUP BY 1,2,3,4
UNION ALL 
SELECT
	'Weekly' AS report_name,
	fact_date,
	'EU' as country,
	'Stock level (Total)' AS kpi_name,
	SUM(kpi_value) 
FROM supply_chain_weekly_kpis
WHERE kpi_name_new IN ('Stock Level (IM)', 'Stock Level (UPS)')
GROUP BY 1,2,3,4
UNION ALL
SELECT
	'Weekly' AS report_name,
	fact_date,
	'EU' as country,
	kpi_name,
	kpi_value 
FROM reverse_happy_path_median
UNION ALL
SELECT
	'Weekly' AS report_name,
	fact_date,
	'EU' as country,
	kpi_name,
	kpi_value 
FROM reverse_net_ops_median
;



-- dm_weekly_monthly

DROP TABLE IF EXISTS dm_weekly_monthly.supply_chain; -- change to dm_operations
CREATE TABLE dm_weekly_monthly.supply_chain AS
select 
	report_name ,
	'Totals' as section_name,
	kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value::float
from final_totals 
union all 
select 
	report_name ,
	'Lead Times' as section_name,
	kpi_name ,
	fact_date ,
	country ,
	is_recirculated ,
	kpi_value::float 
from final_lead_times
union all 
select 
	report_name,
	'Overview' as section_name ,
	kpi_name,
	fact_date ,
	country ,
	'Total' as is_recirculated ,
	kpi_value::float 
from final_overview	
union all 
select 
	report_name,
	'Overview_2024_update' as section_name ,
	kpi_name,
	fact_date ,
	country ,
	'Total' as is_recirculated ,
	kpi_value::float 
from overview_202405_update	
;


GRANT SELECT ON dm_weekly_monthly.supply_chain TO tableau;
