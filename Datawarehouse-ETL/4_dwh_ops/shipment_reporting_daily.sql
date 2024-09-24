DROP TABLE IF EXISTS dwh.shipment_reporting_daily;
CREATE TABLE dwh.shipment_reporting_daily as

with allocations as (
select 
	a.allocation_id,
	case when a.store_short = 'Grover International' then s.country_name 
		 -- when a.store_short = 'Partners Online' then a.store_short || '-' || s.country_name 
		 else a.store_short end as store_short ,
	is_recirculated,
	allocated_at,
	ready_to_ship_at,
  	shipment_label_created_at,
	picked_by_carrier_at,
	failed_delivery_at,
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
	DATEDIFF ( 'hour', picked_by_carrier_at::timestamp, failed_delivery_at::timestamp )as time_to_fd,
	DATEDIFF ( 'hour', picked_by_carrier_at::timestamp, revocation_date::timestamp ) as time_to_revoke,
	case 
  	when (DATEDIFF('day', order_completed_at::timestamp, coalesce(first_delivery_attempt::timestamp, delivered_at::timestamp)) - datediff('week', order_completed_at::timestamp, coalesce(first_delivery_attempt::timestamp, delivered_at::timestamp) ))::decimal<=5 
   then true end as assets_delivered_on_time,
    DATEDIFF ('hour', picked_by_carrier_at::timestamp, first_delivery_attempt::timestamp ) - (24 * datediff('week', picked_by_carrier_at::timestamp, first_delivery_attempt::timestamp )) as time_to_deliver_excl_weekend,
    DATEDIFF('day', order_completed_at::timestamp, delivered_at::timestamp ) - datediff('week', order_completed_at::timestamp, delivered_at::timestamp ) as total_delivery_time_excl_weekend,
     DATEDIFF('day', allocated_at::timestamp, first_delivery_attempt::timestamp ) - datediff('week', allocated_at::timestamp, first_delivery_attempt::timestamp ) as net_ops_delivery_time_excl_weekend,
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
	 left join (select allocation_id,first_delivery_attempt from ods_operations.allocation_shipment ) ash
	    on ash.allocation_id = a.allocation_id
	left join master.subscription s on a.subscription_id = s.subscription_id  
	    where rank_allocations_per_subscription = 1 
	    and a.store_short NOT IN ('Partners Offline', 'Partners Online')
	)
	, allocated_detailed as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate,
		percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80,
		percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)
	, allocated_store as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate,
		percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80,
		percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)
	, allocated_total as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		'Total' AS store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated,
		median(time_to_allocate) as time_to_allocate,
		percentile_cont(0.8) within group (order by time_to_allocate) as time_to_allocate_median_80,
		percentile_cont(0.95) within group (order by time_to_allocate) as time_to_allocate_median_95
		from allocations
		group by 1,2,3
		)	
	, allocated as (
	select * from allocated_detailed 
	union all 
    select * from allocated_store
    UNION ALL 
	select * from allocated_total     
	)
	, ready_to_ship_detailed as (
	select 
		DATE_TRUNC('day',ready_to_ship_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship,
		percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80,
		percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
	, ready_to_ship_store as (
	select 
		DATE_TRUNC('day',ready_to_ship_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship,
		percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80,
		percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
		, ready_to_ship_total as (
	select 
		DATE_TRUNC('day',ready_to_ship_at) as fact_date,
		'Total' as  store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_ready_to_ship,
		median(time_to_prepare_to_ship) as time_to_prepare_to_ship,
		percentile_cont(0.8) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_80,
		percentile_cont(0.95) within group (order by time_to_prepare_to_ship) as time_to_prepare_to_ship_median_95
		from allocations
		group by 1,2,3
		)
	, ready_to_ship as (
	select * from ready_to_ship_detailed 
	union all 
    select * from ready_to_ship_store
    UNION ALL 
	select * from ready_to_ship_total    
	)
	, shipped_detailed as (
	select 
		DATE_TRUNC('day',shipment_label_created_at) as fact_date, --was picked_by_carrier_at before
		store_short,
		is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup,
		percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80,
		percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)
	, shipped_store as (
	select 
		DATE_TRUNC('day',shipment_label_created_at) as fact_date, --was picked_by_carrier_at before
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup,
		percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80,
		percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)	
	, shipped_total as (
	select 
		DATE_TRUNC('day',shipment_label_created_at) as fact_date, --was picked_by_carrier_at before
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_shipped,
		median(time_to_pickup) as time_to_pickup,
		percentile_cont(0.8) within group (order by time_to_pickup) as time_to_pickup_median_80,
		percentile_cont(0.95) within group (order by time_to_pickup) as time_to_pickup_median_95
		from allocations
		group by 1,2,3
		)
	, shipped as (
	select * from shipped_detailed 
	union all 
    select * from shipped_store
    UNION ALL 
	select * from shipped_total    
	)	
	, fd_detailed as (
	select 
		DATE_TRUNC('day',failed_delivery_at) as fact_date,
		 store_short,
		is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd,
		percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80,
		percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd_store as (
	select 
		DATE_TRUNC('day',failed_delivery_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd,
		percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80,
		percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd_total as (
	select 
		DATE_TRUNC('day',failed_delivery_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_failed_delivery,
		median(time_to_fd) as time_to_fd,
		percentile_cont(0.8) within group (order by time_to_fd) as time_to_fd_median_80,
		percentile_cont(0.95) within group (order by time_to_fd) as time_to_fd_median_95
		from allocations
		group by 1,2,3
		)
	, fd as (
	select * from fd_detailed 
	union all 
    select * from fd_store
    UNION ALL 
	select * from fd_total    
	)		
	, revocations_detailed as (
	select 
		DATE_TRUNC('day',revocation_date) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke,
		percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80,
		percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations_store as (
	select 
		DATE_TRUNC('day',revocation_date) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke,
		percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80,
		percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations_total as (
	select 
		DATE_TRUNC('day',revocation_date) as fact_date,
		 'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_revoked,
		median(time_to_revoke) as time_to_revoke,
		percentile_cont(0.8) within group (order by time_to_revoke) as time_to_revoke_median_80,
		percentile_cont(0.95) within group (order by time_to_revoke) as time_to_revoke_median_95
		from allocations
		group by 1,2,3
		)
	, revocations as (
	select * from revocations_detailed 
	union all 
    select * from revocations_store
    UNION ALL 
	select * from revocations_total    
	)	
	, delivered_detailed as (
	select 
		DATE_TRUNC('day',delivered_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend,
		percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80,
		percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)	
	, delivered_store as (
	select 
		DATE_TRUNC('day',delivered_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend,
		percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80,
		percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_all_total as (
	select 
		DATE_TRUNC('day',delivered_at) as fact_date,
		 'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_delivered,
		median(time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend,
		percentile_cont(0.8) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_80,
		percentile_cont(0.95) within group (order by time_to_deliver_excl_weekend) as time_to_deliver_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)	
	, delivered as (
	select * from delivered_detailed 
	union all 
    select * from delivered_store
    UNION ALL 
	select * from delivered_all_total    
	)		
	, delivered_detailed_total as (
	select 
		DATE_TRUNC('day',delivered_at) as fact_date,
		store_short,
		is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend,
		percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80,
		percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_store_total as (
	select 
		DATE_TRUNC('day',delivered_at) as fact_date,
		store_short,
		'Total' as  is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as net_ops_delivery_time_excl_weekend,
		percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80,
		percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
	, delivered_total_total as (
	select 
		DATE_TRUNC('day',delivered_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(case when assets_delivered_on_time then allocation_id end ) as assets_delivered_on_time,
		avg(net_ops_delivery_time_excl_weekend::decimal(10,2))::decimal(10,2) as avg_net_ops_delivery_time_excl_weekend,
		median(net_ops_delivery_time_excl_weekend) as net_ops_delivery_time_excl_weekend,
		percentile_cont(0.8) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_80,
		percentile_cont(0.95) within group (order by net_ops_delivery_time_excl_weekend) as total_delivery_time_excl_weekend_median_95
		from allocations
		group by 1,2,3
		)
		, delivered_total as (
	select * from delivered_detailed_total 
	union all 
    select * from delivered_store_total
    UNION ALL 
	select * from delivered_total_total  
	)	
    , return_shipping_label_created_detailed as (
	select 
		DATE_TRUNC('day',return_shipment_label_created_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset,
		percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80,
		percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	  , return_shipping_label_created_store as (
	select 
		DATE_TRUNC('day',return_shipment_label_created_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset,
		percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80,
		percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	  , return_shipping_label_created_total as (
	select 
		DATE_TRUNC('day',return_shipment_label_created_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping_label_created,
		median(time_to_use_asset) as time_to_use_asset,
		percentile_cont(0.8) within group (order by time_to_use_asset) as time_to_use_asset_median_80,
		percentile_cont(0.95) within group (order by time_to_use_asset) as time_to_use_asset_median_95
		from allocations
		group by 1,2,3
		)
	 , return_shipping_label_created as (
	select * from return_shipping_label_created_detailed 
	union all 
    select * from return_shipping_label_created_store
    UNION ALL 
	select * from return_shipping_label_created_total    
	)		
    , return_ship_detailed as (
	select 
		DATE_TRUNC('day',return_shipment_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack,
		percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80,
		percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship_store as (
	select 
		DATE_TRUNC('day',return_shipment_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack,
		percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80,
		percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship_total as (
	select 
		DATE_TRUNC('day',return_shipment_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_shipping,
		median(time_to_retern_pack) as time_to_retern_pack,
		percentile_cont(0.8) within group (order by time_to_retern_pack) as time_to_retern_pack_median_80,
		percentile_cont(0.95) within group (order by time_to_retern_pack) as time_to_retern_pack_median_95
		from allocations
		group by 1,2,3
		)
	, return_ship as (
	select * from return_ship_detailed 
	union all 
    select * from return_ship_store
    UNION ALL 
	select * from return_ship_total    
	)	
	, return_deliver_detailed as (
	select 
		DATE_TRUNC('day',return_delivery_date) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment,
		percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80,
		percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver_store as (
	select 
		DATE_TRUNC('day',return_delivery_date) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment,
		percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80,
		percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver_total as (
	select 
		DATE_TRUNC('day',return_delivery_date) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as return_delivered,
		median(time_to_retern_shipment) as time_to_retern_shipment,
		percentile_cont(0.8) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_80,
		percentile_cont(0.95) within group (order by time_to_retern_shipment) as time_to_retern_shipment_median_95
		from allocations
		group by 1,2,3
		)
	, return_deliver as (
	select * from return_deliver_detailed 
	union all 
    select * from return_deliver_store
    UNION ALL 
	select * from return_deliver_total    
	)		
	, refurbished_detailed as (
	select 
		DATE_TRUNC('day',refurbishment_start_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment,
		percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80,
		percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
	, refurbished_store as (
	select 
		DATE_TRUNC('day',refurbishment_start_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment,
		percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80,
		percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
	, refurbished_total as (
	select 
		DATE_TRUNC('day',refurbishment_start_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished,
		median(time_to_start_refurbishment) as time_to_start_refurbishment,
		percentile_cont(0.8) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_80,
		percentile_cont(0.95) within group (order by time_to_start_refurbishment) as time_to_start_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished as (
	select * from refurbished_detailed 
	union all 
    select * from refurbished_store
    UNION ALL 
	select * from refurbished_total    
	)	
		, refurbished_end_detailed as (
	select 
		DATE_TRUNC('day',refurbishment_end_at) as fact_date,
		store_short,
		is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment,
		percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80,
		percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end_store as (
	select 
		DATE_TRUNC('day',refurbishment_end_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment,
		percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80,
		percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end_total as (
	select 
		DATE_TRUNC('day',refurbishment_end_at) as fact_date,
		'Total' as store_short,
		'Total' as is_recirculated,
		count(allocation_id) as assets_refurbished_end,
		count(case when assets_refurbished_damaged is true then allocation_id end) as assets_refurbished_damaged,
		median(time_to_finish_refurbishment) as time_to_finish_refurbishment,
		percentile_cont(0.8) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_80,
		percentile_cont(0.95) within group (order by time_to_finish_refurbishment) as time_to_finish_refurbishment_median_95
		from allocations
		group by 1,2,3
		)
		, refurbished_end as (
	select * from refurbished_end_detailed 
	union all 
    select * from refurbished_end_store
    UNION ALL 
	select * from refurbished_end_total    
	)	
		select 
	a.fact_date, 
	a.store_short,
	a.is_recirculated,
	assets_allocated,
	assets_recirculated,
	time_to_allocate,
	time_to_allocate_median_80,
	time_to_allocate_median_95,
	assets_ready_to_ship,
	assets_shipped,
    time_to_deliver_excl_weekend,
    time_to_deliver_excl_weekend_median_80,
    time_to_deliver_excl_weekend_median_95,
    avg_net_ops_delivery_time_excl_weekend,
	total_delivery_time_excl_weekend,
	total_delivery_time_excl_weekend_median_80,
	total_delivery_time_excl_weekend_median_95,
	assets_delivered_on_time,
	time_to_prepare_to_ship,
	time_to_prepare_to_ship_median_80,
	time_to_prepare_to_ship_median_95,
	time_to_pickup,
	time_to_pickup_median_80,
	time_to_pickup_median_95,
	assets_failed_delivery,
		time_to_fd,
		time_to_fd_median_80,
		time_to_fd_median_95,
	revocations.assets_revoked,
		time_to_revoke,
		time_to_revoke_median_80,
		time_to_revoke_median_95,
	assets_delivered,
	return_shipping_label_created,
	time_to_use_asset,
	time_to_use_asset_median_80,
	time_to_use_asset_median_95,
	return_shipping,
		time_to_retern_pack,
		time_to_retern_pack_median_80,
		time_to_retern_pack_median_95,
	return_delivered,
	time_to_retern_shipment,
	time_to_retern_shipment_median_80,
	time_to_retern_shipment_median_95,
	assets_refurbished,
	assets_refurbished_end,
	time_to_start_refurbishment,
	time_to_start_refurbishment_median_80,
	time_to_start_refurbishment_median_95,
	time_to_finish_refurbishment,
	time_to_finish_refurbishment_median_80,
	time_to_finish_refurbishment_median_95,
	assets_refurbished_damaged
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
ORDER BY 1 DESC;

GRANT SELECT ON dwh.shipment_reporting_daily TO tableau;