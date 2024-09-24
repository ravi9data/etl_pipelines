drop table if exists ods_production.asset_allocation_history;
create table ods_production.asset_allocation_history as 
with store as (
 SELECT DISTINCT a.asset_id,
    COALESCE(st.store_name, 'NOT AVAILABLE'::character varying) AS first_allocation_store_name,
    COALESCE(st.store_label, 'NOT AVAILABLE'::character varying::text) AS first_allocation_store_label,
    s.order_id
   FROM ods_production.allocation a
     LEFT JOIN ods_production.subscription s 
      ON a.subscription_id::text = s.subscription_id::text
     LEFT JOIN ods_production.store st
      ON s.store_id = st.id
  WHERE a.rank_allocations_per_asset = 1),
 shipped as (		
select allocation_id,
CASE when st.store_type = 'online' then
	 case when a.picked_by_carrier_at is not null then picked_by_carrier_at
	 	  when a.delivered_at is not null then delivered_at end
	 	  end as shipped_date_new
from ods_production.allocation a 
left join ods_production.subscription s on a.subscription_id = s.subscription_id
left join ods_production.store st on st.id = s.store_id)
select 
 a.asset_id,
 ast.asset_status_original,
 max(first_allocation_store_name) as first_allocation_store_name,
 max(first_allocation_store_label) as first_allocation_store_label,
 max(s.order_id) as first_allocation_order_id,
 count(distinct case when a.allocation_status_original::text not in ('CANCELLED', 'TO BE FIXED') or a.failed_delivery_at is not null then a.allocation_id end) as allocated_assets, 
 min(delivered_at)::date as first_asset_delivered,
 count(distinct case when delivered_at is not null then a.allocation_id end) as delivered_allocations,
 count(distinct case when shipped_date_new is not null then sh.allocation_id end) as shipped_allocations,
 count(distinct case 
       			when (a.delivered_at is not null)
       			and 
	       		(a.return_delivery_date is not null 
       			 or a.allocation_status_original = 'RETURNED'
       			 or a.is_last_allocation_per_asset = false 
       			 or (ast.asset_status_original in ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED') and a.failed_delivery_at is null))
       			 then a.allocation_id
    				end) as returned_allocations,
 count(distinct case when failed_delivery_at is not null then a.allocation_id end) as failed_delivery_allocations,
 count(distinct case when return_delivery_date_old is not null then a.allocation_id end) as returned_allocations_old,
GREATEST(delivered_allocations-returned_allocations,0) as outstanding_allocations,
     count(DISTINCT
        CASE
            WHEN returned_final_condition::text = 'DAMAGED'::text THEN a.allocation_id
            ELSE NULL::character varying
        END) AS damaged_allocations
from ods_production.allocation a 
left join ods_production.asset ast on a.asset_id = ast.asset_id 
left join store s on a.asset_id=s.asset_id
left join shipped sh on sh.allocation_id = a.allocation_id 
group by 1,2
;

GRANT SELECT ON ods_production.asset_allocation_history TO tableau;
