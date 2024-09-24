DROP TABLE if exists monitoring.oo4;

Create TABLE monitoring.oo4 as
Select
a.allocation_id,
a.allocation_status_original,
ast.asset_status_original,
a.asset_id,
ast.serial_number,
ast.asset_name,
ast.subcategory_name,
ast.initial_price as purchase_price,
a.is_last_allocation_per_asset,
a.allocated_at,
a.shipment_tracking_number,
a.wh_goods_order_id__c,
a.is_package_lost,
s.status as subscription_status,
sysdate as SYS_DATE,
datediff(day,a.allocated_at::timestamp without time zone,SYS_DATE) as days_since_readytoship,
c.customer_type
	from
ods_production.allocation a
left join ods_production.asset ast on ast.asset_id = a.asset_id
left join ods_production.subscription s on s.subscription_id = a.subscription_id
left join ods_production.customer c on c.customer_id = s.customer_id
where allocation_status_original = 'READY TO SHIP'
and s.status != 'CANCELLED'
order by allocated_at;

GRANT SELECT ON monitoring.oo4 TO tableau;
