drop table if exists dm_operations.assets_booked;
create table dm_operations.assets_booked as
with 
first_wh as (
	select 
		asset_id , 
		warehouse , 
		row_number() over (partition by asset_id order by date asc ) rn
	from master.asset_historical 
	where purchase_request_item_id is not null ),
first_wh_l as (
	select 
		asset_id ,
		warehouse 
	from first_wh
	where rn = 1
	)	
select 
	w.warehouse ,
	case when w.warehouse in ('office_us', 'ups_softeon_us_kylse') then 'US' else 'EU' end as region,
	a.category_name ,
	a.subcategory_name ,
	a.variant_sku ,
	a.supplier ,
	a.brand,
	date_trunc('day', a.created_at) as created_at , 
	date_trunc('day', r.purchased_date) as purchased_date,
	a.request_id ,
	a.purchase_request_item_id,
	r.purchase_order_number,
	case when r.purchase_order_number like '%BS%' then 'b-stock' else 'new' end as new_bstock,
	sum(a.initial_price) as total_initial_price,
	count(distinct a.asset_id) booked_asset
from master.asset a 
left join first_wh_l w 
	on a.asset_id = w.asset_id
left join ods_production.purchase_request_item r 
	on a.purchase_request_item_id = r.purchase_request_item_id
where a.purchase_request_item_id is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12;

GRANT SELECT ON dm_operations.assets_booked TO tableau;
