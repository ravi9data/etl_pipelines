drop table if exists tmp_catalog_rental_plans_price_change;

create temp table tmp_catalog_rental_plans_price_change
as 
with inc as
(select
	plan_id as id,
	max(start_date) start_date
from
	ods_production.catalog_rental_plans_price_change crppc
group by
	id)
,crpc as (
select
	s.id as plan_id,
	minimum_term_months,
	product_sku ,
	rental_plan_price ,
	product_id,
	store_id ,
	version,
	price_change_reason ,
	price_change_tag ,
	old_price,
	item_condition_id ,
	TIMESTAMP 'epoch' + created_at::bigint * interval '1 second' as created_date,
	consumed_at::timestamp as updated_date,  /*Using consumed date as start date due to discrepancies in update_dt*/
	published_at,
	consumed_at ,
	row_number() over (partition by s.id
order by
	updated_date desc) idx
from
	stg_curated.catalog_rental_plans_price_change_v1 s
	left join inc
		on s.id = inc.id 
	where inc.id is null or s.consumed_at::date > inc.start_date::date
)
select
	plan_id,
	minimum_term_months,
	product_sku ,
	rental_plan_price ,
	product_id,
	store_id ,
	version,
	nullif(price_change_reason, 'null') as price_change_reason,
	nullif(price_change_tag, 'null') as price_change_tag ,
	nullif(old_price, 'null') as old_price,
	item_condition_id ,
	updated_date as start_date,
	case when idx =1 then '2099-12-01' else lag(updated_date) over (partition by plan_id order by updated_date desc) end end_date,
	case
		when idx = 1 then true
		else false
	end as is_active
from
	crpc;


/*Deactivate existing active data*/
update
	ods_production.catalog_rental_plans_price_change
set
	end_date = a.start_date,
	is_active = false
from
	(
	select
		plan_id,
		min(start_date) as start_date
	from
		tmp_catalog_rental_plans_price_change
	group by
		plan_id) a
where
	catalog_rental_plans_price_change.plan_id = a.plan_id
	and catalog_rental_plans_price_change.is_active = true;

/*Insert new data*/
insert into ods_production.catalog_rental_plans_price_change(plan_id,minimum_term_months,product_sku,rental_plan_price,product_id,store_id,version,price_change_reason,price_change_tag,old_price,item_condition_id,start_date,end_date,is_active)
select plan_id,minimum_term_months,product_sku,rental_plan_price,product_id,store_id,version,price_change_reason,price_change_tag,old_price,item_condition_id,start_date,end_date,is_active
from tmp_catalog_rental_plans_price_change;
