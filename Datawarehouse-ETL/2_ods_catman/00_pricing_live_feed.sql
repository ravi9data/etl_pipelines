drop table if exists skyvia.pricing_data_livefeed;
create table skyvia.pricing_data_livefeed as
with old_prices as (
		select distinct
		rp.id,
		LAST_value(op.price::decimal(10,2)) over (partition by op.rental_plan_id order by op.updated_at asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as higher_price
		from s3_spectrum_rds_dwh_api_production.rental_plans rp
		left join s3_spectrum_rds_dwh_api_production.old_prices op on rp.id = op.rental_plan_id
		where rp.active='True'
		and rp.store_id in ('1','20','45','51','126','637')
		 ),
	a as (
		select rp.*,
		op.higher_price,
		concat(concat(rp.rental_plan_price::decimal(10,2), ' , ' )::text, case when op.higher_price::text is not null then op.higher_price::text else ' ' end ) as rental_plan_price_higher_price
		from s3_spectrum_rds_dwh_api_production.rental_plans rp
		left join old_prices op on rp.id = op.id
		where rp.active='True'
		and rp.store_id in ('1','20','45','51','126','637')),
		rental_plans as (
	select
	product_id,
	store_id,
	listagg(minimum_term_months, '  |  ' ) within group (order by minimum_term_months ASC)  as rental_plans,
	max(case when minimum_term_months = 1 then rental_plan_price_higher_price end) as rental_plan_price_1_month,
	max(case when minimum_term_months = 3 then rental_plan_price_higher_price end) as rental_plan_price_3_months,
	max(case when minimum_term_months = 6 then rental_plan_price_higher_price end) as rental_plan_price_6_months,
	max(case when minimum_term_months = 12 then rental_plan_price_higher_price end) as rental_plan_price_12_months,
	max(case when minimum_term_months = 18 then rental_plan_price_higher_price end) as rental_plan_price_18_months,
	max(case when minimum_term_months = 24 then rental_plan_price_higher_price end) as rental_plan_price_24_months
	from a
	group by 1,2),
	other_stores  as (
			select product_id,
				  bool_or(case when store_id = 20 and active='True' then TRUE else FALSE  end) AS MM_ON,
				  bool_or(case when store_id = 192 and active='True' then TRUE else FALSE  end) AS MM_OFF,
				  bool_or(case when store_id = 45 and active='True' then TRUE else FALSE  end) AS CONRAD_ON,
				  bool_or(case when store_id = 382 and active='True' then TRUE else FALSE  end) AS CONRAD_OFF,
				  bool_or(case when store_id = 99 and active='True' then TRUE else FALSE  end) AS OTTO,
				  bool_or(case when store_id = 96 and active='True' then TRUE else FALSE  end) AS Gravis,
				  bool_or(case when store_id = 4 and active='True' then TRUE else FALSE  end) AS Grover_AT,
				  bool_or(case when store_id = 5 and active='True' then TRUE else FALSE  end) AS Grover_NL,
				  bool_or(case when store_id = 567 and active='True' then TRUE else FALSE  end) AS Aldi_Talk,
				  bool_or(case when store_id = 566 and active='True' then TRUE else FALSE  end) AS Welt_bild,
				  bool_or(case when store_id = 570 and active='True' then TRUE else FALSE  end) AS Com_Spot,
				  bool_or(case when store_id = 51 and active='True' then TRUE else FALSE  end) AS Saturn_ON,
				  bool_or(case when store_id = 515 and active='True' then TRUE else FALSE  end) AS Saturn_OFF,
				  bool_or(case when store_id = 126 and active='True' then TRUE else FALSE end) AS Business,
				  bool_or(case when store_id = 637 and active='True' then TRUE else FALSE end) AS MM_Spain
			from  s3_spectrum_rds_dwh_api_production.rental_plans
			group by 1)
			,final_data as (
select
v.variant_sku,
p.category_name,
p.subcategory_name,
v.product_brand,
p.product_sku,
p.product_name,
p.slug as pdp_url,
v.availability_state as Availability,
rp.store_id,
rp.rental_plans as rental_plans_DE,
rp.rental_plan_price_1_month,
rp.rental_plan_price_3_months,
rp.rental_plan_price_6_months,
rp.rental_plan_price_12_months,
rp.rental_plan_price_18_months,
rp.rental_plan_price_24_months,
os.*
from
ods_production.variant v
left join ods_production.product p on v.product_id = p.product_id
left join rental_plans rp on rp.product_id = v.product_id
left join other_stores os on os.product_id = v.product_id)
select * from final_data;


drop table if exists skyvia.mm_price_data;
create table skyvia.mm_price_data as
with a as (
select
	*,
	case when valid_from::date = CURRENT_DATE then True else False end as is_live_today,
	case when is_live_today is True then price else null end as vsku_price_today,
	avg(vsku_price_today::decimal(10,2) ignore nulls) over (partition by product_sku,week_date) as avg_sku_price_today,
    max(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as max_price_3_months,
    case when price::decimal(10,2) = max_price_3_months then week_date end as date_max_price_3_months,
    min(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as min_price_3_months,
    case when price::decimal(10,2) = min_price_3_months then week_date end as date_min_price_3_months,
    max(cast(price as decimal(10,2))) over (partition by variant_sku) as max_price,
    case when price::decimal(10,2) = max_price then week_date end as date_max_price,
    min(cast(price as decimal(10,2))) over (partition by variant_sku) as min_price,
    case when price::decimal(10,2) = min_price then week_date end as date_min_price
    from
		ods_external.mm_price_data
	order by week_date DESC )
	select
		week_date,
		valid_from,
		ean,
		artikelnummer,
		color,
		weight,
		crossedoutprice,
		variant_sku,
		product_sku,
		product_eol_date,
		mpn,
		global_id,
		price,
		is_current,
		is_live_today,
		vsku_price_today,
		avg_sku_price_today,
		max_price_3_months,
		LAST_value(date_max_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price_3months,
		min_price_3_months,
		LAST_value(date_min_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price_3months,
		max_price,
		LAST_value(date_max_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price,
		min_price,
		LAST_value(date_min_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price
		from a
		order by week_date DESC ;

drop table if exists skyvia.saturn_price_data;
create table skyvia.saturn_price_data as
with a as (
select
	*,
	case when valid_from::date = CURRENT_DATE then True else False end as is_live_today,
	case when is_live_today is True then price else null end as vsku_price_today,
	avg(vsku_price_today::decimal(10,2) ignore nulls) over (partition by product_sku,week_date) as avg_sku_price_today,
    max(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as max_price_3_months,
    case when price::decimal(10,2) = max_price_3_months then week_date end as date_max_price_3_months,
    min(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as min_price_3_months,
    case when price::decimal(10,2) = min_price_3_months then week_date end as date_min_price_3_months,
    max(cast(price as decimal(10,2))) over (partition by variant_sku) as max_price,
    case when price::decimal(10,2) = max_price then week_date end as date_max_price,
    min(cast(price as decimal(10,2))) over (partition by variant_sku) as min_price,
    case when price::decimal(10,2) = min_price then week_date end as date_min_price
    from
		ods_external.saturn_price_data
	order by week_date DESC )
	select
		week_date,
		valid_from,
		ean,
		artikelnummer,
		color,
		weight,
		crossedoutprice,
		variant_sku,
		product_sku,
		product_eol_date,
		price,
		is_current,
		is_live_today,
		vsku_price_today,
		avg_sku_price_today,
		max_price_3_months,
		LAST_value(date_max_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price_3months,
		min_price_3_months,
		LAST_value(date_min_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price_3months,
		max_price,
		LAST_value(date_max_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price,
		min_price,
		LAST_value(date_min_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price
		from a
		order by week_date DESC ;

drop table if exists skyvia.mm_price_data_es;
create table skyvia.mm_price_data_es as
with a as (
select
	*,
	case when valid_from::date = CURRENT_DATE then True else False end as is_live_today,
	case when is_live_today is True then price else null end as vsku_price_today,
	avg(vsku_price_today::decimal(10,2) ignore nulls) over (partition by product_sku,week_date) as avg_sku_price_today,
    max(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as max_price_3_months,
    case when price::decimal(10,2) = max_price_3_months then week_date end as date_max_price_3_months,
    min(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as min_price_3_months,
    case when price::decimal(10,2) = min_price_3_months then week_date end as date_min_price_3_months,
    max(cast(price as decimal(10,2))) over (partition by variant_sku) as max_price,
    case when price::decimal(10,2) = max_price then week_date end as date_max_price,
    min(cast(price as decimal(10,2))) over (partition by variant_sku) as min_price,
    case when price::decimal(10,2) = min_price then week_date end as date_min_price
    from
		ods_external.mm_price_data_es
	order by week_date DESC )
	select
		week_date,
		valid_from,
		ean,
		artikelnummer,
		color,
		weight,
		crossedoutprice,
		variant_sku,
		product_sku,
		product_eol_date,
		mpn,
		global_id,
		price,
		is_current,
		is_live_today,
		vsku_price_today,
		avg_sku_price_today,
		max_price_3_months,
		LAST_value(date_max_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price_3months,
		min_price_3_months,
		LAST_value(date_min_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price_3months,
		max_price,
		LAST_value(date_max_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price,
		min_price,
		LAST_value(date_min_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price
		from a
		order by week_date DESC ;

drop table if exists skyvia.mm_price_data_at;

UPDATE ods_external.mm_price_data_at
SET price = REPLACE(price,' EUR','');

create table skyvia.mm_price_data_at as
with a as (
select
	*,
	case when valid_from::date = CURRENT_DATE then True else False end as is_live_today,
	case when is_live_today is True then price else null end as vsku_price_today,
	avg(vsku_price_today::decimal(10,2) ignore nulls) over (partition by product_sku,week_date) as avg_sku_price_today,
    max(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as max_price_3_months,
    case when price::decimal(10,2) = max_price_3_months then week_date end as date_max_price_3_months,
    min(case when DATEDIFF('d',valid_from::date,CURRENT_DATE) <= 90 then cast(price as decimal(10,2)) end) over (partition by variant_sku)  as min_price_3_months,
    case when price::decimal(10,2) = min_price_3_months then week_date end as date_min_price_3_months,
    max(cast(price as decimal(10,2))) over (partition by variant_sku) as max_price,
    case when price::decimal(10,2) = max_price then week_date end as date_max_price,
    min(cast(price as decimal(10,2))) over (partition by variant_sku) as min_price,
    case when price::decimal(10,2) = min_price then week_date end as date_min_price
    from
		ods_external.mm_price_data_at
	order by week_date DESC )
	select
		week_date,
		valid_from,
		ean,
		artikelnummer,
		color,
		weight,
		crossedoutprice,
		variant_sku,
		product_sku,
		product_url,
	    product_eol_date,
		mpn,
		global_id,
		price,
		is_current,
		is_live_today,
		vsku_price_today,
		avg_sku_price_today,
		max_price_3_months,
		LAST_value(date_max_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price_3months,
		min_price_3_months,
		LAST_value(date_min_price_3_months ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price_3months,
		max_price,
		LAST_value(date_max_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_max_price,
		min_price,
		LAST_value(date_min_price ignore nulls) over (partition by variant_sku order by week_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_min_price
		from a
		order by week_date DESC ;

GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  skyvia;

GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  hightouch_pricing;
GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  redash_pricing;
GRANT SELECT ON skyvia.mm_price_data  TO  mahmoudmando;
GRANT SELECT ON skyvia.mm_price_data  TO GROUP mckinsey;
GRANT SELECT ON skyvia.saturn_price_data  TO GROUP mckinsey;
GRANT SELECT ON skyvia.mm_price_data_at  TO GROUP mckinsey;
GRANT SELECT ON skyvia.mm_price_data_es  TO GROUP mckinsey;
