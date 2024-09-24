-- Delta load
DROP TABLE IF EXISTS temp_saturn_price_data;
create temp table temp_saturn_price_data (like stg_external_apis_dl.saturn_price_data_new);

insert into temp_saturn_price_data
select m.*
from stg_external_apis_dl.saturn_price_data_new m
inner join ods_production.variant v on v.ean::bigint =m.ean
where v.ean SIMILAR to '[0-9]*';

begin transaction;

update stg_external_apis.saturn_price_data set is_current=0 , valid_to=current_timestamp at time zone 'CEST'
where id in (select id from temp_saturn_price_data) and valid_to is null;

insert into stg_external_apis.saturn_price_data
select id::bigint,id::bigint as artikelnummer,title,brand,NULL AS category,color, weight,
	ean::bigint, price,crossedoutprice, NULL AS lieferzeit, availability,
current_timestamp as valid_from, null::timestamp as valid_to, 1 AS is_current,product_eol_date::timestamp
FROM temp_saturn_price_data;

end transaction;

drop table temp_saturn_price_data;

-- Load ODS table

drop table if exists ods_external.saturn_price_data;
create table ods_external.saturn_price_data as
with cte as (
  select
  	*
    , row_number() over (
      partition by date_trunc('week',valid_from),ean
      order by valid_from desc) as rn
  from stg_external_apis.saturn_price_data
)
select
	distinct
	date_trunc('week',valid_from)::date as week_date,
    valid_from,
	m.ean,
    artikelnummer,
    color, weight,crossedoutprice, lieferzeit, availability,
	v.variant_sku,
	p.product_sku,
	price,
    is_current,
    product_eol_date
from cte m
INNER join ods_production.variant v on v.ean::int8=m.ean
left join ods_production.product p on p.product_id=v.product_id
where rn=1
and v.ean SIMILAR to '[0-9]*';

truncate stg_external_apis_dl.saturn_price_data_new;
