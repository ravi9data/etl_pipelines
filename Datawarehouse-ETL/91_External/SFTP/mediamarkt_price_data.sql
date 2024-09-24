-- Delta load

create temp table temp_mm_price_data (like stg_external_apis_dl.mm_price_data_new); 

insert into temp_mm_price_data 
SELECT m.* 
FROM stg_external_apis_dl.mm_price_data_new m
INNER join ods_production.variant v on v.ean::int8=m.ean
where v.ean SIMILAR to '[0-9]*';

begin transaction;

UPDATE stg_external_apis.mm_price_data SET is_current=0 , valid_to=current_timestamp AT TIME ZONE 'CEST' 
WHERE id IN (SELECT id FROM temp_mm_price_data) AND valid_to IS NULL;

INSERT INTO stg_external_apis.mm_price_data
SELECT id,id as artikelnummer,title,brand,category,color, weight, 
	ean, price,crossedoutprice, lieferzeit, availability, 
current_timestamp  as valid_from, NULL::timestamp as valid_to, 1 AS is_current
FROM temp_mm_price_data;

end transaction;

drop table temp_mm_price_data;

-- Load ODS table

drop table if exists ods_external.mm_price_data;
create table ods_external.mm_price_data as 
with cte as (
  select
  	*
    , row_number() over (
      partition by date_trunc('week',valid_from),ean 
      order by valid_from desc) as rn
  from stg_external_apis.mm_price_data
)
select
	DISTINCT
	date_trunc('week',valid_from)::date as week_date,
    valid_from,
	m.ean, 
    artikelnummer,
    color, weight,crossedoutprice, lieferzeit, availability,
	v.variant_sku,
	p.product_sku,
	price, 
    is_current
from cte m
INNER join ods_production.variant v on v.ean::int8=m.ean
left join ods_production.product p on p.product_id=v.product_id
where rn=1
and v.ean SIMILAR to '[0-9]*';


-- Clear delta table for next run
truncate stg_external_apis_dl.mm_price_data_new;

--GRANT usage ON SCHEMA skyvia TO  skyvia;
--GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  skyvia;
