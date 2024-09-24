DROP TABLE IF EXISTS temp_mm_price_data_es;

create temp table temp_mm_price_data_es (like stg_external_apis_dl.mm_price_data_es);

insert into temp_mm_price_data_es
SELECT m.*
FROM stg_external_apis_dl.mm_price_data_es m
INNER join ods_production.variant v on v.ean::int8=m.ean
where v.ean SIMILAR to '[0-9]*';

begin transaction;

UPDATE stg_external_apis.mm_price_data_es SET is_current=0 , valid_to=current_timestamp AT TIME ZONE 'CEST'
WHERE id IN (SELECT id FROM temp_mm_price_data_es) AND valid_to IS NULL;

INSERT INTO stg_external_apis.mm_price_data_es
SELECT id::bigint,id::bigint as artikelnummer,title,brand,category,color, weight,
	ean::bigint, price,crossedoutprice, lieferzeit, availability,
current_timestamp  AS valid_from, NULL::TIMESTAMP AS valid_to, 1 AS is_current, product_eol_date::TIMESTAMP, mpn, GLOBAL_ID
FROM temp_mm_price_data_es;

end transaction;



drop table temp_mm_price_data_es;

-- Load ODS table

drop table if exists ods_external.mm_price_data_es;

create table ods_external.mm_price_data_es as
with cte as (
  select
  	*
    , row_number() over (
      partition by date_trunc('week',valid_from),ean
      order by valid_from desc) as rn
  from stg_external_apis.mm_price_data_es
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
    is_current,
    product_eol_date,
    mpn,
    GLOBAL_ID
from cte m
INNER join ods_production.variant v on v.ean::int8=m.ean
left join ods_production.product p on p.product_id=v.product_id
where rn=1
and v.ean SIMILAR to '[0-9]*';


-- Clear delta table for next run
truncate table stg_external_apis_dl.mm_price_data_es;


drop table if exists ods_external.spv_used_asset_price_mediamarkt_directfeed_es;
create table ods_external.spv_used_asset_price_mediamarkt_directfeed_es as
  select
        DISTINCT
        'MEDIAMARKT_DIRECT_FEED' as src,
         m.week_date as reporting_month,
        'Null' as itemid,
        m.product_sku,
        'EUR' as currency,
       m.price,
        'Neu' AS asset_condition,
        product_eol_date,
        mpn,
        GLOBAL_ID
  FROM ods_external.mm_price_data_es m
