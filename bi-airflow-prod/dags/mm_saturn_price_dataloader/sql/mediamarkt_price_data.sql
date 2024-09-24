DROP TABLE IF EXISTS temp_mm_price_data;

create temp table temp_mm_price_data (like stg_external_apis_dl.mm_price_data_de);


insert into temp_mm_price_data
SELECT m.*
FROM stg_external_apis_dl.mm_price_data_de m
INNER join ods_production.variant v on v.ean::int8=m.ean
where v.ean SIMILAR to '[0-9]*'
AND isnumeric(price::TEXT) = TRUE;

begin transaction;

UPDATE stg_external_apis.mm_price_data SET is_current=0 , valid_to=current_timestamp AT TIME ZONE 'CEST'
WHERE id IN (SELECT id FROM temp_mm_price_data) AND valid_to IS NULL;

INSERT INTO stg_external_apis.mm_price_data
SELECT id::bigint,id::bigint as artikelnummer,title,brand,NULL AS category,color, weight,
	ean::bigint, price,crossedoutprice,NULL AS  lieferzeit , availability,
current_timestamp  AS valid_from, NULL::TIMESTAMP as valid_to, 1 AS is_current,product_eol_date::TIMESTAMP, mpn, GLOBAL_ID
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
truncate table stg_external_apis_dl.mm_price_data_de;


drop table if exists ods_external.spv_used_asset_price_mediamarkt_directfeed;
create table ods_external.spv_used_asset_price_mediamarkt_directfeed as
  select
        DISTINCT
        'MEDIAMARKT_DIRECT_FEED' as src,
         m.week_date as reporting_month,
        'Null' as itemid,
        m.product_sku,
        'EUR' as currency,
       m.price,
        'Neu' as asset_condition,
        m.product_eol_date,
        m.mpn,
        m.GLOBAL_ID
  FROM ods_external.mm_price_data  m
