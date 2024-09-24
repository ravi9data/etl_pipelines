DROP TABLE IF EXISTS temp_mm_price_data_at;

create temp table temp_mm_price_data_at (like stg_external_apis_dl.mm_at_price_feed);

insert into temp_mm_price_data_at
SELECT m.*
FROM stg_external_apis_dl.mm_at_price_feed m
--INNER join ods_production.variant v on v.ean::int8=m.ean
--where v.ean SIMILAR to '[0-9]*'
;

begin transaction;

UPDATE stg_external_apis.mm_price_data_at SET is_current=0 , valid_to=current_timestamp AT TIME ZONE 'CEST'
WHERE id IN (SELECT id FROM temp_mm_price_data_at) AND valid_to IS NULL;

INSERT INTO stg_external_apis.mm_price_data_at
SELECT id::bigint,id::bigint as artikelnummer,title,brand,category,color, weight,
	ean::bigint, replace(price, ' EUR','') as price,crossedoutprice, lieferzeit, availability,
current_timestamp  AS valid_from, NULL::TIMESTAMP AS valid_to, 1 AS is_current,deeplink AS product_url, product_eol_date::TIMESTAMP, mpn, GLOBAL_ID
FROM temp_mm_price_data_at;

end transaction;



drop table temp_mm_price_data_at;

-- Load ODS table

drop table if exists ods_external.mm_price_data_at;

create table ods_external.mm_price_data_at as
with cte as (
  select
  	*
    , row_number() over (
      partition by date_trunc('week',valid_from),ean
      order by valid_from desc) as rn
  from stg_external_apis.mm_price_data_at
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
    product_url,
    product_eol_date, 
    mpn, 
    GLOBAL_ID
from cte m
left join ods_production.variant v on v.ean=m.ean
left join ods_production.product p on p.product_id=v.product_id
where rn=1
--and v.ean SIMILAR to '[0-9]*'
;



drop table if exists ods_external.spv_used_asset_price_mediamarkt_directfeed_at;
create table ods_external.spv_used_asset_price_mediamarkt_directfeed_at as
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
  from ods_external.mm_price_data_at  m
  where m.product_sku is not null;
