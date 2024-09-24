drop table if exists stg_external_apis.spv_rebuy;

create table stg_external_apis.spv_rebuy as
select
 	'REBUY' as src,
 	created::date as reporting_month,
 	itemid as item_id,
 	"condition",
 	name,
 	cd,
 	price,
 	"product sku" as product_sku,
 	null as rebuy_link,
 	brand,
 	"product name"
 from stg_external_apis.temp_mozenda_rebuy
where price is not null;

delete from ods_external.spv_used_asset_price_rebuy
using stg_external_apis.spv_rebuy
where ods_external.spv_used_asset_price_rebuy.reporting_month = stg_external_apis.spv_rebuy.reporting_month;

insert into  ods_external.spv_used_asset_price_rebuy
SELECT
	a.src,
    a.item_id,
    a.product_sku,
        CASE
            WHEN btrim("right"(a.price::text, 1)) = '€'::text THEN 'EUR'::text
            ELSE NULL::text
        END AS currency,
        CASE
            WHEN btrim(replace(btrim(replace(a.price::text, '€'::text, ''::text)), ','::text, '.'::text)) = ''::text THEN NULL
            ELSE btrim(replace(btrim(replace(a.price::text, '€'::text, ''::text)), ','::text, '.'::text))
        END AS price,
        CASE
            WHEN a.condition::text = 'Wie neu'::text THEN 'Wie neu'::text
            WHEN a.condition::text = 'Sehr gut'::text THEN 'Sehr gut'::text
            WHEN a.condition::text = 'Gut'::text THEN 'Gut'::text
            WHEN a.condition::text = 'Stark genutzt'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text
        END AS asset_condition,
    a.reporting_month,
       current_timestamp as added_at
   FROM stg_external_apis.spv_rebuy a;

