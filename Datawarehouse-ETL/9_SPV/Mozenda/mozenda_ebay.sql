drop table if exists stg_external_apis.spv_ebay;

create table stg_external_apis.spv_ebay as
select
 	'EBAY' as src,
 	created::date as reporting_month,
 	itemid  as item_id,
 	brand as brand,
 	"product sku" as product_sku,
 	condition2,
 	condition1,
 	price,
 	"product name" as ebay_product_name
 from stg_external_apis.temp_mozenda_ebay
where itemid::text <> '11042'::text  and price is not null;

delete from ods_external.spv_used_asset_price_ebay
using stg_external_apis.spv_ebay
where ods_external.spv_used_asset_price_ebay.reporting_month = stg_external_apis.spv_ebay.reporting_month;

insert into ods_external.spv_used_asset_price_ebay
SELECT
	a.src,
    a.item_id,
    a.product_sku,
    btrim("left"(a.price::text, 3)) AS currency,
        CASE
            WHEN a.price::text ~~ '%Preistendenz%'::text OR a.price::text ~~ '%bis%'::text
            or a.price::text='Nur Abholung: Kostenlos'THEN NULL::numeric
            ELSE
            CASE
                WHEN replace(btrim(replace(replace(a.price::text, '.'::text, ''::text), "left"(a.price::text, 3), ''::text)), ','::text, '.'::text) = ''::text THEN NULL::text
                ELSE replace(btrim(replace(replace(a.price::text, '.'::text, ''::text), "left"(a.price::text, 3), ''::text)), ','::text, '.'::text)
            END::numeric
        END AS price,
        CASE
            WHEN lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%sehr gut%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%sehr guter%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%near mint%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%top zustand%'::text THEN 'Sehr gut'::text
            WHEN lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%wie neu%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%w.neu%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%ausgezeichnet%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%ideal condition%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%excellent%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%neuwertig%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%fast neu%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%mint%'::text THEN 'Wie neu'::text
            WHEN lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%funktionsfähig%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%akzeptabel%'::text THEN 'Akzeptabel'::text
            WHEN lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%gut%'::text OR lower((((a.ebay_product_name::text || ' '::text) || a.condition1::text) || ' '::text) || a.condition2::text) ~~ '%guter zustand%'::text THEN 'Gut'::text
            ELSE 'Others'::text
        END AS asset_condition,
    a.reporting_month,
    current_timestamp as added_at
   FROM  stg_external_apis.spv_ebay a;
