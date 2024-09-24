drop table if exists stg_external_apis.mozenda_amazon;

create table stg_external_apis.mozenda_amazon as
-- Condition and Price
SELECT 
'AMAZON' as src,
itemstatus, 
itemid as item_id,
productname as product_name, 
brand, 
productsku as product_sku, 
amazon, 
amazonname, 
CASE
            WHEN condition::text = 'Neu'::text THEN 'Neu'::text
            WHEN condition::text ~~ '%Gebraucht - Wie neu%'::text THEN 'Wie neu'::text
            WHEN condition::text ~~ '%Gebraucht - Sehr gut%'::text THEN 'Sehr gut'::text
            WHEN condition::text ~~ '%Gebraucht - Gut%'::text THEN 'Gut'::text
            WHEN condition::text ~~ '%Gebraucht - Akzeptabel%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
replace(replace(rtrim(price, ' €'),'.',''),',','.') as price,
linkbroken, 
'EUR' as currency,
zustand,
substring(created,1,10) as reporting_month,
CURRENT_TIMESTAMP as added_at
FROM stg_external_apis.temp_mozenda_amazon
where price is not null

union all

-- MainAdCondition and AdPrice
SELECT 
'AMAZON' as src,
itemstatus, 
itemid as item_id,
productname as product_name, 
brand, 
productsku as product_sku, 
amazon, 
amazonname, 
CASE
            WHEN mainadcondition::text = 'Neu'::text THEN 'Neu'::text
            WHEN mainadcondition::text ~~ '%Gebraucht - Wie neu%'::text THEN 'Wie neu'::text
            WHEN mainadcondition::text ~~ '%Gebraucht - Sehr gut%'::text THEN 'Sehr gut'::text
            WHEN mainadcondition::text ~~ '%Gebraucht - Gut%'::text THEN 'Gut'::text
            WHEN mainadcondition::text ~~ '%Gebraucht - Akzeptabel%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
replace(replace(REGEXP_REPLACE(mainadprice, '€.*$', ''),'.',''),',','.') as price,
linkbroken, 
'EUR' as currency,
zustand,
substring(created,1,10) as reporting_month,
CURRENT_TIMESTAMP as added_at
FROM stg_external_apis.temp_mozenda_amazon
where mainadcondition <> ''
;

delete from ods_external.spv_used_asset_price_amazon 
using stg_external_apis.mozenda_amazon
where ods_external.spv_used_asset_price_amazon.reporting_month = stg_external_apis.mozenda_amazon.reporting_month;

insert into ods_external.spv_used_asset_price_amazon
select
a.src,
a.item_id,
a.product_sku,
a.currency,
a.price,
a.asset_condition,
a.reporting_month::date,
a.added_at
from stg_external_apis.mozenda_amazon a;


