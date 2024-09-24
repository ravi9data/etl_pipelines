-- staging_price_collection.ods_amazon source

create materialized view staging_price_collection.ods_amazon as
(
---------------AMAZON HISTORICAL---------------
with a as
	(
	select
	distinct
		a.*,
		b.product_name,
		b.brand 
	from staging_price_collection.historical_amazon a 
	left join master.variant  b on
	a.product_sku=b.product_sku
	--where a.scr_table = 'spv_used_asset_price_amazon'
	)
select
DISTINCT
'GERMANY' as region,
'AMAZON' as src,
itemid::int as item_id,
product_name as product_name, 
brand, 
product_sku as product_sku, 
null as amazon_name, 
asset_condition,
nullif(regexp_replace(a.price , '([^0-9.])', ''),'')::DECIMAL(38,2)  as price,
nullif(regexp_replace(a.price , '([^0-9.])', ''),'')::DECIMAL(38,2)  as price_in_euro,
'EUR' as currency,
substring(reporting_month::date,1,10)::date as reporting_month,
added_at,
'MOZENDA' AS crawler
from a
where price is not null
union all 
---------------AMAZON US---------------
select 
DISTINCT
'USA' as region,
'AMAZON' as src,
item_id::int as item_id,
product_name as product_name, 
brand, 
product_sku as product_sku, 
amazon_name, 
CASE
            WHEN main_ad_condition::text = 'New'::text THEN 'Neu'::text
            WHEN main_ad_condition::text ~~ '%Used - Like New%'::text THEN 'Wie neu'::text
            WHEN main_ad_condition::text ~~ '%Used - Very Good%'::text THEN 'Sehr gut'::text
            WHEN main_ad_condition::text ~~ '%Used - Good%'::text THEN 'Gut'::text
            WHEN main_ad_condition::text ~~ '%Used - Acceptable%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
regexp_replace(main_ad_price,'([^0-9.])','')::DECIMAL(38,2) as price,
regexp_replace(main_ad_price,'([^0-9.])','')::DECIMAL(38,2)*(select exchange_rate_eur::DECIMAL(38,2) from trans_dev.daily_exchange_rate where  date_=crawled_at::date) as price_in_euro,
'USD' as currency,
CASE 
 WHEN date_trunc('month',inserted_at::date) >= '2023-04-01'
  THEN 
    CASE 
	  WHEN date_trunc('month',crawled_at::date) = date_trunc('month',inserted_at::date)
        THEN substring(crawled_at::date,1,10)::date 
      ELSE NULL 
    END 
 ELSE substring(crawled_at::date,1,10)::date   
END AS reporting_month,
inserted_at as added_at,
'MOZENDA' AS crawler
from staging_price_collection.amazon_us au 
where main_ad_price<>'' and main_ad_price is not null 
AND main_ad_price NOT ILIKE  '%amazon%'
union all 
select 
DISTINCT
'USA' as region,
'AMAZON' as src,
item_id::int as item_id,
product_name as product_name, 
brand, 
product_sku as product_sku, 
amazon_name, 
CASE
            WHEN condition::text = 'New'::text THEN 'Neu'::text
            WHEN condition::text ~~ '%Used - Like New%'::text THEN 'Wie neu'::text
            WHEN condition::text ~~ '%Used - Very Good%'::text THEN 'Sehr gut'::text
            WHEN condition::text ~~ '%Used - Good%'::text THEN 'Gut'::text
            WHEN condition::text ~~ '%Used - Acceptable%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
regexp_replace(price,'([^0-9.])','')::DECIMAL(38,2) as price,
regexp_replace(price,'([^0-9.])','')::DECIMAL(38,2)*(select exchange_rate_eur::DECIMAL(38,2) from trans_dev.daily_exchange_rate where  date_=crawled_at::date) as price_in_euro,
'USD' as currency,
CASE 
 WHEN date_trunc('month',inserted_at::date) >= '2023-04-01'
  THEN 
    CASE 
	  WHEN date_trunc('month',crawled_at::date) = date_trunc('month',inserted_at::date)
        THEN substring(crawled_at::date,1,10)::date 
      ELSE NULL 
    END 
 ELSE substring(crawled_at::date,1,10)::date   
END AS reporting_month,
inserted_at as added_at,
'MOZENDA' AS crawler
from staging_price_collection.amazon_us au 
where price<>'' or price is not NULL
---------------------------------------
UNION ALL 
SELECT DISTINCT 
CASE WHEN amazon_domain iLIKE '%com%' THEN 'USA'
END as region,
'AMAZON' as src,
888888::int as item_id,
title as product_name, 
null as brand, 
custom_id as product_sku, 
null as amazon_name, 
CASE 
	WHEN condition__title SIMILAR TO 'New%|' THEN 'Neu'
	WHEN condition__title SIMILAR TO '%%Like New%|' THEN 'Wie neu'
	WHEN condition__title SIMILAR TO '%Very Good%|' THEN 'Sehr gut'
	WHEN condition__title SIMILAR TO '%Good%|' THEN 'Gut'
	WHEN condition__title SIMILAR TO '%Acceptable%|' THEN 'Akzeptabel'
ELSE 'Others' END AS asset_condition,
price__value::decimal(38,2) AS price,
price__value::decimal(38,2)*(select exchange_rate_eur::DECIMAL(38,2) from trans_dev.daily_exchange_rate where  date_=processed_at::date) as price_in_euro,
'USD' as currency,
CASE 
 WHEN  date_trunc('month',_airbyte_emitted_at::date) >= '2024-08-01'
  THEN 
    CASE 
	  WHEN date_trunc('month',processed_at::date) = date_trunc('month',_airbyte_emitted_at::date)
        THEN substring(processed_at::date,1,10)::date 
      ELSE NULL 
    END 
 ELSE substring(processed_at::date,1,10)::date   
END AS reporting_month,
date("_airbyte_emitted_at")::date as added_at,
'RAINFOREST' AS crawler
FROM staging_price_collection.rainforest_pricing_data sae   
WHERE 1=1
AND price__currency = 'USD'
AND substring(processed_at::date,1,10)::date >= '2023-11-01'
union all
---------------AMAZON EU--------------- 
SELECT
DISTINCT
'GERMANY' as region,
'AMAZON' as src,
item_id::int as item_id,
product_name as product_name, 
brand, 
product_sku as product_sku, 
amazon_name, 
CASE
            WHEN condition::text = 'Neu'::text THEN 'Neu'::text
            WHEN condition::text = 'New'::text THEN 'Neu'::text
            WHEN condition::text = '%Like New%'::text THEN 'Wie neu'::text
            WHEN condition::text = '%Very Good%'::text THEN 'Sehr gut'::text
            WHEN condition::text ~~ '%Gebraucht - Wie neu%'::text THEN 'Wie neu'::text
            WHEN condition::text ~~ '%Gebraucht - Sehr gut%'::text THEN 'Sehr gut'::text
            WHEN condition::text ~~ '%Gebraucht - Gut%'::text THEN 'Gut'::text
            WHEN condition::text ~~ '%Gebraucht - Akzeptabel%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
CASE WHEN price LIKE '€%' THEN NULLIF(regexp_replace(price,'([^0-9.])'),'')::DECIMAL(38,2)
ELSE NULLIF(regexp_replace(replace(replace(price,'.',''),',','.'),'([^0-9.])'),'')::DECIMAL(38,2) END AS price,
CASE WHEN price LIKE '€%' THEN NULLIF(regexp_replace(price,'([^0-9.])'),'')::DECIMAL(38,2)
ELSE NULLIF(regexp_replace(replace(replace(price,'.',''),',','.'),'([^0-9.])'),'')::DECIMAL(38,2) END as price_in_euro, 
'EUR' as currency,
CASE 
 WHEN date_trunc('month',inserted_at::date) >= '2023-04-01'
  THEN 
    CASE 
	  WHEN date_trunc('month',crawled_at::date) = date_trunc('month',inserted_at::date)
        THEN substring(crawled_at::date,1,10)::date 
      ELSE NULL 
    END 
 ELSE substring(crawled_at::date,1,10)::date   
END AS reporting_month,
inserted_at as added_at,
'MOZENDA' AS crawler
FROM staging_price_collection.amazon 
where price is not NULL
union all
SELECT 
DISTINCT
'GERMANY' as region,
'AMAZON' as src,
item_id::int as item_id,
product_name as product_name, 
brand, 
product_sku as product_sku, 
amazon_name, 
CASE
            WHEN main_ad_condition::text = 'Neu'::text THEN 'Neu'::text
            WHEN main_ad_condition::text = 'New'::text THEN 'Neu'::text
            WHEN main_ad_condition::text = 'Used - Like New'::text THEN 'Wie neu'::text
            WHEN main_ad_condition::text = 'Used - Very Good'::text THEN 'Sehr gut'::text
            WHEN main_ad_condition::text = 'Used - Good'::text THEN 'Gut'::text
            WHEN main_ad_condition::text ~~ '%Gebraucht - Wie neu%'::text THEN 'Wie neu'::text
            WHEN main_ad_condition::text ~~ '%Gebraucht - Sehr gut%'::text THEN 'Sehr gut'::text
            WHEN main_ad_condition::text ~~ '%Gebraucht - Gut%'::text THEN 'Gut'::text
            WHEN condition::text ~~ '%Gebraucht - Akzeptabel%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
CASE WHEN price LIKE '€%' THEN NULLIF(regexp_replace(main_ad_price,'([^0-9.])'),'')::DECIMAL(38,2)
		ELSE NULLIF(regexp_replace(replace(replace(main_ad_price,'.',''),',','.'),'([^0-9.])'),'')::DECIMAL(38,2) END as price,
CASE WHEN price LIKE '€%' THEN NULLIF(regexp_replace(main_ad_price,'([^0-9.])'),'')::DECIMAL(38,2)
		ELSE NULLIF(regexp_replace(replace(replace(main_ad_price,'.',''),',','.'),'([^0-9.])'),'')::DECIMAL(38,2) END as price_in_euro, 
'EUR' as currency,
CASE 
 WHEN date_trunc('month',inserted_at::date) >= '2023-04-01'
  THEN 
    CASE 
	  WHEN date_trunc('month',crawled_at::date) = date_trunc('month',inserted_at::date)
        THEN substring(crawled_at::date,1,10)::date 
      ELSE NULL 
    END 
 ELSE substring(crawled_at::date,1,10)::date   
END AS reporting_month,
inserted_at as added_at,
'MOZENDA' AS crawler
FROM staging_price_collection.amazon 
where main_ad_price is not null
union all --- scopas data 
SELECT
DISTINCT
'GERMANY' as region,
'AMAZON' as src,
9999::int as item_id,
input_product_name as product_name, 
input_brand as brand, 
input_product_sku as product_sku, 
result_listing_name as amazon_name, 
CASE
            WHEN result_condition::text = 'Neu'::text THEN 'Neu'::text
            WHEN result_condition::text ~~ '%Wie neu%'::text THEN 'Wie neu'::text
            WHEN result_condition::text ~~ '%Sehr gut%'::text THEN 'Sehr gut'::text
            WHEN result_condition::text ~~ '%Gut%'::text THEN 'Gut'::text
            WHEN result_condition::text ~~ '%Akzeptabel%'::text THEN 'Akzeptabel'::text
            ELSE 'Others'::text 
END AS asset_condition,
case when isnumeric(result_price) then result_price::DECIMAL(38,2) else null end AS price,
case when isnumeric(result_price) then result_price::DECIMAL(38,2) else null end as price_in_euro, 
'EUR' as currency,
substring(extracted_at::date,1,10)::date as reporting_month,
inserted_at as added_at,
'SCOPAS' AS crawler
FROM staging_price_collection.scopas_amazon_eu sae  
where price is not NULL
UNION ALL --RAINFORESt
SELECT DISTINCT 
CASE WHEN amazon_domain iLIKE '%de%' THEN 'GERMANY'
	WHEN amazon_domain iLIKE '%nl%' THEN 'NETHERLANDS'
	WHEN amazon_domain ILIKE '%es%' THEN 'SPAIN'
END as region,
'AMAZON' as src,
888888::int as item_id,
title as product_name, 
null as brand, 
custom_id as product_sku, 
null as amazon_name, 
CASE 
	WHEN condition__title SIMILAR TO '%Neu%|%Nieuw%|%Nuevo%|New%|' THEN 'Neu'
	WHEN condition__title SIMILAR TO '%Ausgezeichneter%|%excelente%|%Como nuevo%|%Als nieuw%|%Wie neu%|%Zeer goed%|%Uitstekende%|%Excellent%|%Like New%|' THEN 'Wie neu'
	WHEN condition__title SIMILAR TO '%Muy bueno%|%Sehr gut%|%Zeer goed%|%Very Good%|' THEN 'Sehr gut'
	WHEN condition__title SIMILAR TO '%buena%|%Bueno%|%Gut%|%Goede%|%Guter%|%Goed%|%Good%|' THEN 'Gut'
	WHEN condition__title SIMILAR TO '|%Aceptable%|%aceptable%|%Acceptabele%|%Akzeptabler%|%Akzeptabel%|%Acceptabel%|' THEN 'Akzeptabel'
ELSE 'Others' END AS asset_condition,
price__value::decimal(38,2) AS price,
price__value::decimal(38,2) AS price_in_euro,
'EUR' as currency,
CASE 
 WHEN  date_trunc('month',_airbyte_emitted_at::date) >= '2024-08-01'
  THEN 
    CASE 
	  WHEN date_trunc('month',processed_at::date) = date_trunc('month',_airbyte_emitted_at::date)
        THEN substring(processed_at::date,1,10)::date 
      ELSE NULL 
    END 
 ELSE substring(processed_at::date,1,10)::date   
END AS reporting_month,
date("_airbyte_emitted_at")::date as added_at,
'RAINFOREST' AS crawler
FROM staging_price_collection.rainforest_pricing_data sae   
WHERE 1=1
AND price__currency = 'EUR'
)

;