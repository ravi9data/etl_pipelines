create materialized view staging_price_collection.ods_ebay as (
--EBAY HISTORICAL
with a as
	(
select
	distinct a.*,
	b.product_name,
	b.brand
from
	staging_price_collection.historical_ebay a
left join master.variant b on
		a.product_sku = b.product_sku
	--where a.scr_table = 'spv_used_asset_price_ebay'
	),
ebay_eu as (
select
	*,
	nullif(regexp_replace(price , '([^0-9.,])', ''), '') new_price_replace,
	position('bis' in new_price_replace) bis_position , 
	bis_position > 0 bis_exists,
	case
		when bis_exists then substring(new_price_replace from bis_position + 4)
		else new_price_replace
	end as newprice_1
from
	staging_price_collection.ebay_eu
where
	(price not like '%bis%'
		and price not like '%to%'
		and price not like '%$%' )
	and price like '%EUR%'
	and price NOT LIKE '%руб%'
 ) ,
ebay_us as (
select 
		 *
		 ,
	case
		when ebay_product_name ilike '%Never Used%'
			or ebay_product_name ilike '% unUsed%' then 'Neu'
			-----------------
			when
		ebay_product_name ilike '%Excellent%'
			or ebay_product_name ilike '% Barely%'
			or ebay_product_name ilike '% Slightly%'
			or ebay_product_name ilike '% Lightly%'
			or ebay_product_name ilike '% Hardly%'
			or ebay_product_name ilike '% Mint%'
			or ebay_product_name ilike '% PRISTINE%'
			or ebay_product_name ilike '% AMAZING%'
			or ebay_product_name ilike '% PERFECT%'
			or ebay_product_name ilike '% Ex condition%'
			or ebay_product_name ilike '% AWESOME%'
	then 'Wie neu'
			-----------------
			when 
		(ebay_product_name ilike '%Great%'
				or ebay_product_name ilike '% A+%'
				or ebay_product_name ilike '%New %'
				or ebay_product_name ilike '% Gently%'
				or ebay_product_name ilike '% very good%')
			and ebay_product_name not ilike '%New listing%' 
	then 'Sehr gut'
			------------------
			when 
		ebay_product_name ilike '%Heavy used%'
			or ebay_product_name ilike '% Working%'
			or ebay_product_name ilike '% Clean%'
			or ebay_product_name ilike '%Expect%' 
	then 'Akzeptabel'
			-----------------
			when 
		ebay_product_name ilike '%condition%'
			or ebay_product_name ilike '% used%'
			or ebay_product_name ilike '% Clean%'
			or ebay_product_name ilike '% Pre-owned%'
			or ebay_product_name ilike '% works but%'
			or ebay_product_name ilike '%but works%' 
	then 'Gut'
			else 'others'
		end as asset_condition
			,
		replace(price, '$', '')new_price_replace
			,
		case
			when position('to' in new_price_replace) <> 0 then position('to' in new_price_replace)
			when position('a ' in new_price_replace) <> 0 then position('a ' in new_price_replace)
			else 0
		end bis_position 
			,
		bis_position > 0 bis_exists
		,
		case
			when bis_exists then substring(new_price_replace from bis_position + 3)
			else new_price_replace
		end as newprice_1
	from
		staging_price_collection.ebay_us
	where
		ebay_product_name not ilike '%Locked%'
		and 
	  ebay_product_name not ilike '%Parts%'
		and
	  ebay_product_name not ilike '%Part %'
		and
	  ebay_product_name not ilike '%Cracked%'
		and 
	  ebay_product_name not ilike '%Replacement%'
		and 
	  ebay_product_name not ilike '%without smart%'
		and 
	  ebay_product_name not ilike '%Missing%'
		and
	  ebay_product_name not ilike '%no Battery%'
		and
	  ebay_product_name not ilike '%no Charger%'
		and 
	  ebay_product_name not ilike '%charging case only%'
		and (price not like '%bis%'
			and price not like '%to%' 
			AND price not LIKE 'COP%'
			AND price NOT LIKE '%руб%'
)
	  )
select
	distinct
	'GERMANY' as region,
	'EBAY' as src,
	itemid::int as item_id,
	product_name as product_name, 
	brand, 
	product_sku as product_sku, 
	asset_condition,
	nullif(regexp_replace(a.price , '([^0-9.])', ''), '')::numeric as price,
	nullif(regexp_replace(a.price , '([^0-9.])', ''), '')::numeric as price_in_euro,
	'EUR' as currency,
	substring(reporting_month::date, 1, 10)::date as reporting_month,
	coalesce (added_at::date,
	reporting_month::date) as added_at,
	'MOZENDA' AS crawler
from
	a
where
	price is not null
union all
--EBAY EU
select
	distinct
	'GERMANY' as region,
	'EBAY' as src,
	item_id::int as item_id,
	ebay_product_name as product_name, 
	brand, 
	product_sku as product_sku, 
	case
		when lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%sehr gut%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%sehr guter%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%near mint%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%top zustand%'::text then 'Sehr gut'::text
		when lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%wie neu%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%w.neu%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%ausgezeichnet%'::text
		or 
            lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%ideal condition%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%excellent%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%neuwertig%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%fast neu%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%mint%'::text then 'Wie neu'::text
		when lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%funktionsf?hig%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%akzeptabel%'::text then 'Akzeptabel'::text
		when lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%gut%'::text
		or lower((((a.ebay_product_name::text || ' '::text) || a.condition::text) || ' '::text) || a.condition::text) ~~ '%guter zustand%'::text then 'Gut'::text
		else 'Others'::text
	end as asset_condition,
	replace(replace(newprice_1, '.', ''), ',', '.')::DECIMAL(38,
	2) as price,
	replace(replace(newprice_1, '.', ''), ',', '.')::DECIMAL(38,
	2) as price_in_euro,
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
	substring(inserted_at::date, 1, 10)::date as added_at,
	'MOZENDA' AS crawler
from
	ebay_eu a
union all
--EBAY US
select 
	distinct
	'USA' as region,
	'EBAY' as src,
	item_id::int as item_id,
	ebay_product_name as product_name, 
	brand, 
	product_sku as product_sku, 
	/*CASE
	            WHEN condition::text = 'Neu'::text THEN 'Neu'::text
	            WHEN condition::text ~~ '%Gebraucht - Wie neu%'::text THEN 'Wie neu'::text
	            WHEN condition::text ~~ '%Gebraucht - Sehr gut%'::text THEN 'Sehr gut'::text
	            WHEN condition::text ~~ '%Gebraucht - Gut%'::text THEN 'Gut'::text
	            WHEN condition::text ~~ '%Gebraucht - Akzeptabel%'::text THEN 'Akzeptabel'::text
	            ELSE 'Others'::text 
	END AS */
	asset_condition,
	nullif(regexp_replace(newprice_1 , '([^0-9.])', ''), '')::DECIMAL(38,
	2) as price,
	nullif(regexp_replace(newprice_1 , '([^0-9.])', ''), '')::DECIMAL(38,
	2)*(
	select
		exchange_rate_eur::DECIMAL(38,
		2)
	from
		trans_dev.daily_exchange_rate
	where
		date_ = crawled_at::date ) as price_in_euro,
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
	substring(inserted_at ::date, 1, 10)::date as added_at,
	'MOZENDA' AS crawler
from
	ebay_us x
where
	price <> ''
union all
select 
	'GERMANY' as region,
	'EBAY' as src,
	'9999' as item_id,
	input_product_name as product_name,
	input_brand as brand,
	input_product_sku as product_sku,
	case
		when result_condition in('Brandneu', 'Brand New') then 'Neu'
		when result_condition in('Neu (Sonstige)', 'New (Other)', 'Hervorragend - Refurbished', 'Excellent - Refurbished') then 'Wie neu'
		when result_condition in('Sehr gut - Refurbished', 'Very Good - Refurbished', 'Zertifiziert - Refurbished', 'Certified - Refurbished') then 'Sehr gut'
		when result_condition in('Gut - Refurbished', 'Refurbished', 'Good - Refurbished') then 'Gut'
		when result_condition in ('Pre-Owned', 'Gebraucht') then 'Akzeptabel'
		else 'Others'
	end as asset_condition,
	result_price::decimal(38,
	2) as price,
	result_price::decimal(38,
	2) as price_in_euro,
	'EUR' as currency,
	extracted_at as reporting_month,
	inserted_at as added_at,
	'SCOPAS' AS crawler
from
	staging_price_collection.scopas_ebay_eu
where
	result_price is not NULL
UNION ALL  ---COUNTDOWN
select 
	CASE 
		WHEN customer_location iLIKE '%de%' THEN 'GERMANY'
		WHEN customer_location iLIKE '%nl%' THEN 'NETHERLANDS'
		WHEN customer_location ILIKE '%es%' THEN 'SPAIN'
		WHEN customer_location ILIKE '%at%' THEN 'AUSTRIA'
	END as region,
	'EBAY' as src,
	888888::int as item_id,
	title as product_name,
	null as brand,
	custom_id as product_sku,
	case
		when condition in('Brandneu', 'Brand New','Gloednieuw') THEN 'Neu' 
		when condition in('Neu (Sonstige)', 'New (Other)', 'Hervorragend - Refurbished', 'Excellent - Refurbished','Nieuw (anders)','Nuevo (de otro tipo)','Totalmente nuevo') then 'Wie neu' 
		when condition in('Sehr gut - Refurbished', 'Very Good - Refurbished', 'Zertifiziert - Refurbished', 'Certified - Refurbished') then 'Sehr gut'
		when condition in('Gut - Refurbished', 'Refurbished', 'Good - Refurbished','Reacondicionado','Opgeknapt') then 'Gut'
		when condition in ('Pre-Owned', 'Gebraucht','Abierto, sin usar','Tweedehands','Usado') then 'Akzeptabel'
		WHEN CONDITION IS NULL THEN 
			CASE WHEN title ILIKE '%NEU%' AND title NOT ILIKE '%WIE NEU%'  THEN 'Neu'
				  WHEN title ILIKE '%WIE NEU%' then 'Wie neu'
				  WHEN title ILIKE '%Sehr gut%' then 'Sehr gut'
				  WHEN title ILIKE '%Gut%' AND title NOT ILIKE '%Sehr gut%' then 'Gut'
				  WHEN title ILIKE '%Akzeptabel%' then 'Akzeptabel'
		   END 
		else 'Others'
	end as asset_condition,
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
	'COUNTDOWN' AS crawler
from
	staging_price_collection.countdown_pricing_data cpd
WHERE
	1=1
AND "condition" NOT IN ('Nur Ersatzteile','Alleen onderdelen','Parts Only','Solo piezas')--Removing all spare parts prices ;
);