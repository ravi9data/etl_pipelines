refresh materialized view staging_price_collection.ods_amazon;
refresh materialized view staging_price_collection.ods_ebay ;


DROP TABLE IF EXISTS ods_spv_historical.tmp_union_sources;
CREATE TABLE ods_spv_historical.tmp_union_sources AS WITH a AS
(
   SELECT distinct
      a.src,
      a.region,
      a.reporting_month::date as extract_date,
      (
         DATEADD('month', 1, date_trunc('month', a.reporting_month::date)::date)::date
      )
      as reporting_month,
      coalesce(a.item_id, 0) item_id,
      a.product_sku,
      a.asset_condition,
      a.currency,
      price,
      price_in_euro
   FROM
      staging_price_collection.ods_amazon a
   WHERE
      reporting_month::date < '{{ params.first_day_of_month }}'::date
   UNION ALL
   SELECT distinct
      e.src,
      e.region,
      e.reporting_month::date as extract_date,
      (
         DATEADD( 'month', 1, date_trunc('month', e.reporting_month::date)::date)::date
      )
      as reporting_month,
      coalesce(e.item_id, 0) item_id,
      e.product_sku,
      e.asset_condition,
      e.currency,
      price,
      price_in_euro
   FROM
      staging_price_collection.ods_ebay e
   WHERE
      reporting_month::date < '{{ params.first_day_of_month }}'::date
   UNION ALL
   select distinct
      r.src,
      r.region,
      r.reporting_month::date as extract_date,
      (
         DATEADD( 'month', 1, date_trunc('month', r.reporting_month::date)::date)::date
      )
      as reporting_month,
      coalesce(r.item_id, 0) item_id,
      r.product_sku,
      r.asset_condition,
      r.currency,
      price::decimal(10, 2) as price,
      price_in_euro
   FROM
      staging_price_collection.ods_rebuy r
   WHERE
      reporting_month::date < '{{ params.first_day_of_month }}'::date
   UNION all
   select distinct
      src,
      region,
      reporting_month::date as extract_date,
      (
         DATEADD( 'month', 1, date_trunc('month', reporting_month::date)::date)::date
      )
      as reporting_month,
      coalesce(item_id, 0) item_id,
      product_sku,
      'Neu' as asset_condition,
      currency,
      price::decimal(10, 2) as price,
      price_in_euro
   FROM
      staging_price_collection.ods_mediamarkt
   WHERE
      reporting_month::date < '{{ params.first_day_of_month }}'::date
   UNION ALL
   select distinct
      'SATURN_DIRECT_FEED' as src,
      region,
      week_date::date as extract_date,
      (
         DATEADD( 'month', 1, date_trunc('month', week_date::date)::date)::date
      )
      as reporting_month,
      coalesce(item_id, 0) item_id,
      product_sku,
      'Neu' as asset_condition,
      'EUR' as currency,
      price::decimal(10, 2) as price,
      price_in_euro
   FROM
      staging_price_collection.ods_saturn
   WHERE
      price > 0
      and reporting_month::date < '{{ params.first_day_of_month }}'::date
)
select
   *
from
   a
;


-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------



DROP TABLE IF EXISTS ods_spv_historical.union_sources_{{ params.tbl_suffix }}_updated;
CREATE TABLE ods_spv_historical.union_sources_{{ params.tbl_suffix }}_updated as
with a as (
SELECT *,
   case 
      when asset_condition in ('Neu') then 'NEW'
      when asset_condition in ('Sehr gut','Wie neu') then 'AGAN'  
   else null end as asset_condition_2
from ods_spv_historical.tmp_union_sources
)
SELECT 
   a.product_sku,
   a.asset_condition,
   a.asset_condition_2,
   min(b.initial_price) AS initial_price,
   0.2*min(b.initial_price)  AS price_LP
from a
left join master.asset b
on a.product_sku = b.product_sku
and a.asset_condition_2 = b.asset_condition
group by 1,2,3
;

DROP TABLE IF EXISTS ods_spv_historical.union_sources_{{ params.tbl_suffix }}_updated_2;
CREATE TABLE ods_spv_historical.union_sources_{{ params.tbl_suffix }}_updated_2 as
SELECT a.*,b.asset_condition_2,price_LP,initial_price,
   case 
      when asset_condition_2 is not null and price < price_LP THEN 1
   else 0 end as outlier_removal 
from  ods_spv_historical.tmp_union_sources a 
left join ods_spv_historical.union_sources_{{ params.tbl_suffix }}_updated  b
on a.product_sku = b.product_sku
and a.asset_condition = b.asset_condition 
;



DROP TABLE IF EXISTS ods_spv_historical.tmp_union_sources;
CREATE TABLE ods_spv_historical.tmp_union_sources AS 
SELECT *
FROM  ods_spv_historical.union_sources_{{ params.tbl_suffix }}_updated_2
WHERE outlier_removal = 0;



-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------


-----Step1: Seprate EU into two tables--Germany and all EU countries-------------------

DROP TABLE IF EXISTS ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu;
CREATE TABLE ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu as
SELECT
   *
FROM
   ods_spv_historical.tmp_union_sources
WHERE
   region = 'GERMANY';
  
---------------------
---------------------

  
DROP TABLE IF EXISTS ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu_all;
CREATE TABLE ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu_all as
SELECT
   *
FROM
   ods_spv_historical.tmp_union_sources
WHERE
   region <> 'USA';
  
  
---------------------------------------------
 
  
INSERT INTO
   ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   SELECT
      'MR' as src,
      'GERMANY' as region,
      luxco_month::date - 1 as extract_date,
      luxco_month::date as reportmonth,
      99999999 as item_id,
      product_sku,
      "asset_condition" as asset_condition,
      'EUR' as currency,
      final_price::double precision as price,
      final_price::double precision as price_in_euro
   FROM
      ods_spv_historical.luxco_manual_revisions mr --Engineers
   WHERE
      mr.capital_source IN 
      (
         'Grover Finance I GmbH',
         'SUSTAINABLE TECH RENTAL EUROPE II GMBH'
      )
      AND mr.luxco_month <= '{{ params.date_for_depreciation }}';
     
     
     
   
---------------------------------------------
 
  
INSERT INTO
   ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu_all ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   SELECT
      'MR' as src,
      'GERMANY' as region,
      luxco_month::date - 1 as extract_date,
      luxco_month::date as reportmonth,
      99999999 as item_id,
      product_sku,
      "asset_condition" as asset_condition,
      'EUR' as currency,
      final_price::double precision as price,
      final_price::double precision as price_in_euro
   FROM
      ods_spv_historical.luxco_manual_revisions mr --Engineers
   WHERE
      mr.capital_source IN 
      (
         'Grover Finance I GmbH',
         'SUSTAINABLE TECH RENTAL EUROPE II GMBH'
      )
      AND mr.luxco_month <= '{{ params.date_for_depreciation }}';    
 
  ---------------------===================================================   
  ---------------------===================================================
  ---------------------===================================================																																		
	
 
--------------------------------------------
--Step1: Seprate US data
DROP TABLE IF EXISTS ods_spv_historical.union_sources_{{ params.tbl_suffix }}_us; --Q
CREATE TABLE ods_spv_historical.union_sources_{{ params.tbl_suffix }}_us as
SELECT
   *
FROM
   ods_spv_historical.tmp_union_sources
where
   region = 'USA';
   
  
insert into
   ods_spv_historical.union_sources_{{ params.tbl_suffix }}_us ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   SELECT
      'MR' as src,
      'USA'as region,
      luxco_month::date - 1 as extract_date,
      luxco_month::date as reportmonth,
      99999999 as item_id,
      product_sku,
      "asset_condition" as asset_condition,
      'USD' as currency,
      final_price::double precision as price,
      final_price::double precision * coalesce(exchange_rate_eur::DECIMAL(38, 2), 1) as price_in_euro
   FROM
      ods_spv_historical.luxco_manual_revisions mr  --Engineers
      left join
         trans_dev.daily_exchange_rate er
         on '{{ params.date_for_depreciation }}'::date - 1 = er.date_
         and mr.luxco_month <= '{{ params.date_for_depreciation }}'
   where
      mr.capital_source in
      (
         'USA_test'
      )
;  