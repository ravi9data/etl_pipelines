DROP TABLE IF EXISTS ods_spv_historical_test.tmp_union_sources;
CREATE TABLE ods_spv_historical_test.tmp_union_sources AS WITH a AS
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
      reporting_month::date < '2023-07-01'::date
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
      reporting_month::date < '2023-07-01'::date
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
      reporting_month::date < '2023-07-01'::date
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
      reporting_month::date < '2023-07-01'::date
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
      and reporting_month::date < '2023-07-01'::date
)
select
   *
from
   a
;


-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------



DROP TABLE IF EXISTS ods_spv_historical.union_sources_202306_updated_new;
CREATE TABLE ods_spv_historical.union_sources_202306_updated_new as
with a as (
SELECT *,
   case 
      when asset_condition in ('Neu') then 'NEW'
      when asset_condition in ('Sehr gut','Wie neu') then 'AGAN'  
   else null end as asset_condition_2
from ods_spv_historical_test.tmp_union_sources
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

DROP TABLE IF EXISTS ods_spv_historical.union_sources_202306_updated_2_new;
CREATE TABLE ods_spv_historical.union_sources_202306_updated_2_new as
SELECT a.*,b.asset_condition_2,price_LP,initial_price,
   case 
      when asset_condition_2 is not null and price < price_LP THEN 1
   else 0 end as outlier_removal 
from  ods_spv_historical_test.tmp_union_sources a 
left join ods_spv_historical.union_sources_202306_updated_new  b
on a.product_sku = b.product_sku
and a.asset_condition = b.asset_condition 
;



DROP TABLE IF EXISTS ods_spv_historical_test.tmp_union_sources;
CREATE TABLE ods_spv_historical_test.tmp_union_sources AS 
SELECT *
FROM  ods_spv_historical.union_sources_202306_updated_2_new
WHERE outlier_removal = 0;



-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------


-----Step1: Seprate EU---------------------

DROP TABLE IF EXISTS ods_spv_historical.union_sources_202306_eu_new;
CREATE TABLE ods_spv_historical.union_sources_202306_eu_new as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
WHERE
   region <> 'USA';
  
  
---------------------------------------------

DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_1;
CREATE TABLE ods_spv_historical_test.union_sources_1 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
WHERE
   region = 'GERMANY'
 AND src IN ('AMAZON','REBUY','MEDIAMARKT_DIRECT_FEED','SATURN_DIRECT_FEED');
 
  
INSERT INTO
   ods_spv_historical_test.union_sources_1 ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   SELECT
      'MR' as src,
      'GERMANY' as region,
      luxco_month::date - 1 as extract_date,
      luxco_month::date as reportmonth,
      99999999 as item_id,
      product_sku,
      "asset condition" as asset_condition,
      'EUR' as currency,
      final_price::double precision as price,
      final_price::double precision as price_in_euro
   FROM
      ods_spv_historical_test.new_luxco_manual_revisions mr --Engineers
   WHERE
      mr.capital_source IN 
      (
         'Grover Finance I GmbH',
         'SUSTAINABLE TECH RENTAL EUROPE II GMBH'
      )
      AND mr.luxco_month <= '2023-06-01';
     
     
DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_2;
CREATE TABLE ods_spv_historical_test.union_sources_2 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
WHERE 
   region = 'GERMANY'
 AND src IN ('EBAY');
 

DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_4;
CREATE TABLE ods_spv_historical_test.union_sources_4 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
where
   region = 'AUSTRIA'
 AND src IN ('EBAY');
 

DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_5;
CREATE TABLE ods_spv_historical_test.union_sources_5 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
where
   region = 'NETHERLANDS'
 AND src IN ('AMAZON');
 
DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_6;
CREATE TABLE ods_spv_historical_test.union_sources_6 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
where
   region = 'NETHERLANDS'
 AND src IN ('EBAY');
 
DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_7;
CREATE TABLE ods_spv_historical_test.union_sources_7 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
where
   region = 'SPAIN'
 AND src IN ('AMAZON');
 

DROP TABLE IF EXISTS ods_spv_historical_test.union_sources_8;
CREATE TABLE ods_spv_historical_test.union_sources_8 as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
where
   region = 'SPAIN'
 AND src IN ('EBAY');
 
--------------------------------------------
--Step1: Seprate US data
DROP TABLE IF EXISTS ods_spv_historical.union_sources_202306_us_new; --Q
CREATE TABLE ods_spv_historical.union_sources_202306_us_new as
SELECT
   *
FROM
   ods_spv_historical_test.tmp_union_sources
where
   region = 'USA';
   
  
insert into
   ods_spv_historical.union_sources_202306_us_new ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   SELECT
      'MR' as src,
      'USA'as region,
      luxco_month::date - 1 as extract_date,
      luxco_month::date as reportmonth,
      99999999 as item_id,
      product_sku,
      "asset condition" as asset_condition,
      'USD' as currency,
      final_price::double precision as price,
      final_price::double precision * coalesce(exchange_rate_eur::DECIMAL(38, 2), 1) as price_in_euro
   FROM
      ods_spv_historical_test.new_luxco_manual_revisions mr  --Engineers
      left join
         trans_dev.daily_exchange_rate er
         on '2023-06-01'::date - 1 = er.date_
         and mr.luxco_month <= '2023-06-01'
   where
      mr.capital_source in
      (
         'USA_test'
      )
;  