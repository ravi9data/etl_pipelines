drop table if exists ods_spv_historical.union_sources_202407;
create table ods_spv_historical.union_sources_202407 as
(
   select
      *
   from
      ods_spv_historical.union_sources_202407_eu
   union
   select
      *
   from
      ods_spv_historical.union_sources_202407_eu_all
   union all
   select
      *
   from
      ods_spv_historical.union_sources_202407_us
)
;
----inserting data into union source is done at final step due to MR_
------------------------------------
DROP TABLE IF EXISTS  ods_spv_historical.spv_used_asset_price_202407_eu ;
CREATE TABLE  ods_spv_historical.spv_used_asset_price_202407_eu  AS
SELECT *
FROM ods_spv_historical.spv_used_asset_price_1
WHERE product_sku IN 
  (SELECT product_sku
   FROM ods_spv_historical.all_eu_prioritization_new_202407
   WHERE price_rank = 1
     AND code = 1);

INSERT INTO ods_spv_historical.spv_used_asset_price_202407_eu
SELECT *
FROM ods_spv_historical.spv_used_asset_price_2
WHERE product_sku NOT IN 
  (SELECT product_sku
   FROM ods_spv_historical.all_eu_prioritization_new_202407
   WHERE price_rank = 1
     AND code = 1)
;

----------------------------------
--Union Source of spv_used_asset_price_
drop table if exists ods_spv_historical.spv_used_asset_price_202407;
create table ods_spv_historical.spv_used_asset_price_202407 as
(
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_202407_eu
   union all
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_202407_us
)
;

delete
from
   ods_spv_historical.spv_used_asset_price
where
   reporting_date = '2024-07-31';
insert into
   ods_spv_historical.spv_used_asset_price
   select
      reporting_date,
      src,
      extract_date,
      reporting_month,
      item_id,
      product_sku,
      asset_condition,
      currency,
      price,
      avg_mm_price,
      avg_pp_price,
      ref_price,
      coeff,
      median_coeff,
      max_available_date,
      price_rank,
      price_rank_month
   FROM
      ods_spv_historical.spv_used_asset_price_202407 ; 
     
     
-- STEP 3
 ------------------------------------
DROP TABLE IF EXISTS  ods_spv_historical.spv_used_asset_price_master_202407_eu;
CREATE TABLE  ods_spv_historical.spv_used_asset_price_master_202407_eu AS 
SELECT 
  nvl(a.reporting_date,b.reporting_date) AS reporting_date,
  nvl(a.product_sku,b.product_sku) AS product_sku, 
  nvl(a.asset_condition,b.asset_condition) AS asset_condition,
  CASE 
     WHEN 
        b.asset_condition = 'Wie neu' 
        AND b.months_since_last_price = 0 
     THEN  b.avg_price
     ELSE a.avg_price END AS avg_price,   
  CASE 
    WHEN 
       b.asset_condition = 'Wie neu' 
       AND b.months_since_last_price = 0 
    THEN  b.max_available_price_date
     ELSE a.max_available_price_date 
  END AS max_available_price_date,  
  nvl(a.months_since_last_price,b.months_since_last_price) AS months_since_last_price, 
  CASE
    WHEN 
       b.asset_condition = 'Wie neu' 
       AND b.months_since_last_price = 0 
    THEN  b.final_market_price
     ELSE a.final_market_price 
  END AS final_market_price
FROM 
ods_spv_historical.spv_used_asset_price_master_2 a
LEFT JOIN ods_spv_historical.spv_used_asset_price_master_1 b
ON a.reporting_date  = b.reporting_date 
AND a.product_sku = b.product_sku 
AND a.asset_condition = b.asset_condition 
;
----------------------------------    
     
--Union Source of spv_used_asset_price_master_
drop table if exists ods_spv_historical.spv_used_asset_price_master_202407;
create table ods_spv_historical.spv_used_asset_price_master_202407 as
(
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_master_202407_eu
   union all
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_master_202407_us
)
;

delete
from
   ods_spv_historical.spv_used_asset_price_master
where
   reporting_date = '2024-07-31';
insert into
   ods_spv_historical.spv_used_asset_price_master
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_master_202407; 
     
     
     
     
     
-- STEP 4
--Union Source of price_per_condition_
drop table if exists ods_spv_historical.price_per_condition_202407;
create table ods_spv_historical.price_per_condition_202407 as
(
   select
      *
   from
      ods_spv_historical.price_per_condition_202407_eu
   union all
   select
      *
   from
      ods_spv_historical.price_per_condition_202407_us
)
;


delete
from
   ods_spv_historical.price_per_condition
where
   reporting_date = '2024-07-31';
insert into
   ods_spv_historical.price_per_condition
   SELECT
      reporting_date::date,
      product_sku,
      neu_price,
      as_good_as_new_price,
      sehr_gut_price,
      gut_price,
      akzeptabel_price,
      neu_price_before_discount,
      as_good_as_new_price_before_discount,
      sehr_gut_price_before_discount,
      gut_price_before_discount,
      akzeptabel_price_before_discount,
      m_since_neu_price,
      m_since_as_good_as_new_price,
      m_since_sehr_gut_price,
      m_since_gut_price,
      m_since_akzeptabel_price,
      new_price_standardized,
      agan_price_standardized,
      agan_price_standardized_before_discount,
      m_since_agan_price_standardized,
      used_price_standardized,
      used_price_standardized_before_discount,
      m_since_used_price_standardized
   FROM
      ods_spv_historical.price_per_condition_202407; 
     
     
     
-- Step 5
--Union Source of asset_market_value_
drop table if exists ods_spv_historical.asset_market_value_202407;
create table ods_spv_historical.asset_market_value_202407 as
(
   select
      *
   from
      ods_spv_historical.asset_market_value_202407_eu
   union all
   select
      *
   from
      ods_spv_historical.asset_market_value_202407_us
)
;


delete
from
   ods_spv_historical.asset_market_value
where
   reporting_date = '2024-07-31';
insert into
   ods_spv_historical.asset_market_value
   SELECT
      reporting_date::Date,
      asset_id,
      residual_value_market_price,
      residual_value_market_price_label,
      average_of_sources_on_condition_this_month,
      m_since_last_valuation_price,
      valuation_method,
      valuation_1,
      valuation_2,
      valuation_3,
      average_of_sources_on_condition_last_available_price
   FROM
      ods_spv_historical.asset_market_value_202407; 

--Step 6
-- update mr prices in eu and us report master tables
     
update
   ods_production.spv_report_master_202407_eu
set
   final_price = lmr.final_price::double precision,
   m_since_last_valuation_price = 0,
   valuation_method = '12.1- (c) - (i),(ii)'
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_202407_eu.product_sku = lmr.product_sku
   and spv_report_master_202407_eu.final_price <> 0
   and
   (
(spv_report_master_202407_eu.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_202407_eu.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '2024-08-01'
   and spv_report_master_202407_eu.capital_source_name ilike lmr.capital_source ;
update
   ods_production.spv_report_master_202407_us
set
   final_price = lmr.final_price::double precision,
   m_since_last_valuation_price = 0
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_202407_us.product_sku = lmr.product_sku
   and spv_report_master_202407_us.final_price <> 0
   and
   (
(spv_report_master_202407_us.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_202407_us.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '2024-08-01'
   and spv_report_master_202407_us.capital_source_name ilike lmr.capital_source;
   
   
   
update
   ods_production.spv_report_master_202407_eu
set
   final_price_without_written_off = lmr.final_price::double precision
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_202407_eu.product_sku = lmr.product_sku
   and spv_report_master_202407_eu.final_price_without_written_off <> 0
   and
   (
(spv_report_master_202407_eu.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_202407_eu.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '2024-08-01'
   and spv_report_master_202407_eu.capital_source_name ilike lmr.capital_source ;
update
   ods_production.spv_report_master_202407_us
set
   final_price_without_written_off = lmr.final_price::double precision
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_202407_us.product_sku = lmr.product_sku
   and spv_report_master_202407_us.final_price_without_written_off <> 0
   and
   (
(spv_report_master_202407_us.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_202407_us.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '2024-08-01'
   and spv_report_master_202407_us.capital_source_name ilike lmr.capital_source;

--Union Source of spv_report_master_
drop table if exists ods_production.spv_report_master_202407;
create table ods_production.spv_report_master_202407 as
(
   select
      *
   from
      ods_production.spv_report_master_202407_eu srme
   union all
   select
      *
   from
      ods_production.spv_report_master_202407_us
)
;



delete
from
   ods_production.spv_report_master
where
   reporting_date = '2024-07-31';
insert into
   ods_production.spv_report_master
   select
      reporting_date,
      asset_id,
      warehouse,
      serial_number,
      product_sku,
      asset_name,
      category,
      subcategory,
      country as country_code,
      city,
      postal_code,
      invoice_number,
      invoice_date,
      invoice_url,
      purchased_date,
      initial_price,
      delivered_allocations,
      returned_allocations,
      last_allocation_days_in_stock as days_in_stock,
      asset_condition_spv as "condition",
      days_since_purchase,
      valuation_method,
      average_of_sources_on_condition_this_month,
      average_of_sources_on_condition_last_available_price,
      m_since_last_valuation_price,
      valuation_1,
      valuation_2,
      valuation_3,
      final_price,
      sold_date,
      sold_price,
      asset_status_original,
      mrr,
      collected_mrr,
      total_inflow,
      capital_source_name,
      exception_rule,
      ean
   from
      ods_production.spv_report_master_202407 ; 