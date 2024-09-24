drop table if exists ods_spv_historical.union_sources_{{ params.tbl_suffix }};
create table ods_spv_historical.union_sources_{{ params.tbl_suffix }} as
(
   select
      *
   from
      ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu
   union
   select
      *
   from
      ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu_all
   union all
   select
      *
   from
      ods_spv_historical.union_sources_{{ params.tbl_suffix }}_us
)
;
----inserting data into union source is done at final step due to MR_
------------------------------------
drop table if exists  ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }}_eu;
create table  ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }}_eu as
SELECT *
FROM ods_spv_historical.spv_used_asset_price_1_mid_month
UNION ALL 
SELECT *
FROM ods_spv_historical.spv_used_asset_price_2_mid_month
/*UNION ALL 
SELECT *
FROM ods_spv_historical.spv_used_asset_price_4
UNION ALL 
SELECT *
FROM ods_spv_historical.spv_used_asset_price_5
UNION ALL
SELECT *
FROM ods_spv_historical.spv_used_asset_price_6
UNION ALL
SELECT *
FROM ods_spv_historical.spv_used_asset_price_7
UNION ALL
SELECT *
FROM ods_spv_historical.spv_used_asset_price_8*/
;

----------------------------------
--Union Source of spv_used_asset_price_
drop table if exists ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }};
create table ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }} as
(
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }}_eu
   union all
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }}_us
)
;

/*delete
from
   ods_spv_historical.spv_used_asset_price
where
   reporting_date = '{{ params.last_day_of_prev_month }}';
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
      ods_spv_historical.spv_used_asset_price_{{ params.tbl_suffix }} ; 
*/     
     
-- STEP 3
 ------------------------------------
drop table if exists  ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }}_eu;
create table  ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }}_eu as
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_1_mid_month
UNION ALL 
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_2_mid_month
/*UNION ALL 
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_4
UNION ALL 
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_5
UNION ALL
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_6
UNION ALL
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_7
UNION ALL
SELECT *
FROM ods_spv_historical.spv_used_asset_price_master_8*/
;

----------------------------------    
     
--Union Source of spv_used_asset_price_master_
drop table if exists ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }};
create table ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }} as
(
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }}_eu
   union all
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }}_us
)
;

/*delete
from
   ods_spv_historical.spv_used_asset_price_master
where
   reporting_date = '{{ params.last_day_of_prev_month }}';
insert into
   ods_spv_historical.spv_used_asset_price_master
   select
      *
   from
      ods_spv_historical.spv_used_asset_price_master_{{ params.tbl_suffix }}; 
 */   
     
     
     
     
-- STEP 4
--Union Source of price_per_condition_
drop table if exists ods_spv_historical.price_per_condition_{{ params.tbl_suffix }};
create table ods_spv_historical.price_per_condition_{{ params.tbl_suffix }} as
(
   select
      *
   from
      ods_spv_historical.price_per_condition_{{ params.tbl_suffix }}_eu
   union all
   select
      *
   from
      ods_spv_historical.price_per_condition_{{ params.tbl_suffix }}_us
)
;

/*
delete
from
   ods_spv_historical.price_per_condition
where
   reporting_date = '{{ params.last_day_of_prev_month }}';
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
      ods_spv_historical.price_per_condition_{{ params.tbl_suffix }}; 
*/     
     
     
-- Step 5
--Union Source of asset_market_value_
drop table if exists ods_spv_historical.asset_market_value_{{ params.tbl_suffix }};
create table ods_spv_historical.asset_market_value_{{ params.tbl_suffix }} as
(
   select
      *
   from
      ods_spv_historical.asset_market_value_{{ params.tbl_suffix }}_eu
   union all
   select
      *
   from
      ods_spv_historical.asset_market_value_{{ params.tbl_suffix }}_us
)
;

/*
delete
from
   ods_spv_historical.asset_market_value
where
   reporting_date = '{{ params.last_day_of_prev_month }}';
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
      ods_spv_historical.asset_market_value_{{ params.tbl_suffix }}; 
*/
--Step 6
-- update mr prices in eu and us report master tables
     
update
   ods_production.spv_report_master_{{ params.tbl_suffix }}_eu
set
   final_price = lmr.final_price::double precision,
   --m_since_last_valuation_price = 0,
   valuation_method = '12.1- (c) - (i),(ii)'
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_{{ params.tbl_suffix }}_eu.product_sku = lmr.product_sku
   and spv_report_master_{{ params.tbl_suffix }}_eu.final_price <> 0
   and
   (
(spv_report_master_{{ params.tbl_suffix }}_eu.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_{{ params.tbl_suffix }}_eu.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '{{ params.first_day_of_month }}'
   and spv_report_master_{{ params.tbl_suffix }}_eu.capital_source_name ilike lmr.capital_source ;
update
   ods_production.spv_report_master_{{ params.tbl_suffix }}_us
set
   final_price = lmr.final_price::double PRECISION--,
   --m_since_last_valuation_price = 0
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_{{ params.tbl_suffix }}_us.product_sku = lmr.product_sku
   and spv_report_master_{{ params.tbl_suffix }}_us.final_price <> 0
   and
   (
(spv_report_master_{{ params.tbl_suffix }}_us.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_{{ params.tbl_suffix }}_us.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '{{ params.first_day_of_month }}'
   and spv_report_master_{{ params.tbl_suffix }}_us.capital_source_name ilike lmr.capital_source;
   
   
   
update
   ods_production.spv_report_master_{{ params.tbl_suffix }}_eu
set
   final_price_without_written_off = lmr.final_price::double precision
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_{{ params.tbl_suffix }}_eu.product_sku = lmr.product_sku
   and spv_report_master_{{ params.tbl_suffix }}_eu.final_price_without_written_off <> 0
   and
   (
(spv_report_master_{{ params.tbl_suffix }}_eu.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_{{ params.tbl_suffix }}_eu.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '{{ params.first_day_of_month }}'
   and spv_report_master_{{ params.tbl_suffix }}_eu.capital_source_name ilike lmr.capital_source ;
update
   ods_production.spv_report_master_{{ params.tbl_suffix }}_us
set
   final_price_without_written_off = lmr.final_price::double precision
from
   ods_spv_historical.luxco_manual_revisions lmr
where
   spv_report_master_{{ params.tbl_suffix }}_us.product_sku = lmr.product_sku
   and spv_report_master_{{ params.tbl_suffix }}_us.final_price_without_written_off <> 0
   and
   (
(spv_report_master_{{ params.tbl_suffix }}_us.asset_condition_spv = 'AGAN'
      and lower(trim(lmr."asset_condition")) in
      (
         'wie neu',
         'sehr gut'
      )
)
      or
      (
         spv_report_master_{{ params.tbl_suffix }}_us.asset_condition_spv = 'NEW'
         and lower(trim(lmr."asset_condition")) in
         (
            'neu'
         )
      )
   )
   and lmr.luxco_month = '{{ params.first_day_of_month }}'
   and spv_report_master_{{ params.tbl_suffix }}_us.capital_source_name ilike lmr.capital_source;

--Union Source of spv_report_master_
drop table if exists ods_production.spv_report_master_{{ params.tbl_suffix }};
create table ods_production.spv_report_master_{{ params.tbl_suffix }} as
(
   select
      *
   from
      ods_production.spv_report_master_{{ params.tbl_suffix }}_eu srme
   union all
   select
      *
   from
      ods_production.spv_report_master_{{ params.tbl_suffix }}_us
)
;


/*
delete
from
   ods_production.spv_report_master
where
   reporting_date = '{{ params.last_day_of_prev_month }}';
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
      ods_production.spv_report_master_{{ params.tbl_suffix }} ; */
