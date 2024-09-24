refresh materialized view staging_price_collection.ods_amazon;
refresh materialized view staging_price_collection.ods_ebay ;

/*Union sources */
--Step1: Union of all sources
drop table if exists tmp_union_sources;
create temp table tmp_union_sources as with a as
(
   select distinct
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
      reporting_month::date < '2023-08-01'::date
   UNION ALL
   select distinct
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
      reporting_month::date < '2023-08-01'::date
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
      reporting_month::date < '2023-08-01'::date
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
      reporting_month::date < '2023-08-01'::date
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
      and reporting_month::date < '2023-08-01'::date
)
select
   *
from
   a
where
   region in
   (
      'GERMANY',
      'USA'
   )
;
--Step1: Seprate EU and US data
drop table if exists ods_spv_historical.union_sources_202307_us;
drop table if exists ods_spv_historical.union_sources_202307_eu;
create table ods_spv_historical.union_sources_202307_eu as
select
   *
from
   tmp_union_sources
where
   region = 'GERMANY';
create table ods_spv_historical.union_sources_202307_us as
select
   *
from
   tmp_union_sources
where
   region = 'USA';
insert into
   ods_spv_historical.union_sources_202307_eu ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   select
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
   from
      ods_spv_historical.luxco_manual_revisions mr
   where
      mr.capital_source in
      (
         'Grover Finance I GmbH',
         'SUSTAINABLE TECH RENTAL EUROPE II GMBH'
      )
      and mr.luxco_month <= '2023-07-01';
insert into
   ods_spv_historical.union_sources_202307_us ( src, region, extract_date, reporting_month, item_id, product_sku, asset_condition, currency, price, price_in_euro)
   select
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
   from
      ods_spv_historical.luxco_manual_revisions mr
      left join
         trans_dev.daily_exchange_rate er
         on '2023-07-01'::date - 1 = er.date_
         and mr.luxco_month <= '2023-07-01'
   where
      mr.capital_source in
      (
         'USA_test'
      )
;


drop table if exists ods_spv_historical.union_sources_202307_eu_updated;
create table ods_spv_historical.union_sources_202307_eu_updated as
with a as (
select *,
   case 
      when asset_condition in ('Neu') then 'NEW'
      when asset_condition in ('Sehr gut','Wie neu') then 'AGAN'  
   else null end as asset_condition_2
from ods_spv_historical.union_sources_202307_eu
)
select 
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

drop table if exists ods_spv_historical.union_sources_202307_eu_updated_2;
create table ods_spv_historical.union_sources_202307_eu_updated_2 as
select a.*,b.asset_condition_2,price_LP,initial_price,
   case 
      when asset_condition_2 is not null and price < price_LP THEN 1
   else 0 end as outlier_removal 
from ods_spv_historical.union_sources_202307_eu a 
left join ods_spv_historical.union_sources_202307_eu_updated b
on a.product_sku = b.product_sku
and a.asset_condition = b.asset_condition 
;



drop table if exists ods_spv_historical.union_sources_202307_eu;
create table ods_spv_historical.union_sources_202307_eu AS 
SELECT * FROM  ods_spv_historical.union_sources_202307_eu_updated_2
WHERE outlier_removal = 0;





drop table if exists ods_spv_historical.union_sources_202307_us_updated;
create table ods_spv_historical.union_sources_202307_us_updated as
with a as (
select *,
   case 
      when asset_condition in ('Neu') then 'NEW'
      when asset_condition in ('Sehr gut','Wie neu') then 'AGAN'  
   else null end as asset_condition_2
from ods_spv_historical.union_sources_202307_us
)
select 
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

drop table if exists ods_spv_historical.union_sources_202307_us_updated_2;
create table ods_spv_historical.union_sources_202307_us_updated_2 as
select a.*,b.asset_condition_2,price_LP,initial_price,
   case 
      when asset_condition_2 is not null and price < price_LP THEN 1
   else 0 end as outlier_removal 
from ods_spv_historical.union_sources_202307_us a 
left join ods_spv_historical.union_sources_202307_us_updated b
on a.product_sku = b.product_sku
and a.asset_condition = b.asset_condition 
;



drop table if exists ods_spv_historical.union_sources_202307_us;
create table ods_spv_historical.union_sources_202307_us AS 
SELECT * FROM  ods_spv_historical.union_sources_202307_us_updated_2
WHERE outlier_removal = 0;
