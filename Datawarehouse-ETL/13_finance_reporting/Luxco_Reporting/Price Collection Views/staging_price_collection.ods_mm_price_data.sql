/*MTLN-1.50.7 (build 698)*/
--Grant Select on tables
--GRANT all ON ALL TABLES IN SCHEMA  staging_price_collection.ods_amazon TO farman_ali;
--GRANT all ON ALL TABLES IN SCHEMA  staging_price_collection TO matillion;
--GRANT all ON ALL TABLES IN SCHEMA pawan_pai TO pawan_pai;
--GRANT usage ON SCHEMA staging_price_collection TO group  bi;
--GRANT select ON ALL TABLES IN SCHEMA staging_price_collection TO group bi;
--GRANT select ON ALL TABLES IN SCHEMA staging_price_collection TO pawan_pai;
--GRANT select ON ALL TABLES IN SCHEMA dm_marketing TO yuvaraj;


--GRANT all ON SCHEMA dm_payments TO divyaraj;

--GRANT usage ON SCHEMA staging_price_collection TO group bi;
--GRANT all ON ALL TABLES IN SCHEMA staging_price_collection TO group  bi;




create or replace  view staging_price_collection.ods_mm_price_data as (
 select 
                  DISTINCT
                  'MEDIAMARKT_DIRECT_FEED' as src,
                   m.week_date as reporting_month,
                  'Null' as itemid,
                  m.product_sku,
                  'EUR' as currency,
               m.price,
                  'Neu' as asset_condition
          from ods_external.mm_price_data  m 
)
with no schema binding;