drop table if exists dm_operations.product_ean;
create table dm_operations.product_ean as
with latest_catman_products as (
    select *
    from (
        select
            sku 
            ,sku_product
            ,ean
            ,name
            ,brand
            ,market_price
            ,event_timestamp
            ,rank() over (partition by ean order by event_timestamp desc) as rn
            ,min(event_timestamp) over (partition by ean) as created_date
        from stg_kafka_events_full.stream_catman_products cp
    ) where rn=1
)
select
     cp.sku
    ,cp.sku_product
    ,cp.ean
    ,created_date
    ,cp."name"
    ,p.brand
    ,cp.market_price
    ,cp.event_timestamp as date_consumed
    ,p.category_name
    ,p.subcategory_name  
from latest_catman_products cp
left join ods_production.product p on p.product_sku = cp.sku_product 
where cp.name not like '%(US)%';