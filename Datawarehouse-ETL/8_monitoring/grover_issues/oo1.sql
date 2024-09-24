drop table if exists monitoring.oo1;
create table monitoring.oo1 as 
WITH allocation_rank as (
SELECT id,
rank () over (partition by asset__c order by allocated__c) as allocationrank_per_asset,
count(*) over (partition by asset__c) as total_allocations
from
stg_salesforce.customer_asset_allocation__c)
select 
a.id as allocation_id,
s.id as subscription_id,
a.asset__c,
a.status__c as allocation_status,
a.package_lost__c,
s.status__c,
p.category_name,
p.subcategory_name,
asset.cost_price__c,
asset.serialnumber,
a.asset_status__c,
a.allocated__c,
a.picked_by_carrier__c,
a.wh_goods_order_id__c,
a.shipment_tracking_number__c,
asset.invoice_url__c,
ar.allocationrank_per_asset,
ar.total_allocations,
sysdate as sys_date,
datediff(day,a.picked_by_carrier__c::timestamp,sys_date) as time_diff,
welcome_letter__c,
asset.name as asset_name
    from stg_salesforce.customer_asset_allocation__c a
    inner join stg_salesforce.subscription__c s on a.subscription__c = s.id
    left join stg_salesforce."order" o on o.id=s.order__c
    inner join stg_salesforce.asset asset on a.asset__c=asset.id
    left join ods_production.product p on asset.f_product_sku_product__c = p.product_sku
    left join allocation_rank ar on a.id = ar.id
    where a.status__c = 'SHIPPED'
    and a.allocated__c IS NOT NULL
    order by a.allocated__c DESC;

GRANT SELECT ON monitoring.oo1 TO tableau;
