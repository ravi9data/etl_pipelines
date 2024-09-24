drop table if exists dm_operations.asset_issue_claims;
create table dm_operations.asset_issue_claims
as
select 
distinct t.asset_id,
a.order_id,
t.serial_number,
a.issue_reason ,
a.issue_date ,
a.issue_comments ,
t.product_name,
t.product_sku,
t.subcategory_name,
t.category_name,
t.brand,
case 
when subcategory_name = 'TV' then regexp_substr(replace(replace(replace(t.product_name , '(', ''), ')', ''), '-', ''), '\\d{2}\\d?') 
else null end as inches,
t.purchased_date,
t.months_since_purchase,
t.supplier,
a.is_recirculated,
a.total_allocations_per_asset,
ash.carrier,
ash.shipment_service,
ash.delivered_at,
ash.shipping_country ,
ash.number_of_attempts,
ash.receiver_city,
t.initial_price,
coalesce(
(select ha.residual_value_market_price 
from master.asset_historical ha
where ha.date <= date_trunc('day', ash.delivered_at)
and ha.asset_id = t.asset_id
order by ha.date desc limit 1),
t.initial_price ) residual_value_when_delivered,
t.residual_value_market_price current_residual_value
from ods_production.allocation a 
left join master.asset t 
on a.asset_id = t.asset_id
left join ods_operations.allocation_shipment ash 
on a.allocation_id = ash.allocation_id 
where issue_date is not null;
