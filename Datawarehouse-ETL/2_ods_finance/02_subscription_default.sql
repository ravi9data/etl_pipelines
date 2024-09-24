drop table if exists ods_production.subscription_default; 

create table ods_production.subscription_Default as
with a as (
select 
s.customer_id,
s.subscription_id, 
c.max_cashflow_date, 
c.last_valid_payment_category,
coalesce(c.asset_purchase_amount_paid,0)-coalesce(c.asset_purchase_amount_chargeback,0) as asset_purchase_amount_paid,
coalesce(a.outstanding_assets,0) as outstanding_assets,
case 
 when outstanding_assets>=1 
 and CURRENT_DATE-max_cashflow_date::Date > 60
 and coalesce(c.asset_purchase_amount_paid,0) <= 0
then 1 else 0 end as subscription_default
from ods_production.subscription s
left join ods_production.subscription_cashflow c 
 on s.subscription_id=c.subscription_id
left join ods_production.subscription_assets a
 on s.subscription_id=a.subscription_id)
select distinct 
customer_id,
subscription_id,
(subscription_default) as is_subscription_default,
max(subscription_default) over (partition by customer_id) as is_customer_default
from a 
order by 4 asc;

GRANT SELECT ON ods_production.subscription_Default TO tableau;
