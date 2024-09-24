DROP TABLE IF EXISTS ods_production.CUSTOMER_ACQUISITION_COHORT;
CREATE TABLE ods_production.CUSTOMER_ACQUISITION_COHORT AS 
with a as (
select distinct
 a.customer_id, 
 first_value(a.order_id) 
  over (partition by a.customer_id order by a.order_created_date, a.min_due_date rows between unbounded preceding and unbounded following) as order_Id,
 first_value(a.subscription_id) 
  over (partition by a.customer_id order by a.order_created_date, rental_period desc, a.min_due_date rows between unbounded preceding and unbounded following) as subscription_id,
-- min(a.order_created_date) over (partition by a.customer_id) as customer_acquisition_cohort, 
 min(first_asset_delivery_date) over (partition by a.customer_id) as customer_acquisition_cohort, 
 min(a.min_due_date) over (partition by a.customer_id) as first_due_date
from (
 select distinct sp.customer_id, sp.order_id, sp.subscription_id, s.rental_period, min(sp.due_date) as min_due_date , min(o.created_date) as order_created_date, 
 min(coalesce(sa.first_asset_delivered,s.first_asset_delivery_date)) as first_asset_delivery_date
 from ods_production.payment_subscription sp 
 left join ods_production.subscription s on sp.subscription_Id=s.subscription_id
 left join ods_production.subscription_assets sa on sa.subscription_id=s.subscription_id
 left join ods_production.subscription_cancellation_reason c on sp.subscription_id=c.subscription_id
 left join ods_production."order" o on o.order_id=sp.order_id
 where sp.payment_type = 'FIRST' 
  and sp.status = 'PAID' 
  and coalesce(c.cancellation_reason_churn,'active') not in ('failed delivery') 
group by 1,2,3,4
) a)
select 
 a.*, 
 st.store_label as customer_acquisition_store,
 s.subscription_plan as customer_acquisition_rental_plan,
 s.category_name as customer_acquisition_category_name,
 s.subcategory_name as customer_acquisition_subcategory_name,
 s.brand as customer_acquisition_product_brand
from a 
left join ods_production.subscription s on a.subscription_id=s.subscription_id
left join ods_production.store st on st.id=s.store_id
order by 4 desc;

GRANT SELECT ON ods_production.CUSTOMER_ACQUISITION_COHORT TO tableau;
