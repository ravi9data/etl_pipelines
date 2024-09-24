drop table if exists ods_production.customer_orders_details;
create table ods_production.customer_orders_details as 
with a as (
SELECT
    o.customer_id,
    max(o.updated_date) as updated_at,
    count(o.order_id) as carts,
    max(o.created_date) as max_cart_date,
    count(
      case
      when submitted_date is not null 
        then o.order_id
      end
    ) as submitted_orders,
    max(
      case
        when submitted_date is not null then o.created_date
      end
    ) as max_submitted_order_date,
    count(
      case
        when o.status = 'PAID' then o.order_id
      end
    ) as paid_orders,
    count(
      case
        when o.status = 'DECLINED' then o.order_id
      end
    ) as declined_orders,
        count(
      case
        when o.status = 'CANCELLED' then o.order_id
      end
    ) as cancelled_orders,
    count(
      case
        when o.status = 'FAILED FIRST PAYMENT' then o.order_id
      end
    ) as ffp_orders,
    count(
      case
        when o.status = 'FAILED FIRST PAYMENT' 
        and o.submitted_date::date > CURRENT_DATE-181 then o.order_id
      end
    ) as ffp_orders_latest,
    count(
      case
        when o.status = 'MANUAL REVIEW' then o.order_id
      end
    ) as manual_review_orders,
    max(
      case
        when o.status = 'PAID' then o.created_date
      end
    ) as max_paid_order_date,
    listagg(DISTINCT(case when o.status = 'PAID' then  o.voucher_code end), ' , ')  as voucher_usage,
    listagg(distinct case when o.order_rank=1 then coalesce(s.country_name,'never_add_to_cart') end) as signup_country
  from ods_production."order" o
  left join ods_production.store s 
   on s.id=o.store_id
  GROUP BY customer_id
),
last_paid_order_id as (
select 
  distinct 
  customer_id, 
  order_id,
row_number() over (partition by o.customer_id order by o.paid_date desc ) as paid_idx
from ods_production.order o
  where paid_date is not null
),
last_created_order_id as ( 
select 
  distinct 
  o.customer_id,
  o.order_id,
  o.created_date, 
  listagg(oi.product_name, ', ') as last_cart_product_names,
  row_number() over (partition by o.customer_id order by o.created_date desc ) as created_idx
from ods_production.order o
left join ods_production.order_item oi 
  on o.order_id = oi.order_id
where created_date is not null
group by 1,2,3
)
select
  a.*,
  po.order_id as last_paid_order_id,
  co.last_cart_product_names
  from a
    left join last_paid_order_id po
    on po.customer_id = a.customer_id
    and po.paid_idx = 1
  left join last_created_order_id co
    on co.customer_id = a.customer_id
    and co.created_idx = 1;

GRANT SELECT ON ods_production.customer_orders_details TO tableau;
