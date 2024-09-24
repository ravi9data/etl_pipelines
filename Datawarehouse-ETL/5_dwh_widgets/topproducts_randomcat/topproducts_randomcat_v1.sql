CREATE VIEW product_requests.topproducts_randomcategory_store as
-- getting the subcategory_name and total orders based on basic filters
with a as( 
select s.subcategory_name, 
       count(*) as total, 
       s.store_label, 
       s.store_id, 
       store_code,
       p.subcategory_id
from master.subscription s 
left join ods_production.store st on s.store_id = st.id
left join ods_production.product p on s.product_sku = p.product_sku
where s.store_type = 'online' 
and s.store_label like '%Grover%' 
and s.store_name not like '%B2B%'
and customer_type = 'normal_customer'
group by 1,3,4,5,6)
-- getting the most ordered products, start date last week for each subcategory store-wise
, b as (
select a.*, product_sku, 
       count(*) as total_products
from master.subscription s 
left join a on a.subcategory_name = s.subcategory_name 
            and a.store_id = s.store_id
where start_date > current_date -7 
and s.store_type = 'online' 
and s.store_label like '%Grover%' 
and s.store_name not like '%B2B%'
and customer_type = 'normal_customer'
group by 1,2,3,4,5,6,7)
-- checking stock and active subs 
, c as (
select b.*, v.product_id,
	sum(v.assets_in_stock) as in_stock
from b
left join master.variant v on b.product_sku = v.product_sku
group by 1,2,3,4,5,6,7,8,9
having in_stock > 0
 ),
-- checking active rental plans
rental_plans as (
 select c.*
 from c
 left join ods_production.rental_plans rp 
 on c.product_id = rp.product_id 
 and c.store_id = rp.store_id
 where rp.active
 group by 1,2,3,4,5,6,7,8,9,10),
-- ranking the products within subcategories that are in stock and has active subs
d as (
select r.*,
	 row_number() over (partition by store_id, subcategory_name order by total_products desc) as product_rank
     from rental_plans r),
e as (
select * from d 
where product_rank <= 10)
select distinct
	   product_sku, 
	   store_id, 
	   store_code, 
	   subcategory_name,
	   subcategory_id,
	   product_rank
from e
order by 2,4,6
WITH NO SCHEMA BINDING;