CREATE VIEW product_requests.topproducts_store as
-- finding top ordered subcategories store wise
with top10_subcategories as( 
select subcategory_name,
       count(*) as total, 
       s.store_label, 
       s.store_id , 
       store_code,
       row_number() over (partition by store_id order by total desc) as ranked
from master.subscription s 
left join ods_production.store st on s.store_id = st.id
where start_date > current_date -7                            
and s.store_type = 'online' 
and s.store_label like '%Grover%' 
and s.store_name not like '%B2B%'
and customer_type = 'normal_customer'
group by 1,3,4,5)
-- getting the top ordered products for top 10 subcategories storewise
, top10_products as (
select a.subcategory_name, product_sku,s.store_id, a.store_code, a.ranked,
       count(*) as total_products
from master.subscription s 
left join top10_subcategories a 
          on a.subcategory_name = s.subcategory_name 
          and a.store_id = s.store_id
where start_date > current_date -7                            
and s.store_type = 'online' 
and s.store_label like '%Grover%' 
and s.store_name not like '%B2B%'
and customer_type = 'normal_customer'
and a.ranked <= 10
group by 1,2,3,4,5)
-- checking stock
, stock_subs as (
select b.*, 
    v.product_id,
	sum(v.assets_in_stock) as in_stock
from top10_products b
left join master.variant v on b.product_sku = v.product_sku
group by 1,2,3,4,5,6,7
having in_stock > 0
 ),
 -- checking active rental plans
 rental_plans as (
 select s.*, rp.active
 from stock_subs s
 left join ods_production.rental_plans rp 
 on s.product_id = rp.product_id 
 and s.store_id = rp.store_id
 where rp.active
 group by 1,2,3,4,5,6,7,8,9),
-- ranking the products within top subcategories that are in stock and has active subs
ranking_products as (
select c.*,
	 row_number() over (partition by store_id, subcategory_name order by total_products desc) as product_rank
     from rental_plans c)
-- choosing a top product per subcategory store-wise
select distinct store_id, 
	   product_sku, 
	   store_code, 
	   ranked as subcategory_rank
from ranking_products 
where product_rank = 1
order by 1,4
WITH NO SCHEMA BINDING;