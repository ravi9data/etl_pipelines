CREATE VIEW product_requests.toptenproducts_store as
with a as(
 select
 product_sku,
 s.store_label,
 store_code,
 s.store_id,
 s.subcategory_name,
 count(*) as total
 from master.subscription s 
 left join ods_production.store st on s.store_id = st.id
 where start_date > CURRENT_DATE-7 
    and s.store_type = 'online' 
  	 and s.store_label like '%Grover%' 
	  	 and s.store_name not like '%B2B%'
    and customer_type = 'normal_customer'
 group by 1,2,3,4,5
 order by 3 desc
	 )
	 ,
prod_excl as 
(select 
a.product_sku, 
a.store_label,
a.store_code,
a.store_id,
a.subcategory_name,
a.total
from a
left join ods_production.product p on a.product_sku = p.product_sku
where (p.created_at > current_date - 90 or p.updated_at > current_date - 90)
)
,
stock_excl as 
(select 
d.product_sku,  
d.store_label, 
d.store_id,
d.store_code,
d.subcategory_name,
d.total,
sum(v.requested_others + v.assets_in_stock) as in_stock 
from prod_excl d
left join master.variant v on d.product_sku = v.product_sku
group by 1,2,3,4,5,6
having in_stock > 0
 )
-- ranking the products store and subcategory wise
,rank_prod as (
select product_sku, store_label, store_id,store_code, subcategory_name, 
row_number() over (partition by store_id,subcategory_name order by total,product_sku desc) as ranked_sub, 
total
from stock_excl)
-- ranking the products considering only the top 2 from each subcategory, store
,rank_prod_2 as (
select product_sku, store_label, store_id,store_code, subcategory_name, ranked_sub,
row_number() over (partition by store_id order by total,product_sku desc) as ranked, 
total
from rank_prod
where ranked_sub <=2)
-- selecting top 10 products store wise
select product_sku, 
store_id,
store_label,
store_code,
ranked 
from rank_prod_2
where ranked <= 10
order by 2
WITH NO SCHEMA BINDING;