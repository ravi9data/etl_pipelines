CREATE VIEW product_requests.toptenproducts_store as
with a as(
 select
 product_sku,
 s.store_label,
 store_code,
 s.store_id,
 count(*) as total
 --row_number() over (partition by store_id order by total desc) as ranked
 from master.subscription s 
 left join ods_production.store st on s.store_id = st.id
 where start_date > CURRENT_DATE-7 
    and s.store_type = 'online' 
  	 and s.store_label like '%Grover%' 
	  	 and s.store_name not like '%B2B%'
    and customer_type = 'normal_customer'
 group by 1,2,3,4
 order by 3 desc
	 )
	 ,
prod_excl as 
(select 
a.product_sku, 
a.store_label,
a.store_code,
a.store_id,
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
d.total,
sum(v.requested_others + v.assets_in_stock) as in_stock 
from prod_excl d
left join master.variant v on d.product_sku = v.product_sku
group by 1,2,3,4,5
having in_stock > 0
 )
 ,rank_prod as (
select product_sku, store_label, store_id,store_code,
row_number() over (partition by store_id order by total desc, product_sku desc) as ranked, total
from stock_excl)
select product_sku, 
store_id,
store_label,
store_code,
ranked 
from rank_prod
where ranked <= 10
order by 2
WITH NO SCHEMA BINDING;