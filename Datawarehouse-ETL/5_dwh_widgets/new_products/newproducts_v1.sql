CREATE VIEW product_requests.new_products as
with a as(
 select
 product_sku,
 s.store_label,
 store_code,
 s.store_id,
 count(*) as total
 from master.subscription s 
left join ods_production.store st on s.store_id = st.id
where s.store_type = 'online' 
and s.store_label like '%Grover%' 
and s.store_name not like '%B2B%'
and customer_type = 'normal_customer'
group by 1,2,3,4
order by 3 desc
	 )
	 ,
-- updated or created within 90 days and rank > 100
prod_excl as 
(select 
 a.*,
 p.product_id
 from a
left join ods_production.product p on a.product_sku = p.product_sku
where (p.created_at > current_date - 90 or p.updated_at > current_date - 90) and p.rank > 100
)
,
-- stock available in store level
stock_excl as 
(select 
 d.*,
 v.availability_state
from prod_excl d
left join master.variant v on d.product_sku = v.product_sku
where availability_state = 'available'
group by 1,2,3,4,5,6,7
 ),
 -- no disount plan 
rental_plans as (
select 
 s.*, 
 rp.discount_plan
from stock_excl s
left join ods_production.rental_plans rp 
on s.product_id = rp.product_id 
and s.store_id = rp.store_id
where rp.discount_plan is null
group by 1,2,3,4,5,6,7,8),
rank_prod as (
select 
 rp.*,
 row_number() over (partition by store_id order by total desc) as ranked, total
from rental_plans rp)
select 
 product_sku, 
 store_id,
 store_code,
 ranked 
from rank_prod
where ranked <= 10
order by 2
WITH NO SCHEMA BINDING;
