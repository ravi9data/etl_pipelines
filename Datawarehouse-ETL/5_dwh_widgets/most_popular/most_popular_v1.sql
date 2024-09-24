CREATE VIEW product_requests.most_popular as
with top_products as 
(select oi.product_sku, oi.product_name, p.product_id, o.store_id, oi.subcategory_name, oi.brand,
 count (oi.product_sku ) as cnt
from ods_production.order o
 				left join ods_production.order_item oi on oi.order_id = o.order_id
 				left join ods_production.store st on o.store_id = st.id
 				left join ods_production.customer c on c.customer_id = o.customer_id 
 				left join ods_production.product p on oi.product_sku = p.product_sku
 				where o.submitted_date is not null
 				and o.store_id in ('1', '4', '5', '618', '621')
 				and c.customer_type = 'normal_customer'
 				and o.submitted_date::date >= current_date - 30
 			    and oi.product_sku not in (SELECT product_sku FROM product_requests.feed f where f.store_id = o.store_id ) -- excluding feed products
 group by 1,2,3,4,5,6)
 -- finding the top 6 subcategies by submitted orders in the last 30 days store-wise  
 , top_subcategory as 
 (select subcategory_name, store_id from (select  oi.subcategory_name, o.store_id,
  count (*) as subcat_count,
  row_number() over( partition by store_id order by subcat_count desc ) as rank_subcat
from ods_production.order o
 				left join ods_production.order_item oi on oi.order_id = o.order_id
 				left join ods_production.store st on o.store_id = st.id
 				left join ods_production.customer c on c.customer_id = o.customer_id 
 				left join ods_production.product p on oi.product_sku = p.product_sku
 				where o.submitted_date is not null 
 				and o.store_id in ('1', '4', '5', '618', '621')
 				and c.customer_type = 'normal_customer'
 				and o.submitted_date::date >= current_date - 30
 group by 1,2)
 where rank_subcat <= 10)
 -- filtering products with active rental plan and stock availability 
 -- filtering the products that does not belong to the top 10 subcategory
 -- counting the occurence of 'deal' using sum() window function - after the 3rd occurence of the deal, the products are excluded  (max 2 or no deal products)
 , stock_deal_filter as (select 
 distinct 
 tp.product_sku, tp.store_id, tp.cnt, tp.product_name, 
 case when rp.is_higher_price_available then 'Deal' else 'No deal' end as is_deal,
 sum (1) over (partition by tp.store_id, is_deal order by tp.cnt desc rows unbounded preceding) as deal_running_total,
 case 
 when is_deal = 'Deal' and deal_running_total <= 2 then 'ok' 
 when is_deal = 'No deal' then 'ok'
 else 'not ok' 
 end as deal_yn,
 tp.brand,
 tp.subcategory_name
 from top_products tp
 left join top_subcategory tc on tc.subcategory_name = tp.subcategory_name and tc.store_id = tp.store_id
 left join ods_production.rental_plans rp on tp.product_id = rp.product_id and tp.store_id = rp.store_id
 left join ods_production.inventory_store_variant_availability isva on tp.product_sku = isva.product_sku and tp.store_id = isva.store_id 
 where rp.active and availability_mode in ('enabled', 'automatic') and tc.subcategory_name is not null
 group by 1,2,3,4,5,8,9)
 -- filtering only the products that fall under 'ok' in the previous step
 -- counting the occurence of 'brands' using sum() window function - after the 3rd occurence of a particular brand, the products are excluded (max 2 occurences of the same brand)
 , brand_filter as 
 (select sd.*,
 sum (1) over (partition by sd.brand, sd.store_id order by sd.cnt desc rows unbounded preceding) as brand_running_total,
 case when brand_running_total <= 2 then 'ok' else 'not ok' end as brand_yn
 from stock_deal_filter sd
 --left join product_requests.feed f on f.product_sku = sd.product_sku
 where deal_yn = 'ok'
 group by 1,2,3,4,5,6,7,8,9)
 -- filtering only the products that fall under 'ok' in the previous step
 -- counting the occurence of 'subcategory' using sum() window function - after the 1st occurence of a subcategory, the products are excluded (max. one product per subcategory)
   , subcategory_filter_1 as 
 (select *,
 sum (1) over (partition by subcategory_name, store_id order by cnt desc rows unbounded preceding) as subcat_running_total_2,
 case when subcat_running_total_2 <= 1 then 'ok' else 'not ok' end as subcat_yn_2
 from brand_filter 
 where brand_yn = 'ok')
 , subcategory_filter_2 as 
 (select bf.*,
 case when bf.subcategory_name = (select distinct subcategory_name from product_requests.feed f where f.store_id = bf.store_id and f.subcategory_name = bf.subcategory_name) then 'same' 
 else 'diff' end as same_subcat
 from subcategory_filter_1 bf
 where subcat_yn_2 = 'ok')
 , subcategory_filter_3 as 
 (select f1.*,
  sum (1) over (partition by f1.store_id, same_subcat order by f1.cnt desc rows unbounded preceding) as subcat_total,
 case 
 when same_subcat = 'same' and subcat_total <= 2 then 'ok' 
 when same_subcat = 'diff' then 'ok'
 else 'not ok' 
 end as subcat_yn
 from  subcategory_filter_2 f1) 
 -- finally ranking the products and getting the top 4 products store wise
 select product_sku, product_name, cnt, store_id, is_deal, brand, subcategory_name, rank_product 
 from (select sf.*,
 row_number() over( partition by store_id order by cnt desc ) as rank_product
 from subcategory_filter_3 sf
 where subcat_yn_2 = 'ok')   
 where rank_product <= 4
 order by 4,8 
WITH NO SCHEMA BINDING;