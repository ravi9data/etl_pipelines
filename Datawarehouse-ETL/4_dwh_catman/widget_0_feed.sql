drop table if exists tmp_widget_feed;

create temp table tmp_widget_feed as
with top_products as (
    select oi.product_sku,
        oi.product_name,
        p.product_id,
        o.store_id,
        oi.category_name,
        oi.brand,
        count (oi.product_sku) as cnt
    from ods_production.order o
        left join ods_production.order_item oi on oi.order_id = o.order_id
        left join ods_production.store st on o.store_id = st.id
        left join ods_production.customer c on c.customer_id = o.customer_id
        left join ods_production.product p on oi.product_sku = p.product_sku
    where (
            o.is_in_salesforce
            or (
                o.submitted_date is not null
                and o.order_mode = 'FLEX'
            )
        )
        and o.store_id in ('1', '4', '5', '618', '621')
        and c.customer_type = 'normal_customer'
        and o.submitted_date::date >= current_date - 7
    group by 1,
        2,
        3,
        4,
        5,
        6
)
-- finding the top 6 subcategies by submitted orders in the last 1 day store-wise
,
top_category_pre as (
    select oi.category_name,
        o.store_id,
        count (*) as cat_count,
        row_number() over(
            partition by store_id
            order by cat_count desc
        ) as rank_cat
    from ods_production.order o
        left join ods_production.order_item oi on oi.order_id = o.order_id
        left join ods_production.store st on o.store_id = st.id
        left join ods_production.customer c on c.customer_id = o.customer_id
        left join ods_production.product p on oi.product_sku = p.product_sku
    where (
            o.is_in_salesforce
            or (
                o.submitted_date is not null
                and o.order_mode = 'FLEX'
            )
        )
        and o.store_id in ('1', '4', '5', '618', '621')
        and c.customer_type = 'normal_customer'
        and o.submitted_date::date >= current_date - 7
        and oi.subcategory_name not in (
            'Camera accessories',
            'Phone Accessories',
            'Computing Accessories',
            'Gaming Accessories',
            'E-Mobility Accessories')
    group by 1,
        2
),
top_category as (
    select category_name,
        store_id
    from top_category_pre
    where rank_cat <= 10 -- filtering products with active rental plan and stock availability
        -- filtering the products that does not belong to the top 6 subcategory
        -- counting the occurence of 'deal' using sum() window function - after the 3rd occurence of the deal, the products are excluded  (max 2 or no deal products)
),
stock_deal_filter as (
    select distinct tp.product_sku,
        tp.product_id,
        tp.store_id,
        tp.cnt,
        tp.product_name,
        case
            when rp.is_higher_price_available then 'Deal'
            else 'No deal'
        end as is_deal,
        sum (1) over (
            partition by tp.store_id,
            is_deal
            order by cnt desc rows unbounded preceding
        ) as deal_running_total,
        case
            when is_deal = 'Deal'
            and deal_running_total <= 2 then 'ok'
            when is_deal = 'No deal' then 'ok'
            else 'not ok'
        end as deal_yn,
        tp.brand,
        tp.category_name
    from top_products tp
        left join top_category tc on tc.category_name = tp.category_name
        and tc.store_id = tp.store_id
        left join ods_production.rental_plans rp on tp.product_id = rp.product_id
        and tp.store_id = rp.store_id
        left join ods_production.inventory_store_variant_availability isva on tp.product_sku = isva.product_sku
        and tp.store_id = isva.store_id
    where rp.active
        and availability_mode in ('enabled', 'automatic')
        and isva.available_count > 0
        and tc.category_name is not null
    group by 1,
        2,
        3,
        4,
        5,
        6,
        9,
        10
) -- filtering only the products that fall under 'ok' in the previous step
-- counting the occurence of 'brands' using sum() window function - after the 3rd occurence of a particular brand, the products are excluded (max 2 occurences of the same brand)
,brand_filter as (
    select *,
        sum (1) over (
            partition by brand,
            store_id
            order by cnt desc rows unbounded preceding
        ) as brand_running_total,
        case
            when brand_running_total <= 1 then 'ok'
            else 'not ok'
        end as brand_yn
    from stock_deal_filter
  --  where deal_yn = 'ok'
    group by 1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
) -- filtering only the products that fall under 'ok' in the previous step
-- counting the occurence of 'subcategory' using sum() window function - after the 1st occurence of a subcategory, the products are excluded (max. one product per subcategory)
,
category_filter as (
    select *,
        sum (1) over (
            partition by category_name,
            store_id
            order by cnt desc rows unbounded preceding
        ) as cat_running_total,
        case
            when cat_running_total <= 2 then 'ok'
            else 'not ok'
        end as cat_yn
    from brand_filter
    where brand_yn = 'ok'
) -- finally ranking the products and getting the top 4 products store wise
select store_id,
    product_sku,
    product_id,
    st.store_code,
    is_deal,
    brand,
    category_name,
    rank_product,
    product_name
from (
        select *,
            row_number() over(
                partition by store_id
                order by cnt desc
            ) as rank_product
        from category_filter
        where cat_yn = 'ok'
    ) sf1
    left join ods_production.store st on sf1.store_id = st.id
where rank_product <= 4
order by 1,
    8;

truncate table product_requests.widget_feed;

insert into product_requests.widget_feed
select * from tmp_widget_feed;