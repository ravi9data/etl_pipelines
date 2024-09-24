drop table if exists tmp_widget_our_best_deals;

create temp table tmp_widget_our_best_deals as 
with top_products as (
    select distinct oi.product_sku,
        oi.product_name,
        p.product_id,
        o.store_id,
        oi.subcategory_name,
        oi.brand,
        count (
            case
                when date_Trunc('week', submitted_date)::date = date_trunc('week', dateadd('week', -1, current_date))::date then oi.product_sku
            end
        ) as cnt_last_week,
        count (
            case
                when (
                    date_Trunc('week', submitted_date)::date = date_trunc('week', dateadd('week', -2, current_date))::date
                ) then oi.product_sku
            end
        ) as cnt_2_week,
        (cnt_last_week::numeric - cnt_2_week::numeric) as wow_diff,
        round((wow_diff) /(nullif (cnt_2_week::numeric, 0)), 2) as wow_diff_1,
        case
            when wow_diff_1 > 0 then 'ok'
            else 'not ok'
        end as is_spike
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
        and o.submitted_date >= current_date - 14
        and oi.subcategory_name not in (
            'Camera accessories',
            'Phone Accessories',
            'Computing Accessories'
        )
        and oi.product_sku not in (
            SELECT product_sku
            FROM product_requests.widget_feed f
            where f.store_id = o.store_id
        ) -- excluding feed products
        and oi.product_sku not in (
            SELECT product_sku
            FROM product_requests.widget_new_arrivals f
            where f.store_id = o.store_id
        ) -- excluding new arrivals products
    group by 1,
        2,
        3,
        4,
        5,
        6
),
stock_deal_filter as (
    select distinct tp.*
    from top_products tp
        left join ods_production.rental_plans rp on tp.product_id = rp.product_id
        and tp.store_id = rp.store_id
        left join ods_production.inventory_store_variant_availability isva on tp.product_sku = isva.product_sku
        and tp.store_id = isva.store_id
    where is_spike = 'ok'
        and rp.active
        and rp.is_higher_price_available
        and availability_mode in ('enabled', 'automatic')
        and isva.available_count > 0
),
brand_filter as (
    select *,
        sum (1) over (
            partition by brand,
            store_id
            order by cnt_last_week desc rows unbounded preceding
        ) as brand_running_total,
        case
            when brand_running_total <= 2 then 'ok'
            else 'not ok'
        end as brand_yn
    from stock_deal_filter
    group by 1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
),
subcategory_filter as (
    select *,
        sum (1) over (
            partition by subcategory_name,
            store_id
            order by cnt_last_week desc rows unbounded preceding
        ) as subcat_running_total,
        case
            when subcat_running_total <= 1 then 'ok'
            else 'not ok'
        end as subcat_yn
    from brand_filter
    where brand_yn = 'ok'
)
select store_id,
    product_sku,
    st.store_code,
    brand,
    subcategory_name,
    rank_product,
    wow_diff_1,
    product_name
from (
        select *,
            row_number() over(
                partition by store_id
                order by cnt_last_week desc
            ) as rank_product
        from subcategory_filter
        where subcat_yn = 'ok'
    ) sf1
    left join ods_production.store st on sf1.store_id = st.id
where rank_product <= 4
order by 1,
    6;

truncate table product_requests.widget_our_best_deals;

insert into product_requests.widget_our_best_deals
select * from tmp_widget_our_best_deals;