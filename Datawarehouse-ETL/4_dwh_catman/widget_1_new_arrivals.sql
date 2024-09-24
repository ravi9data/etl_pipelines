drop table if exists tmp_widget_new_arrivals;
create temp table tmp_widget_new_arrivals as
with first_creation_date AS (
	SELECT 
		product_id,
		store_id,
		min(created_at) AS first_creation_date
	FROM ods_production.rental_plans rp
	GROUP BY 1,2
)	
, new_products as (
    select distinct 
    	rp.product_id,
        rp.store_id,
        p.product_name,
        p.product_sku,
        p.subcategory_name,
        p.brand
    from ods_production.rental_plans rp
    left join ods_production.product p 
    	on p.product_id = rp.product_id
    LEFT JOIN first_creation_date f
    	ON f.product_id = rp.product_id 
    		AND f.store_id = rp.store_id
    where rp.store_id in ('1', '4', '5', '618', '621') 
        and first_creation_date >= current_date - 90 
        and rp.product_store_rank > 100
        and rp.active
        and rp.is_higher_price_available is false
        and rp.product_id not in (
            SELECT product_id
            FROM product_requests.widget_feed f
            WHERE f.store_id = rp.store_id
        ) -- excluding feed products
)
, pageviews as (
    select distinct 
    	np.*,
        sum(pageviews) as pageviews
    from new_products np
    left join dwh.product_reporting pr 
    	on pr.product_sku = np.product_sku
        	and pr.store_id = np.store_id
    where fact_day > current_date - 30
    group by 1,2,3,4,5,6
)
, stock_filter as (
    select distinct mvp.*
    from pageviews mvp
    left join ods_production.inventory_store_variant_availability isva 
    	on mvp.product_sku = isva.product_sku
        	and mvp.store_id = isva.store_id
    where availability_mode in ('enabled', 'automatic')
        and isva.available_count > 0
),
brand_filter as (
    select 
    	*,
        sum (1) over (partition by brand,store_id order by pageviews desc rows unbounded preceding) as brand_running_total,
        case
            when brand_running_total <= 2 
            	then 'ok'
            else 'not ok'
        end as brand_yn
    from stock_filter
    group by 1,2,3,4,5,6,7
),
subcategory_filter as (
    select 
    	*,
        sum (1) over (partition by subcategory_name,store_id order by pageviews desc rows unbounded preceding) as subcat_running_total,
        case
            when subcat_running_total <= 1 
            	then 'ok'
            else 'not ok'
        end as subcat_yn
    from brand_filter
    where brand_yn = 'ok'
)
select 
	store_id,
    product_sku,
    st.store_code,
    brand,
    pageviews,
    subcategory_name,
    rank_product,
    product_name
from (
        select 
        	*,
            row_number() over(partition by store_id order by pageviews desc) as rank_product
        from subcategory_filter
        where subcat_yn = 'ok'
    ) sf1
left join ods_production.store st 
	on sf1.store_id = st.id
where rank_product <= 4
;

truncate table product_requests.widget_new_arrivals;

insert into product_requests.widget_new_arrivals
select * from tmp_widget_new_arrivals;
