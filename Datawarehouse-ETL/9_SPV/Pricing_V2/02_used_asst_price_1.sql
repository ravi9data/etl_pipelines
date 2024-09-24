drop table if exists  trans_dev.pricing_spv_used_asset_price_master; 
create table trans_dev.pricing_spv_used_asset_price_master as
WITH a AS (    
        SELECT 
    		u.extract_date::text as reporting_month,
            u.product_sku,
            u.asset_condition,
            u.src,
            u.max_available_date as max_available_reporting_month,
  			avg(
                CASE 
                    WHEN u.price_rank_reordered <= 3 THEN u.price
                    ELSE NULL
                END) AS avg_3_lowest_price 
          FROM  
          	trans_dev.pricing_outlier_removal_ranked u
          WHERE 
          u.asset_condition <> 'Others'::text 
         -- and product_sku = 'GRB201P4188'
          AND u.product_sku IS NOT NULL AND 
          u.price IS NOT NULL
          GROUP BY u.extract_date, u.product_sku, u.asset_condition, u.src, u.max_available_date     
        ) ,b as (
	select 
		product_sku,
		asset_condition,
		max(max_available_reporting_month)  AS max_available_reporting_month
 FROM  a
group by 1,2)
 SELECT 
 	current_date::date-1 as reporting_date,
 	a.product_sku,
    a.asset_condition,
    avg(
        CASE
            WHEN date_trunc('month',a.reporting_month::date) = date_trunc('month',b.max_available_reporting_month::date) THEN a.avg_3_lowest_price
            ELSE NULL::numeric
        END) AS avg_price,
    max(
        CASE
            WHEN date_trunc('month',a.reporting_month::date) = date_trunc('month',b.max_available_reporting_month::date) THEN a.reporting_month
            ELSE NULL::text
        END) AS max_available_price_date,
    (date_trunc('month', current_date-1)::text::date - max(
        CASE
            WHEN date_trunc('month',a.reporting_month::date) = date_trunc('month',b.max_available_reporting_month::date) THEN a.reporting_month
            ELSE NULL::text
        END)::date) / 30 AS months_since_last_price,
    power(0.97, ((date_trunc('month', current_date-1)::text::date - max(
        CASE
            WHEN date_trunc('month',a.reporting_month::date) = date_trunc('month',b.max_available_reporting_month::date) THEN a.reporting_month
            ELSE NULL::text
        END)::date) / 30)::numeric) * avg(
        CASE
            WHEN date_trunc('month',a.reporting_month::date) = date_trunc('month',b.max_available_reporting_month::date) THEN a.avg_3_lowest_price
            ELSE NULL::numeric
        END) AS final_market_price
   FROM a
          left join b on a.product_sku=b.product_sku and a.asset_condition=b.asset_condition
  GROUP BY a.product_sku, a.asset_condition;
 
