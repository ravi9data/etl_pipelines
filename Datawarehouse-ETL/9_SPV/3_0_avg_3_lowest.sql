--Step 3: Average 3 lowest
delete from ods_spv_historical.spv_used_asset_price_master where reporting_date=(current_Date-1);
insert into ods_spv_historical.spv_used_asset_price_master 
WITH a AS (    
        SELECT 
    		u.extract_date::text as reporting_month,
            u.product_sku,
            u.asset_condition,
            u.src,
            u.max_available_date as max_available_reporting_month,
--            max(
--        		CASE
--            		WHEN u.extract_date::date <> u.max_available_date::date THEN u.extract_date
--            		ELSE NULL
--        			END) OVER (PARTITION BY u.product_sku, u.asset_condition) AS second_max_available_date,
            avg(
                CASE
                    WHEN u.price_rank <= 3 THEN u.price
                    ELSE NULL
                END) AS avg_3_lowest_price
           FROM  ods_spv_historical.spv_used_asset_price u
          WHERE u.asset_condition <> 'Others'::text AND u.product_sku IS NOT NULL AND u.price IS NOT NULL
  			and u.reporting_date=(current_Date-1)
          GROUP BY u.extract_date, u.product_sku, u.asset_condition, u.src, u.max_available_date
          ORDER BY  u.product_sku DESC        
        ) ,b as (
	select 
		product_sku,
		asset_condition,
		max(max_available_reporting_month)  AS max_available_reporting_month
--		max(second_max_available_date) as second_max_available_date
 FROM  a--tgt_dev.test_spv_step2_v1 
 --where product_sku='GRB106P144'
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
  
  