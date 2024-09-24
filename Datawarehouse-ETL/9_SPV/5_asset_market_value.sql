delete from ods_spv_historical.asset_market_value where reporting_date=(current_Date-1);

insert into ods_spv_historical.asset_market_value
with d AS (
     SELECT 
         asset.asset_id,
         asset.product_sku,
         asset.purchased_date,
         max(asset.purchased_date) OVER (PARTITION BY asset.product_sku) AS last_sku_purchase,
         asset.initial_price
     FROM ods_production.asset
      WHERE asset.initial_price IS NOT NULL AND asset.initial_price > 0
 ), e AS (
     SELECT 
   		DISTINCT d_1.product_sku,
        avg(d_1.initial_price) AS initial_price
     FROM d d_1
      WHERE d_1.purchased_date = d_1.last_sku_purchase
     GROUP BY d_1.product_sku
), f AS (
         SELECT DISTINCT
         	 ss.asset_id,
                CASE	
                   /*###############
                    WRITTEN OFF DC
                   ####################*/
                    WHEN ss.asset_status_original='WRITTEN OFF DC' then 0            
                  /*###############
                  LOST/ LOST SOLVED / SOLD
                  #####################*/
                    WHEN ss.asset_status_original in ('SOLD', 'LOST', 'LOST SOLVED') 
                     THEN 0
                  /*###############
                  USED AND IN STOCK FOR 6M
                  #####################*/
                    WHEN (ss.asset_condition_spv = 'USED') 
                     AND ss.last_allocation_days_in_stock >= (6 * 30) 
                     AND (c.m_since_used_price_standardized is null or COALESCE(c.m_since_used_price_standardized,0) > 6) 
                      THEN 0
                  /*###############
                  USED and USED price available
                  #####################*/
                    WHEN (ss.asset_condition_spv = 'USED') 
                     AND c.used_price_standardized IS NOT NULL 
                      THEN c.used_price_standardized
                 /*###############
                  NEW but IN STOCK FOR 6m
                  #####################*/
          --            WHEN ss.asset_condition_spv = 'NEW' 
          --           AND COALESCE(ss.delivered_allocations, 0) = 0 
          --           AND ss.last_allocation_days_in_stock >= (6 * 30)  
          --           AND (c.m_since_neu_price is null or COALESCE(c.m_since_neu_price,0)) > 6
          --            THEN 0
                 /*###############
                  NEW was never delivered - 
                  new price if available
                  else greatest from agan price and depreciated purchase/price & rrp
                  #####################*/
                    WHEN ss.asset_condition_spv = 'NEW' 
                     AND COALESCE(ss.delivered_allocations, 0) = 0 
                     AND COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized, 
                     											(1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date::date) / 30)) * 
                     		COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL 
                     	 THEN COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized, 
                     	 										(1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date::date) / 30)) * 
                     	 										COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)))
                  /*###############
                  IN STOCK FOR 6m
                  #####################*/
                    WHEN (ss.asset_condition_spv = 'AGAN' )
     --               	 OR ss.asset_condition_spv = 'NEW') 
    --                	 AND COALESCE(ss.delivered_allocations, 0) > 0 
                    	 AND ss.last_allocation_days_in_stock >= (6 * 30) 
                    	 AND (c.m_since_agan_price_standardized is null or COALESCE(c.m_since_agan_price_standardized,0) > 6)  
                    	  THEN 0
                  /*###############
                  AGAN, greatest of used and initial price depreciated
                  #####################*/
                    WHEN (ss.asset_condition_spv = 'AGAN' 
                    	 OR ss.asset_condition_spv = 'NEW' 
                    	 AND COALESCE(ss.delivered_allocations, 0) > 0) 
                    	 AND COALESCE(c.agan_price_standardized, 
                    	 			GREATEST(c.used_price_standardized, 
                    	 					COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL 
                    	  THEN COALESCE(c.agan_price_standardized, 
                    	  		GREATEST(c.used_price_standardized, (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date::date) / 30)) * 
                    	  		COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)))
                    WHEN ((1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date) / 30)) * 
                    	COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)) <= 0 
                     THEN 0
                    ELSE (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)
                END AS residual_value_market_price,
                CASE
                    WHEN (ss.asset_condition_spv = 'USED')  
             		 AND c.m_since_used_price_standardized <= 1 
           			 THEN c.used_price_standardized
                    WHEN ss.asset_condition_spv = 'NEW' 
          			 AND COALESCE(ss.delivered_allocations, 0) = 0 
          			 AND c.m_since_neu_price <= 1 	
          			  THEN c.new_price_standardized
                    WHEN ((ss.asset_condition_spv = 'AGAN' OR ss.asset_condition_spv = 'NEW')
                     AND COALESCE(ss.delivered_allocations, 0) > 0) 
          			 AND c.m_since_agan_price_standardized <= 1 
          			  THEN c.agan_price_standardized
                    ELSE NULL
                END AS average_of_sources_on_condition_this_month,
                CASE
         			 WHEN ss.asset_condition_spv = 'AGAN' 
              		      OR ss.asset_condition_spv = 'NEW' 
               		     AND COALESCE(ss.delivered_allocations, 0) > 0 
                     THEN c.agan_price_standardized_before_discount
                    WHEN ss.asset_condition_spv = 'USED'  THEN c.used_price_standardized_before_discount
                    WHEN ss.asset_condition_spv = 'NEW' 
                    AND COALESCE(ss.delivered_allocations, 0) = 0 
                  THEN c.neu_price_before_discount
                    ELSE NULL
                END AS average_of_sources_on_condition_last_available_price,
                CASE
         			 WHEN ss.asset_condition_spv = 'AGAN' 
                     OR ss.asset_condition_spv = 'NEW' 
                     AND COALESCE(ss.delivered_allocations, 0) > 0 
                      THEN c.m_since_agan_price_standardized
                    WHEN ss.asset_condition_spv = 'USED' 
                     THEN c.m_since_used_price_standardized
                    WHEN ss.asset_condition_spv = 'NEW' 
                     AND COALESCE(ss.delivered_allocations, 0) = 0 
                      THEN c.m_since_neu_price
                    ELSE NULL
                END AS m_since_last_valuation_price,
                CASE
                    WHEN ss.asset_condition_spv = 'NEW' 
                     AND COALESCE(ss.delivered_allocations, 0) = 0 
                     THEN c.new_price_standardized
                    WHEN ss.asset_condition_spv = 'AGAN' 
                     OR ss.asset_condition_spv = 'NEW' 
                      AND COALESCE(ss.delivered_allocations, 0) > 0 
                     THEN c.agan_price_standardized
          			WHEN ss.asset_condition_spv = 'USED' 
                     THEN c.used_price_standardized
                    ELSE NULL
                END AS valuation_1,
                CASE
                    WHEN ss.asset_condition_spv = 'NEW' 
                    AND COALESCE(ss.delivered_allocations, 0) = 0 
                     THEN c.agan_price_standardized
                    WHEN ss.asset_condition_spv = 'AGAN' 
                     OR ss.asset_condition_spv = 'NEW' 
                      AND COALESCE(ss.delivered_allocations, 0) > 0 
                     THEN c.used_price_standardized
                    ELSE NULL
                END AS valuation_2,
           (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price) AS depreciated_purchase_price
           FROM master.asset_historical ss
           LEFT JOIN (select * from ods_spv_historical.price_per_condition where reporting_date::date= (CURRENT_DATE::date-1)) c 
  			ON c.product_sku = ss.product_sku
           LEFT JOIN e ON e.product_sku = ss.product_sku    
             where ss."date"::date= (CURRENT_DATE::date-1)     
         )
 SELECT distinct 
 	current_date::Date -1 as reporting_date,
 	f.asset_id,
        CASE
            WHEN f.residual_value_market_price < 0::double precision THEN 0::double precision
            ELSE f.residual_value_market_price
        END AS residual_value_market_price,
    NULL::text AS residual_value_market_price_label,
    f.average_of_sources_on_condition_this_month,
    f.m_since_last_valuation_price,
        CASE
            WHEN f.m_since_last_valuation_price is null or f.m_since_last_valuation_price>6 THEN '12.1- (c) - (iii)'::text
            ELSE '12.1- (c) - (i),(ii)'::text
        END AS valuation_method,
    f.valuation_1,
        CASE
            WHEN f.valuation_1 IS NULL THEN f.valuation_2
            ELSE NULL::numeric
        END AS valuation_2,
--    f.depreciated_purchase_price AS valuation_3,
   		case 
   			when f.depreciated_purchase_price<0::double precision THEN 0::double precision 
            else f.depreciated_purchase_price end AS valuation_3,
f.average_of_sources_on_condition_last_available_price
FROM f;
