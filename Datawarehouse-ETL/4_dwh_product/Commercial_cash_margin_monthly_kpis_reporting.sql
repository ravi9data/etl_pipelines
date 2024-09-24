-- dm_commercial.v_commercial_cash_monthly_kpis_reporting source
CREATE OR REPLACE VIEW dm_commercial.v_commercial_cash_monthly_kpis_reporting AS
WITH revenue_FROM_sub AS (
    SELECT LAST_DAY(paid_date::date)                                   			 AS reporting_date,
           coalesce(s.category_name, a.category_name,'Unknown')                  AS category_name,
	       coalesce(s.subcategory_name, a.subcategory_name,'Unknown')            AS subcategory_name,
           coalesce(s.product_sku, a.product_sku,'Unknown')    	               	 AS product_sku,
           CASE
               WHEN store_commercial IN ('Grover Germany', 'Grover International') 
               	THEN 'Grover'
               WHEN store_commercial IN ('Partnerships Germany', 'Partnerships International') 
               	THEN 'Retail'
               ELSE coalesce(s.store_commercial::text, 'Unknown') 
           END														  AS store_commercial_group,
           coalesce(sp.currency, 'Unknown')                           AS currency,
           coalesce(s.country_name, 'Unknown')                        AS country_name,
           sum(amount_paid)                                  		  AS subs_revenue_incl_shipment,
           sum(CASE 
           	   	WHEN a.purchased_date >= '2023-05-01' 
           	   		THEN amount_paid
           	   END) 												  AS subs_revenue_incl_shipment_may23_cohort,
           'Model Inputs'                                             AS tableau_data_source_workbook_name,
           'Collected Revenue'                                        AS tableau_data_source_tab_name,
           'ods_production.payment_all'                               AS redshift_datasource_1,
           'master.subscription'                                      AS redshift_datasource_2
    FROM master.subscription_payment sp 
    LEFT JOIN master.subscription s
    	ON s.subscription_id = sp.subscription_id
    LEFT JOIN master.asset a
		ON a.asset_id = sp.asset_id	
    WHERE paid_date >= '2021-01-01'
    GROUP BY coalesce(s.product_sku, a.product_sku,'Unknown'),
    		coalesce(s.category_name, a.category_name,'Unknown'),
    		store_commercial_group,
             LAST_DAY(paid_date::date), 
             sp.currency, 
             s.country_name, 
             coalesce(s.subcategory_name, a.subcategory_name,'Unknown')
)
--Depreciation (Portfolio Valuation) initial_price (all DPD values)
   , deprecation_ip AS (
    SELECT DISTINCT reporting_date::date                                     AS reporting_date, --always last day of month
                    coalesce(srh.category, 'Unknown')                        AS category_name,
                    coalesce(srh.product_sku, 'Unknown')                     AS product_sku,
                    coalesce(a.subcategory_name, 'Unknown')                  AS subcategory_name,
                    coalesce(a.currency, 'Unknown')                          AS currency,
                    CASE a.shipping_country
                        WHEN 'The Netherlands' 
                        	THEN 'Netherlands'
                        WHEN 'Soft Deleted Record' 
                        	THEN 'Unknown'
                        WHEN 'DE' 
                        	THEN 'Germany'
                        WHEN 'Österreich' 
                        	THEN 'Austria'
                        WHEN 'AT' 
                        	THEN 'Austria'
                        ELSE COALESCE(a.shipping_country, 'Unknown')
                    END                                                	AS country_name,
                    CASE
                        WHEN store_commercial IN ('Grover Germany', 'Grover International') 
                        	THEN'Grover'
                        WHEN store_commercial IN ('Partnerships Germany', 'Partnerships International') 
                        	THEN'Retail'
                        ELSE coalesce(store_commercial::text, 'Unknown') 
                    END														AS store_commercial_group,
                    round(sum(srh.initial_price), 2::numeric)               AS initial_price,
                    round(sum(CASE 
           	   					  WHEN srh.purchased_date >= '2023-05-01' 
           	   						THEN srh.initial_price
           	   				  END)) 										AS initial_price_may23_cohort,
                    'Model Inputs'                                          AS tableau_data_source_workbook_name,
                    'Portfolio Valuation'                                   AS tableau_data_source_tab_name,
                    'dm_finance.spv_report_historical'                      AS redshift_datasource_1,
                    ''                                                      AS redshift_datasource_2
    FROM dm_finance.spv_report_historical srh
    LEFT JOIN (
    			SELECT DISTINCT asset_id, date, currency, shipping_country,subcategory_name 
    			FROM master.asset_historical
                WHERE date= last_day(date)
               ) a 
    	ON srh.asset_id = a.asset_id 
    	AND srh.reporting_date = a.date
    WHERE srh.warehouse NOT IN('office_us','ups_softeon_us_kylse')
      --dpd bucket = all for initial price
    	AND reporting_date >= '2021-01-01'
    GROUP BY reporting_date, 
    		srh.category, 
    		srh.product_sku, 
    		store_commercial_group, 
    		a.subcategory_name,
             a.currency,
             country_name
)
       --Depreciation (Portfolio Valuation) market_price and monthly_depr_adjusted  (limited list of DPD values)
, 
impairments_rate_sub AS (
	SELECT DISTINCT 
		asset_id, 
		date, 
		currency, 
		shipping_country,
		subcategory_name,
		CASE
			WHEN dpd_bucket IN  ('LOST','WRITTEN OFF') THEN 1
			WHEN last_allocation_dpd BETWEEN 91 AND 180 THEN 0.25
			WHEN last_allocation_dpd BETWEEN 181 AND 270 THEN 0.5
			WHEN last_allocation_dpd BETWEEN 271 AND 360 THEN 0.75
			WHEN last_allocation_dpd >= 361 THEN 1
			ELSE 0
			END AS impairment_rate
	FROM master.asset_historical
	WHERE date = last_day(date)
)
   , deprecation AS (
    SELECT DISTINCT reporting_date::date                                     AS reporting_date, --always last day of month
                    coalesce(srh.category, 'Unknown')                        AS category_name,
                    coalesce(srh.product_sku, 'Unknown')                     AS product_sku,
                    coalesce(a.subcategory_name, 'Unknown')                  AS subcategory_name,
                    coalesce(a.currency, 'Unknown')                          AS currency,
                    CASE a.shipping_country
                        WHEN 'The Netherlands' 
                        	THEN 'Netherlands'
                        WHEN 'Soft Deleted Record' 
                        	THEN 'Unknown'
                        WHEN 'DE' 
                        	THEN 'Germany'
                        WHEN 'Österreich' 
                        	THEN 'Austria'
                        WHEN 'AT' 
                        	THEN'Austria'
                        ELSE COALESCE(a.shipping_country, 'Unknown')
                    END		                                                AS country_name,
                    CASE
                        WHEN store_commercial IN ('Grover Germany', 'Grover International') 
                        	THEN 'Grover'
                        WHEN store_commercial IN ('Partnerships Germany', 'Partnerships International') 
                        	THEN 'Retail'
                        ELSE coalesce(store_commercial::text, 'Unknown') 
                    END													AS store_commercial_group,
                    round(sum(final_price), 2::numeric) AS market_price,   --renamed according to Tableau workbook
                    --monthly_depr_adjusted KPI logic FROM Tableau:
                    round(sum(final_price) - sum(coalesce(prev_final_price, srh.initial_price, prev_final_price)), 2::numeric) AS monthly_depr_adjusted,
					round(sum(-a.impairment_rate*((final_price::decimal))),2) 							   AS monthly_impairment,
                          			--(nvl(prev_final_price::decimal, srh.initial_price::decimal, prev_final_price::decimal)))), 2) AS monthly_impairment,
                    monthly_depr_adjusted::decimal + monthly_impairment::decimal 						   AS total_loss,  
                    round(sum(CASE 
           	   					  WHEN srh.purchased_date >= '2023-05-01' 
           	   						THEN srh.final_price
           	   				  END), 2::numeric) 														   AS market_price_may23_cohort,
           	   		round(sum(CASE 
           	   					WHEN srh.purchased_date >= '2023-05-01'
           	   						THEN (final_price) - coalesce(prev_final_price, srh.initial_price, prev_final_price) 
							  END), 2::numeric) 														   AS monthly_depr_adjusted_may23_cohort,	
					round(sum(CASE 
           	   					WHEN srh.purchased_date >= '2023-05-01'
           	   						THEN (-a.impairment_rate*((final_price::decimal))) 
           	   				  END),2) 																	   AS monthly_impairment_may23_cohort,	
           	   		monthly_depr_adjusted_may23_cohort::decimal + monthly_impairment_may23_cohort::decimal AS total_loss_may23_cohort,	  
                    'Model Inputs'                                          							   AS tableau_data_source_workbook_name,
                    'Portfolio Valuation'                                   							   AS tableau_data_source_tab_name,
                    'dm_finance.spv_report_historical'                      							   AS redshift_datasource_1,
                    ''                                                     								   AS redshift_datasource_2
    FROM dm_finance.spv_report_historical srh
    LEFT JOIN impairments_rate_sub a 
    	ON srh.asset_id = a.asset_id 
    	AND srh.reporting_date = a.date
    WHERE  srh.warehouse NOT IN ('office_us','ups_softeon_us_kylse')
      AND reporting_date >= '2021-01-01'
    GROUP BY reporting_date, 
    		srh.category, 
    		srh.product_sku, 
    		store_commercial_group, 
    		a.subcategory_name,
             a.currency,
             country_name
)
   --Returned Allocation by Category:
   , returned_allocations AS (
    SELECT coalesce(asset.category_name, 'Unknown')                 AS category_name,
           coalesce(asset.subcategory_name, 'Unknown')              AS subcategory_name,
           coalesce(asset.currency, 'Unknown')                      AS currency,
           coalesce(subscription.country_name, 'Unknown')           AS country_name,
           coalesce(asset.product_sku, 'Unknown')                   AS product_sku,
           CASE
               WHEN store_commercial IN ('Grover Germany', 'Grover International') 
               	THEN 'Grover'
               WHEN store_commercial IN ('Partnerships Germany', 'Partnerships International') 
               	THEN 'Retail'
               ELSE coalesce(store_commercial::text, 'Unknown') 
           END														AS store_commercial_group,
           LAST_DAY(allocation.return_delivery_date::date)          AS reporting_date,
           count(DISTINCT allocation.allocation_id)                 AS returned_allocation,
           count(DISTINCT CASE 
           				  	  WHEN asset.purchased_date >= '2023-05-01'
           				  	  	THEN allocation.allocation_id
           				  END) 										AS returned_allocation_may23_cohort,
           'Model Inputs'                                           AS tableau_data_source_workbook_name,
           'Returned Incremental'                                   AS tableau_data_source_tab_name,
           'master.asset'                                           AS redshift_datasource_1,
           'master.subscription'                                    AS redshift_datasource_2
    FROM "master"."allocation" "allocation"
    INNER JOIN "master"."asset" "asset" 
    	ON ("allocation"."asset_id" = "asset"."asset_id")
    LEFT JOIN "master"."subscription" "subscription"
        ON ("allocation"."subscription_id" = "subscription"."subscription_id")
    WHERE
          --allocation.customer_type in ('normal_customer', 'business_customer') and
       return_delivery_date::date >= '2021-01-01'
    	AND country_name NOT IN ('United States','United Kingdom')
    GROUP BY asset.product_sku, 
    		asset.category_name, 
    		store_commercial_group, 
    		asset.subcategory_name,
             subscription.country_name, asset.currency,
             LAST_DAY(allocation.return_delivery_date::date)
)
,
   -- --Delivered Allocation by Category:
    delivered_allocations AS (
        SELECT coalesce(asset.category_name, 'Unknown')                 AS category_name,
               coalesce(asset.subcategory_name, 'Unknown')              AS subcategory_name,
               coalesce(asset.currency, 'Unknown')                      AS currency,
               coalesce(subscription.country_name, 'Unknown')           AS country_name,
               coalesce(asset.product_sku, 'Unknown')                   AS product_sku,
               CASE
                   WHEN store_commercial IN ('Grover Germany', 'Grover International') 
                   	THEN 'Grover'
                   WHEN store_commercial IN ('Partnerships Germany', 'Partnerships International') 
                   	THEN 'Retail'
                   ELSE coalesce(store_commercial::text, 'Unknown') 
               END														AS store_commercial_group,
               LAST_DAY(allocation.delivered_at::date)                  AS reporting_date,
               count(DISTINCT allocation.allocation_id)                 AS delivered_allocations,
               count(DISTINCT CASE 
           				  	    WHEN asset.purchased_date >= '2023-05-01'
           				  	  	  THEN allocation.allocation_id
           				 	  END) 										AS delivered_allocations_may23_cohort,
               'Model Inputs'                                           AS tableau_data_source_workbook_name,
               'Returned Incremental'                                   AS tableau_data_source_tab_name,
               'master.asset'                                           AS redshift_datasource_1,
               'master.subscription'                                    AS redshift_datasource_2
        FROM "master"."allocation" "allocation"
        INNER JOIN "master"."asset" "asset" 
        	ON ("allocation"."asset_id" = "asset"."asset_id")
        LEFT JOIN "master"."subscription" "subscription"
            ON ("allocation"."subscription_id" = "subscription"."subscription_id")
         WHERE
         -- allocation.customer_type in ('normal_customer', 'business_customer') and
           delivered_at::date>= '2021-01-01'
          AND country_name  NOT IN ('United States','United Kingdom')
        GROUP BY asset.product_sku, asset.category_name, store_commercial_group, asset.subcategory_name,
                 subscription.country_name, asset.currency,
                 LAST_DAY(allocation.delivered_at::date)
)
, utilization_rates AS (
    SELECT LAST_DAY(u.reporting_date::date)        AS reporting_date,
           coalesce(u.category_name, 'Unknown')    AS category_name,
           coalesce(u.subcategory_name, 'Unknown') AS subcategory_name,
           coalesce(u.product_sku, 'Unknown')      AS product_sku,
           --hardcoding values for the following splits as they don't exist in the source table
           'Unknown'                               AS store_commercial_group,
           'EUR'                                   AS currency,
           'Unknown'                               AS country_name,
           round(1.0::double precision  -
                 (sum(purchase_price_in_stock) /
                  NULLIF((sum(purchase_price_in_stock) + sum(purchase_price_on_rent)), 0)),
                 5)
                                                   AS marketable_utilization_rate_by_product_sku,
           round(1.0::double precision - ((sum(purchase_price_in_stock) +sum(purchase_price_inbound) + sum(purchase_price_refurbishment)) / NULLIF(
                   (sum(purchase_price_in_stock) +sum(purchase_price_inbound) + sum(purchase_price_refurbishment) + sum(purchase_price_on_rent)),
                   0)),
                 5)
                                                   AS inventory_utilization_rate_by_product_sku, -----here  the changes FOR BI-6044
           sum(purchase_price_in_stock)            AS purchase_price_in_stock,
           sum(purchase_price_on_rent)             AS purchase_price_on_rent,
           sum(purchase_price_refurbishment)       AS purchase_price_refurbishment,
           round(1.0::double precision -
		   (sum(CASE
					WHEN u.purchased_date >= '2023-05-01'
						THEN purchase_price_in_stock END) /
		   NULLIF(sum(CASE 
						WHEN u.purchased_date >= '2023-05-01' 
							THEN purchase_price_in_stock+purchase_price_on_rent
					 END ), 0)),5) 				   AS marketable_utilization_rate_by_product_sku_may23_cohort,
           round(1.0::double precision - 
           (sum(CASE 
			       WHEN u.purchased_date >= '2023-05-01' 
		   			   THEN COALESCE(purchase_price_in_stock,0) + COALESCE(purchase_price_inbound,0) + COALESCE(purchase_price_refurbishment,0) END) /
		   NULLIF(sum(CASE 
			   		  	  WHEN u.purchased_date >= '2023-05-01' 
			   		  	  	  THEN COALESCE(purchase_price_in_stock,0) + COALESCE(purchase_price_inbound,0) + COALESCE(purchase_price_refurbishment,0)+ COALESCE(purchase_price_on_rent,0) 
					  END),0)),5) 			   AS inventory_utilization_rate_by_product_sku_may23_cohort, 
           sum(CASE 
           	       	WHEN u.purchased_date >= '2023-05-01'
           	       		THEN purchase_price_in_stock
           	   END) AS purchase_price_in_stock_may23_cohort, 		
           sum(CASE 
           	       	WHEN u.purchased_date >= '2023-05-01'
           	       		THEN purchase_price_on_rent
           	   END) AS purchase_price_on_rent_may23_cohort,	       					
           sum(CASE 
           	       	WHEN u.purchased_date >= '2023-05-01'
           	       		THEN purchase_price_refurbishment
           	   END) AS purchase_price_refurbishment_may23_cohort,
           'Inventory Efficiency'                  AS tableau_data_source_workbook_name,
           'Asset Utilization'                     AS tableau_data_source_tab_name,
            'dwh.utilization'                      AS redshift_datasource_1,
            ''                                     AS redshift_datasource_2
	FROM dwh.utilization u
    WHERE warehouse NOT IN ('office_us','ups_softeon_us_kylse')
      --limiting reporting_date to only last days of the months for consistency with the other subqueries
    	AND reporting_date::date >= '2021-01-01' 
    	AND reporting_date::date = LAST_DAY(reporting_date)
    GROUP BY 1,2,3,4,5,6,7
)
, repair_costs AS (
    SELECT 
   	  coalesce(LAST_DAY(r.outbound_date::date), LAST_DAY(rce.date_updated::date), LAST_DAY(rce.date_offer::date), LAST_DAY(r.cost_estimate_date::date), NULL)::date 
   	  												AS reporting_date,
   	  coalesce(r.category_name, 'Unknown')    		AS category_name,
       coalesce(r.subcategory_name, 'Unknown') 		AS subcategory_name,
       coalesce(r.product_sku, 'Unknown')      		AS product_sku,
       -- hardcoding values for the following splits as they don't exist in the source table
       CASE
       	WHEN ao.store_commercial IN ('Grover Germany', 'Grover International') 
       		THEN 'Grover'
       	WHEN ao.store_commercial IN ('Partnerships Germany', 'Partnerships International') 
       		THEN 'Retail'
       	ELSE coalesce(ao.store_commercial::text, 'Unknown') 
       END 											AS store_commercial_group,
       'EUR'::VARCHAR(12)			                AS currency,
       coalesce(ao.store_country, 'Unknown')     	AS country_name,
       sum(r.repair_price) 							AS total_repair_costs_eur,
       sum(CASE 
           	       	WHEN ao.purchased_date >= '2023-05-01'
           	       		THEN r.repair_price
           	   END) 								AS total_repair_costs_eur_may23_cohort
	FROM dm_recommerce.repair_invoices r
	LEFT JOIN dm_recommerce.repair_cost_estimates rce 
		ON r.kva_id = rce.kva_id 
	LEFT JOIN 
			(
			SELECT
				a.serial_number,
				a.currency, -- NOT needed RIGHT now AS ALL repair costs FROM this SOURCE ARE IN EUR (backup location)
				o.store_commercial, 
				o.store_country,
				a.purchased_date 
			FROM master.asset a 
			LEFT JOIN master."order" o 
				ON a.first_order_id = o.order_id 
			) ao 
		ON ao.serial_number = r.serial_number  
	WHERE reporting_date >= '2021-01-01'
		AND country_name NOT IN ('United States','United Kingdom')
	GROUP BY 1,2,3,4,5,6,7
)
, dimensions AS (
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM revenue_FROM_sub
    UNION
    DISTINCT
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM deprecation
    UNION
    DISTINCT
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM deprecation_ip
    UNION
    DISTINCT
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM delivered_allocations
    UNION
    DISTINCT
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM returned_allocations
    UNION
    DISTINCT
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM utilization_rates
    UNION
    DISTINCT
    SELECT reporting_date,
           category_name,
           subcategory_name,
           product_sku,
           currency,
           country_name,
           store_commercial_group
    FROM repair_costs
)
--main output query
SELECT DISTINCT --dimensions (all possible groups)
                d.reporting_date,
                d.category_name,
                d.subcategory_name,
                d.product_sku,
                d.currency,
                d.country_name,
                d.store_commercial_group,
                --metrics
                subs_revenue_incl_shipment AS subs_revenue_incl_shipment,
                subs_revenue_incl_shipment_may23_cohort,
                initial_price              AS initial_price,
                initial_price_may23_cohort,
                market_price               AS market_price,
                market_price_may23_cohort,
                de.monthly_depr_adjusted   AS monthly_depr_adjusted,
                de.monthly_depr_adjusted_may23_cohort,
                de.monthly_impairment,
                de.monthly_impairment_may23_cohort,
                de.total_loss,
                de.total_loss_may23_cohort,
                returned_allocation        AS returned_allocations,
                returned_allocation_may23_cohort,
                delivered_allocations      AS delivered_allocations,
                delivered_allocations_may23_cohort,
                purchase_price_in_stock,
                purchase_price_in_stock_may23_cohort,
                purchase_price_on_rent,
                purchase_price_on_rent_may23_cohort,
                purchase_price_refurbishment,
                purchase_price_refurbishment_may23_cohort,
                inventory_utilization_rate_by_product_sku,
                inventory_utilization_rate_by_product_sku_may23_cohort,
                marketable_utilization_rate_by_product_sku,
                marketable_utilization_rate_by_product_sku_may23_cohort,
                total_repair_costs_eur,
                total_repair_costs_eur_may23_cohort,
                current_timestamp          AS last_updated_ts
FROM dimensions d
LEFT JOIN revenue_from_sub s
	ON (d.reporting_date = s.reporting_date 
		AND d.category_name = s.category_name 
		AND d.subcategory_name = s.subcategory_name
        AND d.currency = s.currency 
        AND d.country_name = s.country_name 
        AND d.product_sku = s.product_sku 
        AND d.store_commercial_group = s.store_commercial_group)
LEFT JOIN deprecation_ip dip
	ON (d.reporting_date = dip.reporting_date 
		AND d.category_name = dip.category_name 
		AND d.subcategory_name = dip.subcategory_name
        AND d.currency = dip.currency 
        AND d.country_name = dip.country_name 
        AND d.product_sku = dip.product_sku 
        AND d.store_commercial_group = dip.store_commercial_group)
LEFT JOIN deprecation de
    ON (d.reporting_date = de.reporting_date 
    	AND d.category_name = de.category_name 
    	AND d.subcategory_name = de.subcategory_name
        AND d.currency = de.currency 
        AND d.country_name = de.country_name 
        AND d.product_sku = de.product_sku 
        AND d.store_commercial_group = de.store_commercial_group)
LEFT JOIN returned_allocations r
    ON (d.reporting_date = r.reporting_date 
    	AND d.category_name = r.category_name 
    	AND d.subcategory_name = r.subcategory_name
        AND d.currency = r.currency 
        AND d.country_name = r.country_name 
        AND d.product_sku = r.product_sku 
        AND d.store_commercial_group = r.store_commercial_group)
LEFT JOIN delivered_allocations da
    ON (d.reporting_date = da.reporting_date 
    	AND d.category_name = da.category_name 
    	AND d.subcategory_name = da.subcategory_name
        AND d.currency = da.currency 
        AND d.country_name = da.country_name 
        AND d.product_sku = da.product_sku 
        AND d.store_commercial_group = da.store_commercial_group)
LEFT JOIN utilization_rates u
    ON (d.reporting_date = u.reporting_date 
    	AND d.category_name = u.category_name 
    	AND	d.subcategory_name = u.subcategory_name
        AND d.currency = u.currency 
        AND d.country_name = u.country_name 
        AND d.product_sku = u.product_sku 
        AND d.store_commercial_group = u.store_commercial_group)
LEFT JOIN repair_costs rc
    ON (d.reporting_date = rc.reporting_date 
    	AND d.category_name = rc.category_name 
    	AND d.subcategory_name = rc.subcategory_name
        AND d.currency = rc.currency 
        AND d.country_name = rc.country_name 
        AND d.product_sku = rc.product_sku 
        AND d.store_commercial_group = rc.store_commercial_group)
GROUP BY d.reporting_date,
         d.category_name,
         d.subcategory_name,
         d.product_sku,
         d.currency,
         d.country_name,
         d.store_commercial_group,
--metrics
         subs_revenue_incl_shipment,
         subs_revenue_incl_shipment_may23_cohort,
         initial_price,
         initial_price_may23_cohort,
         market_price,
         market_price_may23_cohort,
         monthly_depr_adjusted,
         monthly_depr_adjusted_may23_cohort,
         returned_allocation,
         returned_allocation_may23_cohort,
         delivered_allocations,
         delivered_allocations_may23_cohort,
         purchase_price_in_stock,
         purchase_price_in_stock_may23_cohort,
         purchase_price_on_rent,
         purchase_price_on_rent_may23_cohort,
         purchase_price_refurbishment,
         purchase_price_refurbishment_may23_cohort,
         inventory_utilization_rate_by_product_sku,
         inventory_utilization_rate_by_product_sku_may23_cohort,
         marketable_utilization_rate_by_product_sku,
         marketable_utilization_rate_by_product_sku_may23_cohort,
         total_repair_costs_eur,
         total_repair_costs_eur_may23_cohort,
		 de.monthly_impairment,
		 de.monthly_impairment_may23_cohort,
         de.monthly_depr_adjusted,
         de.monthly_depr_adjusted_may23_cohort,
         de.total_loss,
         de.total_loss_may23_cohort
WITH NO SCHEMA binding;
