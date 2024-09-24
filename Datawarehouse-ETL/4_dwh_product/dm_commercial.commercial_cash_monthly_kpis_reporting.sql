DROP TABLE IF EXISTS populate_fields;
CREATE TEMP TABLE populate_fields
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
WITH assets_null_info AS (
	SELECT DISTINCT 
		asset_id 
	FROM master.asset_historical f
	WHERE LAST_DAY(f."date") >= DATE_ADD('month',-3, current_date) -- LAST 3 months 
      AND f."date" = LAST_DAY(f."date") -- ONLY LAST DAY OF the MONTH
  	  AND (asset_allocation_id IS NULL OR subscription_id IS NULL)
)
, populate_fields_prep AS (
	SELECT 
		"date",
		asset_id,
		asset_allocation_id,
		subscription_id
	FROM master.asset_historical a
	WHERE 
--	"date" >= DATE_ADD('month',-6, current_date)
--		AND 
		(asset_allocation_id IS NOT NULL OR subscription_id IS NOT NULL)
		AND EXISTS (SELECT NULL FROM assets_null_info n WHERE n.asset_id = a.asset_id)
)
, populate_fields_prep_row_1 AS (
	SELECT 
		f."date" AS reporting_date,
		f.asset_id,
		f.category_name AS category_name ,
		f.subcategory_name AS subcategory_name,
		COALESCE(o.store_country, s.country_name) AS country,
		COALESCE(o.customer_type, s.customer_type) AS customer_type,
		COALESCE(f.asset_allocation_id, p.asset_allocation_id) AS asset_allocation_id,
		COALESCE(f.subscription_id, p.subscription_id, a.subscription_id) AS subscription_id,
		f.asset_status_new,
		f.asset_status_original,
		f.initial_price,
		f.serial_number,
		f.created_at AS created_at_serial_number,
		ROW_NUMBER() OVER (PARTITION BY f.asset_id, f."date" ORDER BY p."date" DESC) AS row_num
	FROM master.asset_historical f 
	LEFT JOIN populate_fields_prep p 
		ON f.asset_id = p.asset_id 
		AND f."date" >= p."date"
	LEFT JOIN ods_production.allocation a
		ON COALESCE(f.asset_allocation_id, p.asset_allocation_id) = a.allocation_id
	LEFT JOIN master.ORDER o 
		ON COALESCE(a.order_id, f.first_order_id) = o.order_id 
	LEFT JOIN master.subscription s 
		ON COALESCE(f.subscription_id, p.subscription_id, a.subscription_id, f.active_subscription_id) = s.subscription_id
	WHERE LAST_DAY(f."date") >= DATE_ADD('month',-3, current_date)
      AND f."date" = LAST_DAY(f."date") -- ONLY LAST DAY OF the MONTH
      AND COALESCE(o.store_country, s.country_name, 'Unknown') NOT IN ('United States', 'United Kingdom')
 )
 , serial_number AS (
	 SELECT 
	 	*,
	 	ROW_NUMBER() OVER (PARTITION BY f.serial_number, f.reporting_date  ORDER BY f.created_at_serial_number) AS row_num_serial_number
	 FROM populate_fields_prep_row_1 f
	 WHERE row_num = 1	 
)
SELECT 
	reporting_date,
	asset_id,
	category_name ,
	subcategory_name,
	country,
	customer_type,
	asset_allocation_id,
	subscription_id,
	asset_status_new,
	asset_status_original,
	initial_price,
	CASE WHEN row_num_serial_number = 1 THEN serial_number END AS serial_number
FROM serial_number
;






DROP TABLE IF EXISTS purchase_prices;
CREATE TEMP TABLE purchase_prices
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
	SELECT DISTINCT 
		f.reporting_date,
		f.asset_id,
		f.category_name ,
		f.subcategory_name ,
		f.country,
		f.customer_type,
		sum(CASE
	            WHEN f.asset_status_new in ('IN STOCK') THEN f.initial_price else 0
	        	END) as purchase_price_in_stock,
	    sum(CASE
	            WHEN f.asset_status_new in ('PERFORMING','AT RISK','NOT AVAILABLE') THEN f.initial_price  else 0
	        	END) as purchase_price_on_rent,     
		sum(CASE
	            WHEN f.asset_status_new = 'REFURBISHMENT' --before Jan 12, in repair and irreparable were in this group
	                and f.asset_status_original not in ('IN REPAIR', 'IRREPARABLE') THEN f.initial_price  else 0
	        	END) as purchase_price_refurbishment,
	    sum(CASE
	            WHEN f.asset_status_original = 'IN REPAIR' THEN f.initial_price  else 0
	        	END) as purchase_price_in_repair
	FROM populate_fields f
	GROUP BY 1,2,3,4,5,6
;




DROP TABLE IF EXISTS initial_price;
CREATE TEMP TABLE initial_price
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
    SELECT DISTINCT 
    	srh.reporting_date::date AS reporting_date, --always last day of month
        srh.asset_id,
        coalesce(srh.category, a.category_name) AS category_name,
        a.subcategory_name AS subcategory_name,
        a.country AS country,
        a.customer_type AS customer_type,
        round(sum(srh.initial_price), 2::numeric) AS initial_price
    FROM dm_finance.spv_report_historical srh
	LEFT JOIN populate_fields a 
    	ON srh.asset_id = a.asset_id 
    	AND srh.reporting_date = a.reporting_date
    WHERE srh.reporting_date >= DATE_ADD('month',-3, current_date)
    	AND coalesce(a.country,'Unknown') NOT IN ('United States', 'United Kingdom')
    GROUP BY 1,2,3,4,5,6
;

 


DROP TABLE IF EXISTS returned_allocations;
CREATE TEMP TABLE returned_allocations
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
SELECT 
	LAST_DAY(allocation.return_delivery_date::date) AS reporting_date,
	asset_id,
	p2.category_name AS category_name,
	p2.subcategory_name AS subcategory_name,
	subscription.country_name AS country,
	subscription.customer_type AS customer_type,
	count(DISTINCT allocation.allocation_id) AS returned_allocations
FROM "master"."allocation" "allocation"
LEFT JOIN ods_production.product p2 
    	ON p2.product_sku = "allocation".product_sku
    LEFT JOIN "master"."subscription" "subscription"
        ON ("allocation"."subscription_id" = "subscription"."subscription_id")
WHERE 
    (LAST_DAY(allocation.return_delivery_date::date) >= DATE_ADD('month',-3, current_date) 
    	AND LAST_DAY(allocation.return_delivery_date::date) <= current_date)
    AND coalesce(subscription.country_name, 'Unknown')  NOT IN ('United States','United Kingdom')
GROUP BY 1,2,3,4,5,6
;


DROP TABLE IF EXISTS delivery_allocations;
CREATE TEMP TABLE delivery_allocations
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
SELECT 
	LAST_DAY(allocation.delivered_at::date) AS reporting_date,
	asset_id,
	p2.category_name AS category_name,
	p2.subcategory_name AS subcategory_name,
	subscription.country_name AS country,
	subscription.customer_type AS customer_type,
	count(DISTINCT allocation.allocation_id) AS delivered_allocations
FROM "master"."allocation" "allocation"
LEFT JOIN ods_production.product p2 
    	ON p2.product_sku = "allocation".product_sku
    LEFT JOIN "master"."subscription" "subscription"
        ON ("allocation"."subscription_id" = "subscription"."subscription_id")
WHERE 
	(LAST_DAY(allocation.delivered_at::date) >= DATE_ADD('month',-3, current_date)
    	AND LAST_DAY(allocation.delivered_at::date) <= current_date)
    AND coalesce(subscription.country_name, 'Unknown')  NOT IN ('United States','United Kingdom')
GROUP BY 1,2,3,4,5,6
;



DROP TABLE IF EXISTS revenue_subs;
CREATE TEMP TABLE revenue_subs
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
    SELECT 
       LAST_DAY(paid_date::date) AS reporting_date,
       pa.asset_id,
       COALESCE(s.category_name,pf.category_name)  AS category_name,
       COALESCE(s.subcategory_name, pf.subcategory_name) AS subcategory_name,
       COALESCE(s.customer_type, pf.customer_type) AS customer_type,
       COALESCE(s.country_name, pf.country) AS country,
       sum(pa.amount_paid) AS subs_revenue_incl_shipment,
       sum(CASE WHEN pa.payment_type NOT IN 
       		('GROVER SOLD','CUSTOMER BOUGHT') 
       		THEN pa.amount_paid END) AS subs_revenue_incl_shipment_without_sold 
    FROM ods_production.payment_all pa
    LEFT JOIN master.subscription s
    	ON s.subscription_id = pa.subscription_id
    LEFT JOIN populate_fields pf 
    	ON pa.asset_id = pf.asset_id
    	AND LAST_DAY(paid_date::date) = pf.reporting_date
    WHERE payment_type IN (	'CHARGE BACK SUBSCRIPTION_PAYMENT', 
    						'REFUND SUBSCRIPTION_PAYMENT', 
    						'SHIPMENT', 
    						'RECURRENT', 
    						'FIRST',
    						'CUSTOMER BOUGHT',
    						'GROVER SOLD')
    	AND LAST_DAY(paid_date::date) >= DATE_ADD('month',-3, current_date)
    	AND LAST_DAY(paid_date::date) <= current_date
    	AND coalesce(s.country_name, 'Unknown')  NOT IN ('United States','United Kingdom')
    GROUP BY 1,2,3,4,5,6
;





DROP TABLE IF EXISTS repair_cost;
CREATE TEMP TABLE repair_cost
SORTKEY(reporting_date,	asset_id)
DISTKEY(asset_id)
AS
SELECT
	coalesce(LAST_DAY(rce.date_updated), 
		LAST_DAY(rce.date_offer), 
		LAST_DAY(r.cost_estimate_date),
		LAST_DAY(r.outbound_date))::date AS reporting_date,
	a.asset_id,
   	a.category_name,
   	a.subcategory_name,
    a.country,
    a.customer_type,
    sum(r.repair_price) AS total_repair_costs_eur
FROM dm_recommerce.repair_invoices r 
LEFT JOIN dm_recommerce.repair_cost_estimates rce 
		ON r.kva_id = rce.kva_id 
LEFT JOIN populate_fields a 
	ON r.serial_number = a.serial_number 
	AND coalesce(LAST_DAY(rce.date_updated), 
		LAST_DAY(rce.date_offer), 
		LAST_DAY(r.cost_estimate_date),
		LAST_DAY(r.outbound_date))::date = a.reporting_date
WHERE coalesce(LAST_DAY(rce.date_updated), 
		LAST_DAY(rce.date_offer), 
		LAST_DAY(r.cost_estimate_date),
		LAST_DAY(r.outbound_date))::date >= DATE_ADD('month',-3, current_date)
		AND 
		coalesce(LAST_DAY(rce.date_updated), 
		LAST_DAY(rce.date_offer), 
		LAST_DAY(r.cost_estimate_date),
		LAST_DAY(r.outbound_date))::date = 
						LAST_DAY(coalesce(LAST_DAY(rce.date_updated), 
									LAST_DAY(rce.date_offer), 
									LAST_DAY(r.cost_estimate_date),
									LAST_DAY(r.outbound_date)))::date -- ONLY LAST DAY OF the MONTH
		AND 
		coalesce(LAST_DAY(rce.date_updated), 
		LAST_DAY(rce.date_offer), 
		LAST_DAY(r.cost_estimate_date),
		LAST_DAY(r.outbound_date))::date <= current_date
GROUP BY 1,2,3,4,5,6
;





DROP TABLE IF EXISTS dm_commercial.commercial_cash_monthly_kpis_reporting;
CREATE TABLE dm_commercial.commercial_cash_monthly_kpis_reporting AS
WITH dimensions AS (
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM populate_fields
	UNION 
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM revenue_subs
	UNION 
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM returned_allocations 
	UNION
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM delivery_allocations 
	UNION
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM initial_price 
	UNION
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM purchase_prices
	UNION
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		category_name,
		subcategory_name,
		customer_type,
		country
	FROM repair_cost
)
, dimensions_prep AS (
	SELECT DISTINCT 
		reporting_date,
		asset_id,
		COALESCE(MAX(category_name),'Unknown') as category_name ,
		COALESCE(max(subcategory_name),'Unknown') AS subcategory_name,
		COALESCE(max(customer_type),'Unknown') AS customer_type,
		COALESCE(max(country),'Unknown') AS country
	FROM dimensions
	GROUP BY 1,2
)
SELECT
	dc.reporting_date,
	dc.country,
	CASE WHEN dc.customer_type = 'business_customer' THEN 'B2B' 
		 WHEN dc.customer_type = 'normal_customer' THEN 'B2C'
		 ELSE dc.customer_type
		 END AS customer_type,
	dc.category_name,
	dc.subcategory_name,
	COALESCE(sum(re.subs_revenue_incl_shipment),0) AS subs_revenue_incl_shipment,
   	COALESCE(sum(re.subs_revenue_incl_shipment_without_sold),0) AS subs_revenue_incl_shipment_without_sold,
   	COALESCE(sum(ra.returned_allocations),0) AS returned_allocations,
  	COALESCE(sum(da.delivered_allocations),0) AS delivered_allocations,
   	COALESCE(sum(ip.initial_price),0) AS initial_price,
   	COALESCE(sum(pp.purchase_price_in_stock),0) AS purchase_price_in_stock,
	COALESCE(sum(pp.purchase_price_on_rent),0) AS purchase_price_on_rent,     
	COALESCE(sum(pp.purchase_price_refurbishment),0) AS purchase_price_refurbishment,
	COALESCE(sum(pp.purchase_price_in_repair),0) AS purchase_price_in_repair,
	COALESCE(sum(rc.total_repair_costs_eur),0) AS total_repair_costs_eur
FROM dimensions_prep dc
LEFT JOIN revenue_subs re
	ON dc.reporting_date = re.reporting_date
	AND dc.asset_id = re.asset_id
LEFT JOIN returned_allocations ra
	ON dc.reporting_date = ra.reporting_date
	AND dc.asset_id = ra.asset_id
LEFT JOIN delivery_allocations da
	ON dc.reporting_date = da.reporting_date
	AND dc.asset_id = da.asset_id
LEFT JOIN initial_price ip
	ON dc.reporting_date = ip.reporting_date
	AND dc.asset_id = ip.asset_id
LEFT JOIN purchase_prices pp
	ON dc.reporting_date = pp.reporting_date
	AND dc.asset_id = pp.asset_id
LEFT JOIN repair_cost rc
	ON dc.reporting_date = rc.reporting_date
	AND dc.asset_id = rc.asset_id
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
;
