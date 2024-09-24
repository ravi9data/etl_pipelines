BEGIN;

TRUNCATE TABLE master.asset;

INSERT into master.asset
with fso as (
SELECT DISTINCT
    a.asset_id,
    s.order_id,
    COALESCE(st.id,null) AS first_allocation_store_id,
    COALESCE(st.store_name, 'NOT AVAILABLE'::character varying) AS first_allocation_store_name,
    COALESCE(st.store_label, 'NOT AVAILABLE'::character varying::text) AS first_allocation_store_label,
    COALESCE(s.order_id, 'NOT AVAILABLE'::character varying::text) AS first_order_id,
    COALESCE(c.customer_type, 'NOT AVAILABLE'::character varying::text) AS first_allocation_customer_type
   FROM ods_production.allocation a
     LEFT JOIN ods_production.subscription s
      ON a.subscription_id::text = s.subscription_id::text
     LEFT JOIN ods_production.store st ON s.store_id = st.id
     LEFT JOIN ods_production.customer c on c.customer_id=a.customer_id
  WHERE a.rank_allocations_per_asset = 1
),
residual_last_month as(
select asset_id,final_price as last_month_residual_value_market_price
from ods_production.spv_report_master
where reporting_date = last_day(Dateadd('month',-2, DATE_TRUNC('month',CURRENT_DATE)))
),
residual_current_month as(
select *
from ods_production.spv_report_master
where reporting_date = last_day(Dateadd('month',-1, DATE_TRUNC('month',CURRENT_DATE)))
)
, purchase_price_commercial_2022_one_time_adjustment AS (-- one time fix based on SF prices, output table provided by Giulio Betetto (CP&A) after his checks 
	SELECT 
		asset_id,
		"commercial 3"::float AS commercial_purchase_price_adjusted
	FROM staging_airbyte_bi.commercial_pp
)
, asset_sold_status AS (
	SELECT distinct assetid,
	LAST_VALUE (createddate) OVER (PARTITION BY assetid order BY createddate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sold_date,
	LAST_VALUE (oldvalue) OVER (PARTITION BY assetid order BY createddate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_old_status,
	LAST_VALUE (newvalue) OVER (PARTITION BY assetid order BY createddate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_new_status
	FROM stg_salesforce.asset_history ah
)
, debt_collection_asset_payment AS (
	SELECT *
	FROM ods_production.payment_asset pa
	WHERE payment_type='DEBT COLLECTION' AND status ='PAID')
, debt_collection_asset_sale AS (
SELECT DISTINCT
  asset_id,
	a.sold_date,
	LAST_VALUE (paid_date) IGNORE NULLS OVER (PARTITION BY asset_id ORDER BY paid_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS sold_date_pa,
	SUM(amount_paid) OVER (PARTITION BY asset_id)  AS sold_price_dc
FROM debt_collection_asset_payment pa
INNER JOIN asset_sold_status a
  ON a.assetid = pa.asset_id
WHERE last_old_status ='IN DEBT COLLECTION'
	AND last_new_status ='SOLD')
SELECT distinct
  	a.asset_id::VARCHAR(18)
  ,a.customer_id
  ,a.subscription_id
  ,a.created_date::TIMESTAMP WITHOUT TIME ZONE as created_at
  ,a.updated_date::TIMESTAMP WITHOUT TIME ZONE as updated_At
  ,coalesce(a.asset_allocation_id,aa1.allocation_id) as asset_allocation_id
  ,aa1.allocation_sf_id::VARCHAR(80) as asset_allocation_sf_id
  ,a.warehouse::VARCHAR(255)
  ,a.capital_source_name::VARCHAR(80)
  ,a.supplier::VARCHAR(65535)
  ,coalesce(fso.first_allocation_store_label,'never allocated')::VARCHAR(60) as first_allocation_store
    ,coalesce(fso.first_allocation_store_name,'never allocated')::VARCHAR(60) as first_allocation_store_name
    ,coalesce(fso.first_allocation_customer_type,'never allocated')::VARCHAR(60) as first_allocation_customer_type
    ,a.serial_number::VARCHAR(80)
  ,a.product_ean::VARCHAR(65535) as ean
  ,a.product_sku::VARCHAR(255)
  ,a.asset_name
  ,a.asset_condition
  ,a.asset_condition_spv
  ,a.variant_sku::VARCHAR(65535)
  ,a.product_name::VARCHAR(510)
  ,a.category_name::VARCHAR(65535)
  ,a.subcategory_name::VARCHAR(65535)
  ,a.brand::VARCHAR(65535)
    ,a.invoice_url
  ,coalesce(aa1.total_allocations_per_asset,0)::BIGINT AS total_allocations_per_asset
  ,a.asset_order_number
	,a.purchase_request_item_id as purchase_request_item_sfid
	,pri.purchase_request_item_id
	,pri.request_id
  ,a.purchased_date::DATE
    ,a.months_since_purchase::INTEGER
    ,a.days_since_purchase::INTEGER
    ,a.days_on_book::INTEGER
    ,a.months_on_book::INTEGER
  ,a.amount_rrp::DOUBLE PRECISION as amount_rrp
  ,a.initial_price::DOUBLE PRECISION as initial_price
  ,mv.final_price::DOUBLE PRECISION as residual_value_market_price
  ,residual.last_month_residual_value_market_price
  ,mv.average_of_sources_on_condition_this_month
  ,mv.average_of_sources_on_condition_last_available_price
  ,a.sold_price AS sold_price
  ,a.sold_date as sold_date
  ,a.currency::VARCHAR(255)
  ,a.asset_status_original::VARCHAR(255)
  ,aa2.asset_status_new::VARCHAR(30) as asset_status_new
  ,CASE
    WHEN a.asset_status_original = 'SOLD' AND dcas.asset_id IS NOT NULL
     THEN 'SOLD to Customer'
    ELSE aa2.asset_status_detailed::VARCHAR(255)
   END AS asset_status_detailed
  ,a.lost_reason
  ,a.lost_date
  ,aa2.last_allocation_days_in_stock::DOUBLE PRECISION
  ,last_allocation_dpd::INTEGER
  ,aa2.dpd_bucket
  ,ac.subscription_revenue_paid::DOUBLE PRECISION as subscription_revenue
  ,ac.amount_refund::DOUBLE PRECISION as amount_refund
    ,ac.subscription_revenue_due::DOUBLE PRECISION as subscription_revenue_due
,ac.subscription_revenue_last_31day::DOUBLE PRECISION as subscription_revenue_last_31day
,ac.subscription_revenue_paid_last_month::DOUBLE PRECISION as subscription_revenue_last_month
,ac.subscription_revenue_current_month::DOUBLE PRECISION as subscription_revenue_current_month
  ,ac.avg_subscription_amount::DOUBLE PRECISION
  ,ac.max_subscription_amount::DOUBLE PRECISION
  ,ac.sub_payments_due::BIGINT as payments_due
    ,ac.last_payment_amount_due::BIGINT as last_payment_amount_due
    ,ac.last_payment_amount_paid::BIGINT as last_payment_amount_paid
  ,ac.sub_payments_paid::BIGINT as payments_paid
  ,ac.shipment_cost_paid AS shipment_cost_paid
  ,ac.repair_cost_paid::DOUBLE PRECISION
  ,ac.customer_bought_paid::DOUBLE PRECISION
  ,ac.additional_charge_paid::DOUBLE PRECISION
  ,ad.delivered_allocations::BIGINT
  ,ad.returned_allocations::BIGINT
  ,ac.max_paid_date::TIMESTAMP WITHOUT TIME ZONE
    ,a.office_or_sponsorships::VARCHAR(22)
    ,coalesce(
      spv.last_final_valuation::DOUBLE PRECISION,
      greatest((1 - 0.03 *
      	((date_trunc('month', coalesce(ac.max_asset_sold_date::date,a.sold_date::DATE,a.lost_date,current_date-1))::date - a.purchased_date)
      	/ 30)),0) * a.initial_price)
       as last_market_valuation
    ,spv.last_valuation_report_date::TIMESTAMP WITHOUT TIME ZONE
    ,greatest(a.initial_price-(((a.months_since_purchase  )-1)*1::decimal/36::decimal*a.initial_price),0)::DOUBLE PRECISION as asset_value_linear_depr
    ,greatest(a.initial_price-(((a.months_on_book  )-1)*1::decimal/36::decimal*a.initial_price),0)::DOUBLE PRECISION as asset_value_linear_depr_book
    ,gmv.mkp as market_price_at_purchase_date
    ,asr.active_subscription_id
   	,asr.active_subscriptions_bom
   	,asr.active_subscriptions
   	,asr.acquired_subscriptions
   	,asr.cancelled_subscriptions
   	,asr.active_subscription_value
   	,asr.acquired_subscription_value
   	,asr.rollover_subscription_value
   	,asr.cancelled_subscription_value
    ,a.shipping_country
    ,ac.asset_sold_invoice
    ,a.invoice_date
    ,a.invoice_number
    ,a.invoice_total
    ,a.revenue_share
    ,fso.first_order_id
    ,COALESCE(o.shippingcountry,o.billingcountry) AS country,
	COALESCE(o.shippingcity,o.billingcity) AS city,
	COALESCE(o.shippingpostalcode,o.billingpostalcode) AS postal_code
    ,cf.asset_cashflow_from_old_subscriptions
    ,asr.last_active_subscription_id
   ,CASE
    WHEN COALESCE(o.shippingcountry,o.billingcountry) = 'United States' --US should not touched by the fix, keeping initital price there
    	OR a.warehouse IN ('office_us','ups_softeon_us_kylse')
    	OR a.locale__c IN ('California, US','USA')
    THEN a.initial_price
    WHEN a.purchased_date::date >= '2022-01-01' AND a.purchased_date::date <= '2022-12-31'
    	AND a.asset_id IN (SELECT asset_id FROM purchase_price_commercial_2022_one_time_adjustment) --changing pp for all those in the provided list from Giulio 
    THEN ota.commercial_purchase_price_adjusted
    WHEN a.purchased_date::date >='2020-07-01' AND a.purchased_date::date <'2020-12-31'
    	AND(a.subcategory_name = 'Smartphones' 
    		OR ((a.subcategory_name = 'Tablets' OR a.category_name = 'Wearables')
    			AND (a.product_name LIKE '%LTE%' OR a.product_name ILIKE '%Cellular%'))
    			OR (a.subcategory_name = 'Tablets' AND a.brand = 'APPLE' 
    			AND supplier IN ('ComLine GmbH','TechData GmbH & Co. OHG','Ingram Micro Distribution GmbH')))
  		AND  (SUM(a.initial_price) OVER(PARTITION BY pri.purchase_request_item_id)) > 5000
  	THEN (a.initial_price * 1.16)::DOUBLE PRECISION
    WHEN a.purchased_date::date >='2021-01-01' AND a.purchased_date::date <'2022-06-01'
    		AND(a.subcategory_name = 'Smartphones' 
    		OR ((a.subcategory_name = 'Tablets' OR a.category_name = 'Wearables')
    			AND (a.product_name LIKE '%LTE%' OR a.product_name ILIKE '%Cellular%'))
    			OR (a.subcategory_name = 'Tablets' AND a.brand = 'APPLE' 
    			AND supplier IN ('ComLine GmbH','TechData GmbH & Co. OHG','Ingram Micro Distribution GmbH')))
  		AND  (SUM(a.initial_price) OVER(PARTITION BY pri.purchase_request_item_id)) > 5000
  	THEN (a.initial_price * 1.19)::DOUBLE PRECISION
  	WHEN a.purchased_date::date >= '2022-08-01'
  			 AND (a.category_name = 'Cameras' AND a.brand = 'SONY')
  	THEN a.initial_price
 	 	WHEN a.purchased_date::date >= '2022-10-01'
 	 	AND a.supplier = 'Sony Europe B.V'
  			 AND (a.category_name = 'Cameras' AND a.brand = 'SONY')
  	THEN a.initial_price
 	WHEN a.purchased_date::date >= '2022-10-01'
  		AND (COALESCE(a.locale__c,'n/a') <> 'Netherlands' AND a.supplier NOT IN ('Micromedia B.V.',
'Photospecialist (Kamera Express B.V.)','GezamenlijkVoordeel B.V.','Copaco Nederland B.V.','Caseking GmbH'))
		AND first_allocation_store NOT LIKE '%offline%'
 	THEN (a.initial_price * 1.19)::DOUBLE PRECISION
 	--new addition
  	WHEN a.purchased_date::date >='2022-06-01'
		AND (a.locale__c = 'Netherlands' OR a.supplier IN ('Micromedia B.V.',
'Photospecialist (Kamera Express B.V.)','GezamenlijkVoordeel B.V.','Copaco Nederland B.V.','Caseking GmbH'))
		AND a.subcategory_name IN ('All in One PCs','Desktop Computers','Gaming Computers','Laptops','Smartphones','Tablets')
		AND (SUM(a.initial_price) OVER(PARTITION BY pri.request_id)) > 10000
		AND first_allocation_store NOT LIKE '%offline%'
	THEN (a.initial_price * 1.21)::DOUBLE PRECISION
	--end of new addition
  		ELSE  a.initial_price
  END AS purchase_price_commercial
  ,a.locale__c AS supplier_locale
FROM ods_production.asset a
LEFT JOIN ods_production.allocation aa1
 ON aa1.asset_id=a.asset_id
 and (aa1.is_last_allocation_per_asset
  OR aa1.allocation_id IS NULL)
  LEFT JOIN ods_production."order" o on aa1.order_id=o.order_id
--  left join ods_production.product p on a.product_sku=p.product_sku
  left join ods_production.asset_allocation_history ad on ad.asset_id=a.asset_id
  LEFT JOIN ods_production.asset_last_allocation_details aa2 ON aa2.asset_id=a.asset_id
  left join ods_production.asset_cashflow ac on ac.asset_id=a.asset_id
  left join residual_current_month mv  on mv.asset_id =a.asset_id
  left join residual_last_month residual on residual.asset_id=a.asset_id
  left join fso on fso.asset_id=a.asset_id
  left join ods_spv_historical.sold_asset_valuation spv on spv.asset_id=a.asset_id
  left join ods_production.asset_subscription_reconciliation asr on asr.asset_id=a.asset_id
 		        on date_trunc('month',a.purchased_date)::date=date_trunc('month',gmv.datum)::date
	          and a.product_sku=gmv.product_sku
  left join ods_production.subscription_cashflow cf on a.subscription_id = cf.subscription_id
  left join ods_production.purchase_request_item pri on pri.purchase_request_item_sfid = a.purchase_request_item_id
  LEFT JOIN purchase_price_commercial_2022_one_time_adjustment ota
  	ON ota.asset_id = a.asset_id
   LEFT JOIN debt_collection_asset_sale dcas
    ON a.asset_id = dcas.asset_id
WHERE TRUE
 AND a.asset_status_grouped <> 'NEVER PURCHASED'
;

COMMIT;
