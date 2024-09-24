delete from ods_production.spv_report_master where reporting_date=(current_Date-1);
--reporting_date=date_trunc('month',getdate())- INTERVAL '1MONTH';

insert into ods_production.spv_report_master
SELECT distinct 
--	date_trunc('month',getdate())- INTERVAL '1MONTH' as reporting_date,
	(current_Date-1)	as reporting_date,
	a.asset_id,
	a.warehouse,
	a.serial_number,
	a.product_sku,
    a.asset_name,
    p.category_name as category,
    p.subcategory_name as subcategory,
	a.country,
	a.city,
	a.postal_code,
    a.invoice_number,
    a.invoice_date,
    a.invoice_url,
    a.purchased_date,
    a.initial_price,
    a.delivered_allocations,
    a.returned_allocations,
    a.last_allocation_days_in_stock,
    a.asset_condition_spv,
    COALESCE(a.sold_date::date, 'now'::text::date) - a.purchased_date::date AS days_since_purchase,    
    mv.valuation_method,
    mv.average_of_sources_on_condition_this_month,
    mv.average_of_sources_on_condition_last_available_price,
    mv.m_since_last_valuation_price,
    mv.valuation_1,
    mv.valuation_2,
    mv.valuation_3,
    mv.residual_value_market_price as final_price,
    a.sold_date AS sold_date,
    a.sold_price,
    a.asset_status_original,
    a.subscription_revenue_due AS mrr,
    a.subscription_revenue as collected_mrr,
    null::double precision as total_inflow,
    a.capital_source_name,
    null as exception_rule,
    a.ean
   FROM master.asset_historical a
      LEFT JOIN ods_production.product p 
       ON p.product_sku = a.product_sku
      LEFT JOIN (select * from ods_spv_historical.asset_market_value where reporting_date =(CURRENT_DATE::date-1)) mv--tgt_dev.asset_market_value_dec2 mv 
       ON mv.asset_id = a.asset_id
  --    LEFT JOIN ods_production.asset_allocation_history h 
    --   ON h.asset_id = a.asset_id
 WHERE a.asset_status_original not in ('DELETED'::character varying::text, 'BUY NOT APPROVED'::character varying::text, 'BUY'::character varying::text, 'RETURNED TO SUPPLIER'::character varying::text)
      and (a.warehouse::text in 
                  ('office_de'::character varying::text, 
                  'synerlogis_de'::character varying::text , 
                  'office_us'::character varying::text ,
                  'ups_softeon_us_kylse'::character varying::text ,
                  'ups_softeon_eu_nlrng'::character varying::text,
                  'ingram_micro_eu_flensburg'::character varying::text)
    and a."date"=(CURRENT_DATE::date)-1);  