-- dm_commercial.v_commercial_cash_monthly_kpis_us source

--CREATE OR REPLACE
--VIEW dm_commercial.v_commercial_cash_monthly_kpis_us
--AS /*MTLN-1.59.9 (build 551efe94dc8/47eeb2f3-c)*/
--ALTER TABLE backup_tables." allocation_backup_20220415" RENAME TO allocation_backup_20220415;
--drop table backup_tables.purchase_request_item_backup_20220417
/*
GRANT SELECT ON hightouch_sources.performance_tracker TO hightouch_pricing;
GRANT SELECT ON hightouch_sources.catman_trackers_pending_allocation TO hightouch_pricing;
GRANT SELECT ON hightouch_sources.catman_trackers_subs_cancelled TO hightouch_pricing;
*/

CREATE OR REPLACE VIEW dm_commercial.v_commercial_cash_monthly_kpis_us AS
SELECT
	DISTINCT 
	d.reporting_date,
	d.category_name,
	d.subcategory_name,
	d.product_sku,
	d.currency,
	d.country_name,
	d.store_commercial_group,
	s.subs_revenue_incl_shipment,
	dip.initial_price,
	de.market_price,
	de.monthly_depr_adjusted,
	r.returned_allocation AS returned_allocations,
	da.delivered_allocations,
	u.purchase_price_in_stock,
	u.purchase_price_on_rent,
	u.purchase_price_refurbishment,
	u.inventory_utilization_rate_by_product_sku,
	u.marketable_utilization_rate_by_product_sku,
	'now'::text::timestamp WITH time ZONE AS last_updated_ts
FROM
	(((((
	SELECT
		revenue_from_sub.reporting_date,
		revenue_from_sub.category_name,
		revenue_from_sub.subcategory_name,
		revenue_from_sub.product_sku,
		revenue_from_sub.currency,
		revenue_from_sub.country_name,
		revenue_from_sub.store_commercial_group
	FROM
		(
		SELECT
			last_day(pa.paid_date::date::timestamp WITHOUT time ZONE) AS reporting_date,
			COALESCE(s.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
			COALESCE(s.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
			COALESCE(s.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
			CASE
				WHEN s.store_commercial::text = 'Grover Germany'::text
				OR s.store_commercial::text = 'Grover International'::text THEN 'Grover'::CHARACTER VARYING
				WHEN s.store_commercial::text = 'Partnerships Germany'::text
				OR s.store_commercial::text = 'Partnerships International'::text THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(s.store_commercial, 'Unknown'::CHARACTER VARYING)
			END AS store_commercial_group,
			COALESCE(pa.currency, 'Unknown'::CHARACTER VARYING) AS currency,
			COALESCE(s.country_name, 'Unknown'::CHARACTER VARYING) AS country_name,
			sum(pa.amount_paid) AS subs_revenue_incl_shipment,
			'Model Inputs' AS tableau_data_source_workbook_name,
			'Collected Revenue' AS tableau_data_source_tab_name,
			'ods_production.payment_all' AS redshift_datasource_1,
			'master.subscription' AS redshift_datasource_2
		FROM ods_production.payment_all pa
		LEFT JOIN master.subscription s 
		 ON s.subscription_id::text = pa.subscription_id::text
		WHERE (pa.payment_type::text = 'CHARGE BACK SUBSCRIPTION_PAYMENT'::text
		 OR pa.payment_type::text = 'REFUND SUBSCRIPTION_PAYMENT'::text
		 OR pa.payment_type::text = 'SHIPMENT'::text
		 OR pa.payment_type::text = 'RECURRENT'::text
		 OR pa.payment_type::text = 'FIRST'::text)
		  AND pa.paid_date >= '2021-01-01 00:00:00'::timestamp WITHOUT time ZONE
		GROUP BY 
			s.product_sku,
			s.category_name,
			CASE
				WHEN s.store_commercial::text = 'Grover Germany'::text
				 OR s.store_commercial::text = 'Grover International'::text 
				  THEN 'Grover'::CHARACTER VARYING
				WHEN s.store_commercial::text = 'Partnerships Germany'::text
				 OR s.store_commercial::text = 'Partnerships International'::text 
				  THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(s.store_commercial, 'Unknown'::CHARACTER VARYING)
			END,
			last_day(pa.paid_date::date::timestamp WITHOUT time ZONE),
			pa.currency,
			s.country_name,
			s.subcategory_name
		ORDER BY
			last_day(pa.paid_date::date::timestamp WITHOUT time ZONE)) revenue_from_sub
UNION
	SELECT
		deprecation.reporting_date,
		deprecation.category_name,
		deprecation.subcategory_name,
		deprecation.product_sku,
		deprecation.currency,
		deprecation.country_name,
		deprecation.store_commercial_group
	FROM
		(
		SELECT
			DISTINCT 
			srh.reporting_date::date AS reporting_date,
			COALESCE(srh.category, 'Unknown'::CHARACTER VARYING) AS category_name,
			COALESCE(srh.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
			COALESCE(a.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
			COALESCE(a.currency, 'Unknown'::CHARACTER VARYING) AS currency,
			CASE
				WHEN a.shipping_country::text = 'The Netherlands'::text 
				 THEN 'Netherlands'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
				 THEN 'Unknown'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'DE'::text 
				 THEN 'Germany'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'Österreich'::text 
				 THEN 'Austria'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'AT'::text 
				 THEN 'Austria'::CHARACTER VARYING
				ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
			END AS country_name,
			CASE
				WHEN srh.store_commercial::text = 'Grover Germany'::text
				 OR srh.store_commercial::text = 'Grover International'::text 
				   THEN 'Grover'::CHARACTER VARYING
				WHEN srh.store_commercial::text = 'Partnerships Germany'::text
				 OR srh.store_commercial::text = 'Partnerships International'::text 
				   THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
			END AS store_commercial_group,
			round(sum(srh.final_price), 2::NUMERIC) AS market_price,
			round(sum(srh.final_price) - sum(COALESCE(srh.prev_final_price, srh.initial_price, srh.prev_final_price)), 2::NUMERIC) AS monthly_depr_adjusted,
			'Model Inputs'::text AS tableau_data_source_workbook_name,
			'Portfolio Valuation'::text AS tableau_data_source_tab_name,
			'dm_finance.spv_report_historical'::text AS redshift_datasource_1,
			''::text AS redshift_datasource_2
		FROM dm_finance.spv_report_historical srh
		LEFT JOIN (
				SELECT
					DISTINCT asset_historical.asset_id,
					asset_historical.date,
					asset_historical.currency,
					asset_historical.shipping_country,
					asset_historical.subcategory_name
				FROM master.asset_historical
				WHERE asset_historical.date = last_day(asset_historical.date::timestamp WITHOUT time ZONE)) a 
		ON srh.asset_id::text = a.asset_id::text
		 AND srh.reporting_date = a.date::timestamp WITHOUT time ZONE
		WHERE srh.warehouse::text = 'office_us'::text
		 AND (srh.dpd_bucket::text = 'PENDING WRITEOFF'::text
			OR srh.dpd_bucket::text = 'OD 1-30'::text
			OR srh.dpd_bucket::text = 'IN STOCK'::text
			OR srh.dpd_bucket::text = 'OD 121-150'::text
			OR srh.dpd_bucket::text = 'OD 61-90'::text
			OR srh.dpd_bucket::text = 'OD 91-120'::text
			OR srh.dpd_bucket::text = 'OD 151-180'::text
			OR srh.dpd_bucket::text = 'OD 180+'::text
			OR srh.dpd_bucket::text = 'OD 31-60'::text
			OR srh.dpd_bucket::text = 'PERFORMING'::text)
			 AND srh.reporting_date >= '2021-01-01 00:00:00'::timestamp WITHOUT time ZONE
		GROUP BY
			srh.reporting_date,
			srh.category,
			srh.product_sku,
			CASE
				WHEN srh.store_commercial::text = 'Grover Germany'::text
				 OR srh.store_commercial::text = 'Grover International'::text 
				  THEN 'Grover'::CHARACTER VARYING
				WHEN srh.store_commercial::text = 'Partnerships Germany'::text
				 OR srh.store_commercial::text = 'Partnerships International'::text 
				  THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
			END,
			a.subcategory_name,
			a.currency,
			CASE
				WHEN a.shipping_country::text = 'The Netherlands'::text 
				 THEN 'Netherlands'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
				 THEN 'Unknown'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'DE'::text 
				 THEN 'Germany'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'Österreich'::text 
				 THEN 'Austria'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'AT'::text 
				 THEN 'Austria'::CHARACTER VARYING
				ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
			END) deprecation)
UNION
SELECT
	deprecation_ip.reporting_date,
	deprecation_ip.category_name,
	deprecation_ip.subcategory_name,
	deprecation_ip.product_sku,
	deprecation_ip.currency,
	deprecation_ip.country_name,
	deprecation_ip.store_commercial_group
FROM
	(
	SELECT
		DISTINCT srh.reporting_date::date AS reporting_date,
		COALESCE(srh.category, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(srh.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		COALESCE(a.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(a.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		CASE
			WHEN a.shipping_country::text = 'The Netherlands'::text 
			 THEN 'Netherlands'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
			 THEN 'Unknown'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'DE'::text 
			 THEN 'Germany'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Österreich'::text 
			 THEN 'Austria'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'AT'::text 
			 THEN 'Austria'::CHARACTER VARYING
			ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
		END AS country_name,
		CASE
			WHEN srh.store_commercial::text = 'Grover Germany'::text
			 OR srh.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN srh.store_commercial::text = 'Partnerships Germany'::text
			 OR srh.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		round(sum(srh.initial_price), 2::NUMERIC) AS initial_price,
		'Model Inputs'::text AS tableau_data_source_workbook_name,
		'Portfolio Valuation'::text AS tableau_data_source_tab_name,
		'dm_finance.spv_report_historical'::text AS redshift_datasource_1,
		''::text AS redshift_datasource_2
		FROM dm_finance.spv_report_historical srh
		LEFT JOIN (
			SELECT
				DISTINCT asset_historical.asset_id,
				asset_historical.date,
				asset_historical.currency,
				asset_historical.shipping_country,
				asset_historical.subcategory_name
			FROM master.asset_historical
			WHERE asset_historical.date = last_day(asset_historical.date::timestamp WITHOUT time ZONE)) a 
		 ON srh.asset_id::text = a.asset_id::text
		  AND srh.reporting_date = a.date::timestamp WITHOUT time ZONE
		WHERE srh.warehouse::text = 'office_us'::text
		 AND srh.reporting_date >= '2021-01-01 00:00:00'::timestamp WITHOUT time ZONE
		GROUP BY
			srh.reporting_date,
			srh.category,
			srh.product_sku,
			CASE
				WHEN srh.store_commercial::text = 'Grover Germany'::text
				 OR srh.store_commercial::text = 'Grover International'::text 
				  THEN 'Grover'::CHARACTER VARYING
				WHEN srh.store_commercial::text = 'Partnerships Germany'::text
				 OR srh.store_commercial::text = 'Partnerships International'::text 
				  THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
			END,
			a.subcategory_name,
			a.currency,
			CASE
				WHEN a.shipping_country::text = 'The Netherlands'::text 
				 THEN 'Netherlands'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
				 THEN 'Unknown'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'DE'::text 
				 THEN 'Germany'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'Österreich'::text 
				 THEN 'Austria'::CHARACTER VARYING
				WHEN a.shipping_country::text = 'AT'::text 
				 THEN 'Austria'::CHARACTER VARYING
				ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
			END) deprecation_ip)
UNION
SELECT
	delivered_allocations.reporting_date,
	delivered_allocations.category_name,
	delivered_allocations.subcategory_name,
	delivered_allocations.product_sku,
	delivered_allocations.currency,
	delivered_allocations.country_name,
	delivered_allocations.store_commercial_group
FROM
	(
	SELECT
		COALESCE(asset.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(asset.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(asset.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		COALESCE(subscription.country_name, 'Unknown'::CHARACTER VARYING) AS country_name,
		COALESCE(asset.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		CASE
			WHEN subscription.store_commercial::text = 'Grover Germany'::text
			 OR subscription.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
			 OR subscription.store_commercial::text = 'Partnerships International'::text 
			   THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
			last_day(allocation.delivered_at::date::timestamp WITHOUT time ZONE) AS reporting_date,
			count(DISTINCT allocation.allocation_id) AS delivered_allocations,
			'Model Inputs' AS tableau_data_source_workbook_name,
			'Returned Incremental' AS tableau_data_source_tab_name,
			'master.asset' AS redshift_datasource_1,
			'master.subscription' AS redshift_datasource_2
		FROM
			master.allocation allocation
		JOIN master.asset asset ON
			allocation.asset_id::text = asset.asset_id::text
		LEFT JOIN master.subscription subscription ON
			allocation.subscription_id::text = subscription.subscription_id::text
		WHERE
			allocation.delivered_at::date >= '2021-01-01'::date
		GROUP BY
			asset.product_sku,
			asset.category_name,
			CASE
				WHEN subscription.store_commercial::text = 'Grover Germany'::text
				OR subscription.store_commercial::text = 'Grover International'::text THEN 'Grover'::CHARACTER VARYING
				WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
				OR subscription.store_commercial::text = 'Partnerships International'::text THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
			END,
			asset.subcategory_name,
			subscription.country_name,
			asset.currency,
			last_day(allocation.delivered_at::date::timestamp WITHOUT time ZONE)) delivered_allocations)
UNION
SELECT
	returned_allocations.reporting_date,
	returned_allocations.category_name,
	returned_allocations.subcategory_name,
	returned_allocations.product_sku,
	returned_allocations.currency,
	returned_allocations.country_name,
	returned_allocations.store_commercial_group
FROM
	(
	SELECT
		COALESCE(asset.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(asset.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(asset.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		COALESCE(subscription.country_name, 'Unknown'::CHARACTER VARYING) AS country_name,
		COALESCE(asset.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		CASE
			WHEN subscription.store_commercial::text = 'Grover Germany'::text
				OR subscription.store_commercial::text = 'Grover International'::text THEN 'Grover'::CHARACTER VARYING
				WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
				OR subscription.store_commercial::text = 'Partnerships International'::text THEN 'Retail'::CHARACTER VARYING
				ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		last_day(allocation.return_delivery_date::date::timestamp WITHOUT time ZONE) AS reporting_date,
		count(DISTINCT allocation.allocation_id) AS returned_allocation,
		'Model Inputs' AS tableau_data_source_workbook_name,
		'Returned Incremental' AS tableau_data_source_tab_name,
		'master.asset' AS redshift_datasource_1,
		'master.subscription' AS redshift_datasource_2
		FROM master.allocation allocation
		JOIN master.asset asset 
		 ON allocation.asset_id::text = asset.asset_id::text
		LEFT JOIN master.subscription subscription 
		 ON allocation.subscription_id::text = subscription.subscription_id::text
		WHERE allocation.return_delivery_date::date >= '2021-01-01'::date
		GROUP BY
			asset.product_sku,
			asset.category_name,
			CASE
				WHEN subscription.store_commercial::text = 'Grover Germany'::text
				 OR subscription.store_commercial::text = 'Grover International'::text 
				  THEN 'Grover'::CHARACTER VARYING
				WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
				 OR subscription.store_commercial::text = 'Partnerships International'::text 
				  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
			END,
			asset.subcategory_name,
			subscription.country_name,
			asset.currency,
			last_day(allocation.return_delivery_date::date::timestamp WITHOUT time ZONE)) returned_allocations)
UNION
SELECT
	utilization_rates.reporting_date,
	utilization_rates.category_name,
	utilization_rates.subcategory_name,
	utilization_rates.product_sku,
	utilization_rates.currency::CHARACTER VARYING AS currency,
	utilization_rates.country_name::CHARACTER VARYING AS country_name,
	utilization_rates.store_commercial_group::CHARACTER VARYING AS store_commercial_group
FROM
	(
	SELECT
		last_day(u.reporting_date::timestamp WITHOUT time ZONE) AS reporting_date,
		COALESCE(u.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(u.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(u.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		'Unknown'::text AS store_commercial_group,
		'Unknown'::text AS currency,
		'Unknown'::text AS country_name,
		round(1.0::double PRECISION - sum(u.purchase_price_in_stock) / 
                        CASE
                            WHEN (sum(u.purchase_price_in_stock) + sum(u.purchase_price_on_rent)) = 0::double PRECISION 
                             THEN NULL::double PRECISION
                            ELSE sum(u.purchase_price_in_stock) + sum(u.purchase_price_on_rent)
                        END, 5::NUMERIC) AS marketable_utilization_rate_by_product_sku,
		round(1.0::double PRECISION - (sum(u.purchase_price_in_stock) + sum(u.purchase_price_refurbishment)) / 
                        CASE
                            WHEN (sum(u.purchase_price_in_stock) + sum(u.purchase_price_refurbishment) + sum(u.purchase_price_on_rent)) = 0::double PRECISION 
                             THEN NULL::double PRECISION
                            ELSE sum(u.purchase_price_in_stock) + sum(u.purchase_price_refurbishment) + sum(u.purchase_price_on_rent)
                        END, 5::NUMERIC) AS inventory_utilization_rate_by_product_sku,
		sum(u.purchase_price_in_stock) AS purchase_price_in_stock,
		sum(u.purchase_price_on_rent) AS purchase_price_on_rent,
		sum(u.purchase_price_refurbishment) AS purchase_price_refurbishment,
		'Inventory Efficiency' AS tableau_data_source_workbook_name,
		'Asset Utilization' AS tableau_data_source_tab_name,
		'dwh.utilization' AS redshift_datasource_1,
		'' AS redshift_datasource_2
	FROM dwh.utilization u
	WHERE u.warehouse::text = 'office_us'::text
	 AND u.reporting_date >= '2021-01-01'::date
	 AND u.reporting_date = last_day(u.reporting_date::timestamp WITHOUT time ZONE)
	GROUP BY
		last_day(u.reporting_date::timestamp WITHOUT time ZONE),
		u.category_name,
		u.subcategory_name,
		6,
		7,
		5,
		u.product_sku) utilization_rates) d
LEFT JOIN (
	SELECT
		last_day(pa.paid_date::date::timestamp WITHOUT time ZONE) AS reporting_date,
		COALESCE(s.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(s.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(s.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		CASE
			WHEN s.store_commercial::text = 'Grover Germany'::text
				OR s.store_commercial::text = 'Grover International'::text 
				 THEN 'Grover'::CHARACTER VARYING
			WHEN s.store_commercial::text = 'Partnerships Germany'::text
				OR s.store_commercial::text = 'Partnerships International'::text 
				 THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(s.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		COALESCE(pa.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		COALESCE(s.country_name, 'Unknown'::CHARACTER VARYING) AS country_name,
		sum(pa.amount_paid) AS subs_revenue_incl_shipment,
		'Model Inputs' AS tableau_data_source_workbook_name,
		'Collected Revenue' AS tableau_data_source_tab_name,
		'ods_production.payment_all' AS redshift_datasource_1,
		'master.subscription' AS redshift_datasource_2
	FROM ods_production.payment_all pa
	LEFT JOIN master.subscription s 
	 ON s.subscription_id::text = pa.subscription_id::text
	WHERE (pa.payment_type::text = 'CHARGE BACK SUBSCRIPTION_PAYMENT'::text
	 OR pa.payment_type::text = 'REFUND SUBSCRIPTION_PAYMENT'::text
	 OR pa.payment_type::text = 'SHIPMENT'::text
	 OR pa.payment_type::text = 'RECURRENT'::text
	 OR pa.payment_type::text = 'FIRST'::text)
	  AND pa.paid_date >= '2021-01-01 00:00:00'::timestamp WITHOUT time ZONE
	GROUP BY
		s.product_sku,
		s.category_name,
		CASE
			WHEN s.store_commercial::text = 'Grover Germany'::text
			 OR s.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN s.store_commercial::text = 'Partnerships Germany'::text
			 OR s.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(s.store_commercial, 'Unknown'::CHARACTER VARYING)
		END,
		last_day(pa.paid_date::date::timestamp WITHOUT time ZONE),
		pa.currency,
		s.country_name,
		s.subcategory_name
	ORDER BY last_day(pa.paid_date::date::timestamp WITHOUT time ZONE)) s 
 ON d.reporting_date = s.reporting_date
  AND d.category_name::text = s.category_name::text
  AND d.subcategory_name::text = s.subcategory_name::text
  AND d.currency::text = s.currency::text
  AND d.country_name::text = s.country_name::text
  AND d.product_sku::text = s.product_sku::text
  AND d.store_commercial_group::text = s.store_commercial_group::text
LEFT JOIN (
	SELECT
		DISTINCT srh.reporting_date::date AS reporting_date,
		COALESCE(srh.category, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(srh.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		COALESCE(a.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(a.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		CASE
			WHEN a.shipping_country::text = 'The Netherlands'::text 
			 THEN 'Netherlands'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
			 THEN 'Unknown'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'DE'::text 
			 THEN 'Germany'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Österreich'::text 
			 THEN 'Austria'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'AT'::text 
			 THEN 'Austria'::CHARACTER VARYING
			ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
		END AS country_name,
		CASE
			WHEN srh.store_commercial::text = 'Grover Germany'::text
			 OR srh.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN srh.store_commercial::text = 'Partnerships Germany'::text
			 OR srh.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		round(sum(srh.initial_price), 2::NUMERIC) AS initial_price,
		'Model Inputs'::text AS tableau_data_source_workbook_name,
		'Portfolio Valuation'::text AS tableau_data_source_tab_name,
		'dm_finance.spv_report_historical'::text AS redshift_datasource_1,
		''::text AS redshift_datasource_2
	FROM dm_finance.spv_report_historical srh
	LEFT JOIN (
			SELECT
				DISTINCT asset_historical.asset_id,
				asset_historical.date,
				asset_historical.currency,
				asset_historical.shipping_country,
				asset_historical.subcategory_name
			FROM master.asset_historical
			WHERE asset_historical.date = last_day(asset_historical.date::timestamp WITHOUT time ZONE)) a 
	ON srh.asset_id::text = a.asset_id::text
	 AND srh.reporting_date = a.date::timestamp WITHOUT time ZONE
	WHERE srh.warehouse::text = 'office_us'::text
	 AND srh.reporting_date >= '2021-01-01 00:00:00'::timestamp WITHOUT time ZONE
	GROUP BY
		srh.reporting_date,
		srh.category,
		srh.product_sku,
		CASE
			WHEN srh.store_commercial::text = 'Grover Germany'::text
			 OR srh.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN srh.store_commercial::text = 'Partnerships Germany'::text
			 OR srh.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
		END,
		a.subcategory_name,
		a.currency,
		CASE
			WHEN a.shipping_country::text = 'The Netherlands'::text 
			 THEN 'Netherlands'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
			 THEN 'Unknown'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'DE'::text 
			 THEN 'Germany'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Österreich'::text 
			 THEN 'Austria'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'AT'::text 
			 THEN 'Austria'::CHARACTER VARYING
			ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
		END) dip 
ON d.reporting_date = dip.reporting_date
 AND d.category_name::text = dip.category_name::text
 AND d.subcategory_name::text = dip.subcategory_name::text
 AND d.currency::text = dip.currency::text
 AND d.country_name::text = dip.country_name::text
 AND d.product_sku::text = dip.product_sku::text
 AND d.store_commercial_group::text = dip.store_commercial_group::text
LEFT JOIN (
	SELECT
		DISTINCT srh.reporting_date::date AS reporting_date,
		COALESCE(srh.category, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(srh.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		COALESCE(a.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(a.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		CASE
			WHEN a.shipping_country::text = 'The Netherlands'::text 
			 THEN 'Netherlands'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Soft Deleted Record'::text 
			 THEN 'Unknown'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'DE'::text 
			 THEN 'Germany'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'Österreich'::text 
			 THEN 'Austria'::CHARACTER VARYING
			WHEN a.shipping_country::text = 'AT'::text 
			 THEN 'Austria'::CHARACTER VARYING
			ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
		END AS country_name,
		CASE
			WHEN srh.store_commercial::text = 'Grover Germany'::text
			 OR srh.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN srh.store_commercial::text = 'Partnerships Germany'::text
			 OR srh.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		round(sum(srh.final_price), 2::NUMERIC) AS market_price,
		round(sum(srh.final_price) - sum(COALESCE(srh.prev_final_price, srh.initial_price, srh.prev_final_price)), 2::NUMERIC) AS monthly_depr_adjusted,
		'Model Inputs'::text AS tableau_data_source_workbook_name,
		'Portfolio Valuation'::text AS tableau_data_source_tab_name,
		'dm_finance.spv_report_historical'::text AS redshift_datasource_1,
		''::text AS redshift_datasource_2
	FROM dm_finance.spv_report_historical srh
	LEFT JOIN (
			SELECT
				DISTINCT asset_historical.asset_id,
				asset_historical.date,
				asset_historical.currency,
				asset_historical.shipping_country,
				asset_historical.subcategory_name
			FROM master.asset_historical
			WHERE asset_historical.date = last_day(asset_historical.date::timestamp WITHOUT time ZONE)) a 
	 ON srh.asset_id::text = a.asset_id::text
	  AND srh.reporting_date = a.date::timestamp WITHOUT time ZONE
	WHERE srh.warehouse::text = 'office_us'::text
	 AND (srh.dpd_bucket::text = 'PENDING WRITEOFF'::text
		OR srh.dpd_bucket::text = 'OD 1-30'::text
		OR srh.dpd_bucket::text = 'IN STOCK'::text
		OR srh.dpd_bucket::text = 'OD 121-150'::text
		OR srh.dpd_bucket::text = 'OD 61-90'::text
		OR srh.dpd_bucket::text = 'OD 91-120'::text
		OR srh.dpd_bucket::text = 'OD 151-180'::text
		OR srh.dpd_bucket::text = 'OD 180+'::text
		OR srh.dpd_bucket::text = 'OD 31-60'::text
		OR srh.dpd_bucket::text = 'PERFORMING'::text)
	 AND srh.reporting_date >= '2021-01-01 00:00:00'::timestamp WITHOUT time ZONE
			GROUP BY
				srh.reporting_date,
				srh.category,
				srh.product_sku,
				CASE
					WHEN srh.store_commercial::text = 'Grover Germany'::text
					OR srh.store_commercial::text = 'Grover International'::text THEN 'Grover'::CHARACTER VARYING
					WHEN srh.store_commercial::text = 'Partnerships Germany'::text
					OR srh.store_commercial::text = 'Partnerships International'::text THEN 'Retail'::CHARACTER VARYING
					ELSE COALESCE(srh.store_commercial, 'Unknown'::CHARACTER VARYING)
				END,
				a.subcategory_name,
				a.currency,
				CASE
					WHEN a.shipping_country::text = 'The Netherlands'::text THEN 'Netherlands'::CHARACTER VARYING
					WHEN a.shipping_country::text = 'Soft Deleted Record'::text THEN 'Unknown'::CHARACTER VARYING
					WHEN a.shipping_country::text = 'DE'::text THEN 'Germany'::CHARACTER VARYING
					WHEN a.shipping_country::text = 'Österreich'::text THEN 'Austria'::CHARACTER VARYING
					WHEN a.shipping_country::text = 'AT'::text THEN 'Austria'::CHARACTER VARYING
					ELSE COALESCE(a.shipping_country, 'Unknown'::CHARACTER VARYING)
				END) de 
ON d.reporting_date = de.reporting_date
 AND d.category_name::text = de.category_name::text
 AND d.subcategory_name::text = de.subcategory_name::text
 AND d.currency::text = de.currency::text
 AND d.country_name::text = de.country_name::text
 AND d.product_sku::text = de.product_sku::text
 AND d.store_commercial_group::text = de.store_commercial_group::text
LEFT JOIN (
	SELECT
		COALESCE(asset.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(asset.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(asset.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		COALESCE(subscription.country_name, 'Unknown'::CHARACTER VARYING) AS country_name,
		COALESCE(asset.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		CASE
			WHEN subscription.store_commercial::text = 'Grover Germany'::text
			 OR subscription.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
			 OR subscription.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		last_day(allocation.return_delivery_date::date::timestamp WITHOUT time ZONE) AS reporting_date,
		count(DISTINCT allocation.allocation_id) AS returned_allocation,
		'Model Inputs' AS tableau_data_source_workbook_name,
		'Returned Incremental' AS tableau_data_source_tab_name,
		'master.asset' AS redshift_datasource_1,
		'master.subscription' AS redshift_datasource_2
	FROM master.allocation allocation
	JOIN master.asset asset ON allocation.asset_id::text = asset.asset_id::text
	LEFT JOIN master.subscription subscription 
	 ON allocation.subscription_id::text = subscription.subscription_id::text
	WHERE allocation.return_delivery_date::date >= '2021-01-01'::date
	GROUP BY 
		asset.product_sku,
		asset.category_name,
		CASE
			WHEN subscription.store_commercial::text = 'Grover Germany'::text
			 OR subscription.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
			 OR subscription.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
		END,
		asset.subcategory_name,
		subscription.country_name,
		asset.currency,
		last_day(allocation.return_delivery_date::date::timestamp WITHOUT time ZONE)) r 
ON d.reporting_date = r.reporting_date
 AND d.category_name::text = r.category_name::text
 AND d.subcategory_name::text = r.subcategory_name::text
 AND d.currency::text = r.currency::text
 AND d.country_name::text = r.country_name::text
 AND d.product_sku::text = r.product_sku::text
 AND d.store_commercial_group::text = r.store_commercial_group::text
LEFT JOIN (
	SELECT
		COALESCE(asset.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(asset.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(asset.currency, 'Unknown'::CHARACTER VARYING) AS currency,
		COALESCE(subscription.country_name, 'Unknown'::CHARACTER VARYING) AS country_name,
		COALESCE(asset.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		CASE
			WHEN subscription.store_commercial::text = 'Grover Germany'::text
			 OR subscription.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
			 OR subscription.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
			ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
		END AS store_commercial_group,
		last_day(allocation.delivered_at::date::timestamp WITHOUT time ZONE) AS reporting_date,
		count(DISTINCT allocation.allocation_id) AS delivered_allocations,
		'Model Inputs' AS tableau_data_source_workbook_name,
		'Returned Incremental' AS tableau_data_source_tab_name,
		'master.asset' AS redshift_datasource_1,
		'master.subscription' AS redshift_datasource_2
	FROM master.allocation allocation
	JOIN master.asset asset
	 ON allocation.asset_id::text = asset.asset_id::text
	LEFT JOIN master.subscription subscription
	 ON allocation.subscription_id::text = subscription.subscription_id::text
	WHERE allocation.delivered_at::date >= '2021-01-01'::date
	GROUP BY
		asset.product_sku,
		asset.category_name,
		CASE
			WHEN subscription.store_commercial::text = 'Grover Germany'::text
			 OR subscription.store_commercial::text = 'Grover International'::text 
			  THEN 'Grover'::CHARACTER VARYING
			WHEN subscription.store_commercial::text = 'Partnerships Germany'::text
			 OR subscription.store_commercial::text = 'Partnerships International'::text 
			  THEN 'Retail'::CHARACTER VARYING
		ELSE COALESCE(subscription.store_commercial, 'Unknown'::CHARACTER VARYING)
		END,
		asset.subcategory_name,
		subscription.country_name,
		asset.currency,
		last_day(allocation.delivered_at::date::timestamp WITHOUT time ZONE)) da 
ON d.reporting_date = da.reporting_date
 AND d.category_name::text = da.category_name::text
 AND d.subcategory_name::text = da.subcategory_name::text
 AND d.currency::text = da.currency::text
 AND d.country_name::text = da.country_name::text
 AND d.product_sku::text = da.product_sku::text
 AND d.store_commercial_group::text = da.store_commercial_group::text
LEFT JOIN (
	SELECT
		last_day(u.reporting_date::timestamp WITHOUT time ZONE) AS reporting_date,
		COALESCE(u.category_name, 'Unknown'::CHARACTER VARYING) AS category_name,
		COALESCE(u.subcategory_name, 'Unknown'::CHARACTER VARYING) AS subcategory_name,
		COALESCE(u.product_sku, 'Unknown'::CHARACTER VARYING) AS product_sku,
		'Unknown'::text AS store_commercial_group,
		'Unknown'::text AS currency,
		'Unknown'::text AS country_name,
		round(1.0::double PRECISION - sum(u.purchase_price_in_stock) / 
         CASE
            WHEN (sum(u.purchase_price_in_stock) + sum(u.purchase_price_on_rent)) = 0::double PRECISION 
             THEN NULL::double PRECISION
            ELSE sum(u.purchase_price_in_stock) + sum(u.purchase_price_on_rent)
         END, 5::NUMERIC) AS marketable_utilization_rate_by_product_sku,
		round(1.0::double PRECISION - (sum(u.purchase_price_in_stock) + sum(u.purchase_price_refurbishment)) / 
         CASE
            WHEN (sum(u.purchase_price_in_stock) + sum(u.purchase_price_refurbishment) + sum(u.purchase_price_on_rent)) = 0::double PRECISION 
             THEN NULL::double PRECISION
            ELSE sum(u.purchase_price_in_stock) + sum(u.purchase_price_refurbishment) + sum(u.purchase_price_on_rent)
         END, 5::NUMERIC) AS inventory_utilization_rate_by_product_sku,
		sum(u.purchase_price_in_stock) AS purchase_price_in_stock,
		sum(u.purchase_price_on_rent) AS purchase_price_on_rent,
		sum(u.purchase_price_refurbishment) AS purchase_price_refurbishment,
		'Inventory Efficiency' AS tableau_data_source_workbook_name,
		'Asset Utilization' AS tableau_data_source_tab_name,
		'dwh.utilization' AS redshift_datasource_1,
		'' AS redshift_datasource_2
	FROM dwh.utilization u
	WHERE u.warehouse::text = 'office_us'::text
	 AND u.reporting_date >= '2021-01-01'::date
	 AND u.reporting_date = last_day(u.reporting_date::timestamp WITHOUT time ZONE)
	GROUP BY
		last_day(u.reporting_date::timestamp WITHOUT time ZONE),
		u.category_name,
		u.subcategory_name,
		6,
		7,
		5,
		u.product_sku) u 
ON d.reporting_date = u.reporting_date
 AND d.category_name::text = u.category_name::text
 AND d.subcategory_name::text = u.subcategory_name::text
 AND d.currency::text = u.currency
 AND d.country_name::text = u.country_name
 AND d.product_sku::text = u.product_sku::text
 AND d.store_commercial_group::text = u.store_commercial_group
GROUP BY
	d.reporting_date,
	d.category_name,
	d.subcategory_name,
	d.product_sku,
	d.currency,
	d.country_name,
	d.store_commercial_group,
	s.subs_revenue_incl_shipment,
	dip.initial_price,
	de.market_price,
	de.monthly_depr_adjusted,
	r.returned_allocation,
	da.delivered_allocations,
	u.purchase_price_in_stock,
	u.purchase_price_on_rent,
	u.purchase_price_refurbishment,
	u.inventory_utilization_rate_by_product_sku,
	u.marketable_utilization_rate_by_product_sku
WITH NO SCHEMA BINDING;