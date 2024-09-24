/* Filename : reporting_profitability_variables.sql
 * Version  : 0.1
 * Purpose  : Creation of a dashboard for finacnce reporting of profitability variables
 * JIRA     : https://byebuyglobaloperations.atlassian.net/browse/BI-2699
 * Author   : Sneha Rao
 * Reviewer : Akshay
 * Date     : 2020-11-04
 * Source   : 1.master_asset
 * 
 * Table Update: EOM or 14th of every month
 * 	Output  : dm_finance.reporting_profitability_variables
 * The dashboard was running on a custome sql script. The new script will run on ETL and create a entries on EOM or 14th of the month. The dashboard will be plugged into this table instead of custom sql
 *
 */	


-- Table creation One time run for historic updates
/*drop table if exists dm_finance.reporting_profitability_variables;
create table dm_finance.reporting_profitability_variables as
with a as
(
select 
a.*
from master.asset_historical a 
left join public.dim_dates d on a.date=d.datum
where 
 d.day_is_last_of_month=1 or 
 d.month_day_number = 14
) select * from a;
*/



DROP TABLE IF EXISTS dm_finance.reporting_profitability_variables;
CREATE TABLE dm_finance.reporting_profitability_variables AS 
WITH returned_and_delivered_allocations AS (
    SELECT
        a.asset_id,
        count(DISTINCT CASE WHEN a.return_delivery_date::date BETWEEN '2020-01-01' AND '2020-12-31' THEN a.allocation_id END) AS returned_allocations_2020,
        count(DISTINCT CASE WHEN a.return_delivery_date::date BETWEEN '2021-01-01' AND '2021-12-31' THEN a.allocation_id END) AS returned_allocations_2021,
        count(DISTINCT CASE WHEN a.return_delivery_date::date BETWEEN '2022-01-01' AND '2022-12-31' THEN a.allocation_id END) AS returned_allocations_2022,
        count(DISTINCT CASE WHEN a.return_delivery_date::date BETWEEN '2023-01-01' AND '2023-12-31' THEN a.allocation_id END) AS returned_allocations_2023,
        count(DISTINCT CASE WHEN a.return_delivery_date::date BETWEEN '2024-01-01' AND '2024-12-31' THEN a.allocation_id END) AS returned_allocations_2024,
        count(DISTINCT CASE WHEN a.delivered_at::date BETWEEN '2020-01-01' AND '2020-12-31' THEN a.allocation_id END) AS delivered_allocations_2020,
        count(DISTINCT CASE WHEN a.delivered_at::date BETWEEN '2021-01-01' AND '2021-12-31' THEN a.allocation_id END) AS delivered_allocations_2021,
        count(DISTINCT CASE WHEN a.delivered_at::date BETWEEN '2022-01-01' AND '2022-12-31' THEN a.allocation_id END) AS delivered_allocations_2022,
        count(DISTINCT CASE WHEN a.delivered_at::date BETWEEN '2023-01-01' AND '2023-12-31' THEN a.allocation_id END) AS delivered_allocations_2023,
        count(DISTINCT CASE WHEN a.delivered_at::date BETWEEN '2024-01-01' AND '2024-12-31' THEN a.allocation_id END) AS delivered_allocations_2024
    FROM ods_production.allocation a
    GROUP BY 1
)
, repair_amount AS (
    SELECT
        asset_id,
        outbound_date,
        sum(repair_price) AS repair_price
    FROM dm_recommerce.repair_invoices
    GROUP BY 1,2
)
, asset_final AS (
	SELECT
	      a.*
	     ,lag(a.date) OVER(PARTITION BY a.asset_id ORDER BY a.date) AS previous_date
	     ,rd.returned_allocations_2020
	     ,rd.returned_allocations_2021
	     ,rd.returned_allocations_2022
	     ,rd.returned_allocations_2023
	     ,rd.returned_allocations_2024
	     ,rd.delivered_allocations_2020
	     ,rd.delivered_allocations_2021
	     ,rd.delivered_allocations_2022
	     ,rd.delivered_allocations_2023
	     ,rd.delivered_allocations_2024
	FROM master.asset_historical a
	         LEFT JOIN public.dim_dates d
	                   ON d.datum = a."date"
	         LEFT JOIN returned_and_delivered_allocations rd
	                   ON rd.asset_id = a.asset_id
	WHERE (d.day_is_last_of_month=1 OR d.month_day_number = 14)
)
, negative_subscription_revenue_by_subs AS ( --finding the subs WITH negative VALUES FOR subs_revenue
	SELECT 
		d.datum,
		aa.subscription_id, -- the subs_revenue cannot be negative IN subs level
		COALESCE(sum(
	                CASE
	                    WHEN sp.paid_date IS NOT NULL 
	                     THEN coalesce(sp.amount_paid,0)-COALESCE(sp.chargeback_amount, 0::numeric)
	                    ELSE NULL::numeric
	                END), 0::numeric) AS subscription_revenue
	FROM public.dim_dates d
	LEFT JOIN ods_production.allocation aa
		 ON d.datum > aa.created_at
	left join ods_production.payment_subscription sp 
	    on sp.allocation_id=aa.allocation_id
	    AND sp.invoice_date <= d.datum
	WHERE true and sp.status not in ('CANCELLED')
		AND (d.day_is_last_of_month=1 OR d.month_day_number = 14)
		AND d.datum < current_date
		AND d.datum >= '2019-02-11' -- min date we have IN master.asset_historical
		AND sp.paid_date IS NOT NULL 
	GROUP BY 1,2
	HAVING subscription_revenue < 0
)
, asset_with_negative_values_by_subs AS ( --allocating an asset_id TO those subscriptions
	SELECT 
		n.datum,
		n.subscription_id,
		n.subscription_revenue * (-1) AS subscription_revenue, --turning it to positive IN ORDER TO sum later
		a.asset_id,
		row_number() over (partition by n.subscription_id, n.datum order by a.created_at desc) AS rn
	FROM negative_subscription_revenue_by_subs n
	LEFT JOIN ods_production.allocation a 
		ON n.subscription_id = a.subscription_id	
)
, asset_with_negative_values AS ( -- finding the value IN asset_id level
	SELECT 
		datum,
		asset_id,
		sum(subscription_revenue) AS subscription_revenue
	FROM asset_with_negative_values_by_subs
	WHERE rn = 1 
	GROUP BY 1,2
)
, first_allocation_channel_raw AS (
	SELECT 
		a.asset_id,
		a.customer_type,
		a.subscription_id,
		ROW_NUMBER () OVER (PARTITION BY asset_id ORDER BY allocated_at) AS rowno
	FROM master.allocation a
)
, first_allocation_channel AS ( 
	SELECT 
		a.asset_id,
		a.customer_type,
		fm.is_freelancer 
	FROM first_allocation_channel_raw a
	LEFT JOIN master.subscription s 
		ON s.subscription_id = a.subscription_id
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping fm
		ON fm.company_type_name = c2.company_type_name	
	WHERE rowno = 1
)
SELECT 
	a.asset_id,
	a.customer_id,
	a.subscription_id,
	a.created_at,
	a.updated_at,
	a.asset_allocation_id,
	a.asset_allocation_sf_id,
	a.warehouse,
	a.capital_source_name,
	a.supplier,
	a.first_allocation_store,
	a.first_allocation_store_name,
	a.first_allocation_customer_type,
	a.serial_number,
	a.ean,
	a.product_sku,
	a.asset_name,
	a.asset_condition,
	a.asset_condition_spv,
	a.variant_sku,
	a.product_name,
	a.category_name,
	a.subcategory_name,
	a.brand,
	a.invoice_url,
	a.total_allocations_per_asset,
	a.asset_order_number,
	a.purchase_request_item_sfid,
	a.purchase_request_item_id,
	a.request_id,
	a.purchased_date,
	a.months_since_purchase,
	a.days_since_purchase,
	a.days_on_book,
	a.months_on_book,
	a.amount_rrp,
	a.initial_price,
	a.residual_value_market_price,
	a.residual_value_for_written_off_assets,
	a.last_month_residual_value_market_price,
	a.average_of_sources_on_condition_this_month,
	a.average_of_sources_on_condition_last_available_price,
	a.sold_price,
	a.sold_date,
	a.currency,
	a.asset_status_original,
	a.asset_status_new,
	a.asset_status_detailed,
	a.lost_reason,
	a.lost_date,
	a.last_allocation_days_in_stock,
	a.last_allocation_dpd,
	a.dpd_bucket,
	CASE WHEN n.subscription_revenue IS NOT NULL AND a.subscription_revenue < 0 THEN 0
		 ELSE a.subscription_revenue END AS subscription_revenue,
	a.amount_refund,
	a.subscription_revenue_due,
	a.subscription_revenue_last_31day,
	a.subscription_revenue_last_month,
	a.subscription_revenue_current_month,
	a.avg_subscription_amount,
	a.max_subscription_amount,
	a.payments_due,
	a.last_payment_amount_due,
	a.last_payment_amount_paid,
	a.payments_paid,
	a.shipment_cost_paid,
	a.repair_cost_paid,
	a.customer_bought_paid,
	a.additional_charge_paid,
	a.delivered_allocations,
	a.returned_allocations,
	a.max_paid_date,
	a.office_or_sponsorships,
	a.last_market_valuation,
	a.last_valuation_report_date,
	a.asset_value_linear_depr,
	a.asset_value_linear_depr_book,
	a.market_price_at_purchase_date,
	a.active_subscription_id,
	a.active_subscriptions_bom,
	a.active_subscriptions,
	a.acquired_subscriptions,
	a.cancelled_subscriptions,
	a.active_subscription_value,
	a.acquired_subscription_value,
	a.rollover_subscription_value,
	a.cancelled_subscription_value,
	a.shipping_country,
	a.asset_sold_invoice,
	a.invoice_date,
	a.invoice_number,
	a.invoice_total,
	a.revenue_share,
	a.first_order_id,
	a.country,
	a.city,
	a.postal_code,
	a.asset_cashflow_from_old_subscriptions,
	a.last_active_subscription_id,
	a.purchase_price_commercial,
	a.supplier_locale,
	a.date,
	a.snapshot_time,
	a.previous_date,
	a.returned_allocations_2020,
	a.returned_allocations_2021,
	a.returned_allocations_2022,
	a.returned_allocations_2023,
	a.returned_allocations_2024,
	a.delivered_allocations_2020,
	a.delivered_allocations_2021,
	a.delivered_allocations_2022,
	a.delivered_allocations_2023,
	a.delivered_allocations_2024,
	sum(b.repair_price) OVER (PARTITION BY a.asset_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS repair_price,
	CASE WHEN f.customer_type = 'normal_customer' THEN 'B2C' ELSE 'B2B' END AS first_allocation_channel,
	CASE 
		WHEN f.is_freelancer = 1 THEN 'B2B-Freelancers'
		WHEN f.is_freelancer = 0 THEN  'B2B-Non Freelancers'
	END AS is_freelancer
FROM asset_final a
LEFT JOIN repair_amount b 
	ON a.asset_id = b.asset_id 
	 AND b.outbound_date BETWEEN a.previous_date AND a.date
LEFT JOIN asset_with_negative_values n 
	ON n.datum = a.date
	AND n.asset_id = a.asset_id
LEFT JOIN first_allocation_channel f
	ON f.asset_id = a.asset_id
;
	 
GRANT SELECT ON dm_finance.reporting_profitability_variables TO tableau;
