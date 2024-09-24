CREATE OR REPLACE VIEW dm_finance.v_due_dilligence AS 
WITH subscriptions AS (
SELECT 
		sh.*,
		DATE_PART('year', sh.date) AS year_r,
	EXTRACT('Month'
FROM
	sh.date)+ 1 AS month_r,
	DATE_PART('day', sh.start_date) AS day_r,
	TO_DATE((year_r || '-0' || month_r || '-' || day_r ), 'yyyy-mm-dd') AS replacement_date,
	CASE 
		WHEN sh.status = 'ACTIVE' THEN 
			CASE
				WHEN sh.minimum_cancellation_date IS NULL
			AND sh.subscription_plan LIKE 'Pay%' THEN replacement_date
			WHEN sh.minimum_cancellation_date IS NULL
			AND sh.subscription_plan NOT LIKE 'Pay%' THEN DATEADD('month',
				CAST(sh.rental_period AS integer),
				sh.start_date) ::date
			ELSE sh.minimum_cancellation_date
		END
		ELSE NULL
	END AS maturity_date
FROM
	master.subscription_historical sh
WHERE
	TRUE
	AND date >= DATEADD(MONTH,-3,CURRENT_DATE)
	AND (date = LAST_DAY(date) OR date = CURRENT_DATE -1))
,
payment_sub AS (
SELECT  
		DISTINCT subscription_id ,
		LAST_VALUE(capital_source) OVER (PARTITION BY subscription_id
ORDER BY
	due_date 
			ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS capital_source
FROM
	ods_production.payment_subscription
	) 
,
revenues_paid as 
(
SELECT subscription_id ,date , 
	sum(case when payment_type = 'REPAIR COST' and status = 'PAID' then amount_paid else 0 end) as repair_cost_paid,
	sum(case when payment_type in ('FIRST', 'RECURRENT') and status = 'PAID' then amount_paid else 0 end) as subscription_revenue_paid,
	sum(case when payment_type in ('SHIPMENT') and status = 'PAID' then amount_paid else 0 end) as shipment_cost_paid,
	sum(case when payment_type in ('CUSTOMER BOUGHT') and status = 'PAID' then amount_paid else 0 end) as customer_bought_paid,
	sum(case when payment_type in ('DEBT COLLECTION') and status = 'PAID' then amount_paid else 0 end) as debt_collection_paid,
	sum(case when payment_type in ('ADDITIONAL CHARGE', 'COMPENSATION') and status = 'PAID' then amount_paid else 0 end) as additional_charge_paid,
	sum(case when payment_type like ('%CHARGE BACK%') and status = 'PAID' then amount_paid  else 0 end) as chargeback_paid,
	sum(case when payment_type like '%REFUND%' and status = 'PAID' then amount_paid  else 0 end) as refunds_paid
FROM master.payment_all_historical
GROUP BY 1,2
)
,
customer_risk as 
(
select customer_id, burgel_risk_category
from ods_production.customer_scoring
)


SELECT
	sh.subscription_id,
	subscription_sf_id,
	order_created_date,
	created_date,
	updated_date,
	start_date,
	rank_subscriptions,
	first_subscription_start_date,
	subscriptions_per_customer,
	sh.customer_id,
	customer_type,
	customer_acquisition_cohort,
	subscription_limit,
	order_id,
	store_id,
	store_name,
	store_label,
	store_type,
	store_number,
	account_name,
	ps.capital_source,
	status,
	variant_sku,
	allocation_status,
	allocation_tries,
	cross_sale_attempts,
	replacement_attempts,
	allocated_assets,
	delivered_assets,
	returned_packages,
	returned_assets,
	outstanding_assets,
	outstanding_asset_value,
	outstanding_residual_asset_value,
	outstanding_rrp,
	first_asset_delivery_date,
	last_return_shipment_at,
	subscription_plan,
	rental_period,
	subscription_value,
	(committed_sub_value + COALESCE(additional_committed_sub_value,0)) as committed_sub_value,
	next_due_date,
	commited_sub_revenue_future,
	currency,
	subscription_duration,
	effective_duration,
	outstanding_duration,
	months_required_to_own,
	max_payment_number,
	payment_count,
	paid_subscriptions,
	last_valid_payment_category,
	dpd,
	subscription_revenue_due,
	sh.subscription_revenue_paid,
	outstanding_subscription_revenue,
	subscription_revenue_refunded,
	subscription_revenue_chargeback,
	net_subscription_revenue_paid,
	cancellation_date,
	cancellation_note,
	cancellation_reason,
	cancellation_reason_new,
	cancellation_reason_churn,
	is_widerruf,
	payment_method,
	debt_collection_handover_date,
	dc_status,
	result_debt_collection_contact,
	avg_asset_purchase_price,
	product_sku,
	product_name,
	category_name,
	subcategory_name,
	brand,
	new_recurring,
	retention_group,
	minimum_cancellation_date,
	minimum_term_months,
	asset_cashflow_from_old_subscriptions,
	exposure_to_default,
	mietkauf_amount_overpayment,
	is_eligible_for_mietkauf,
	trial_days,
	trial_variant,
	is_not_triggered_payments,
	asset_recirculation_status,
	store_short,
	country_name,
	store_commercial,
	maturity_date,
	repair_cost_paid,
	shipment_cost_paid,
	customer_bought_paid,
	debt_collection_paid,
	additional_charge_paid,
	chargeback_paid,
	refunds_paid,
	chargeback_paid + refunds_paid as refunds_and_chargebacks,
	cr.burgel_risk_category,
	sh."date"
FROM
	subscriptions sh
LEFT JOIN payment_sub ps 
ON
	sh.subscription_id = ps.subscription_id
LEFT JOIN revenues_paid r 
ON
	sh.subscription_id = r.subscription_id	
	and sh.date = r.date	
LEFT JOIN customer_risk cr 
ON
	sh.customer_id = cr.customer_id
WITH NO SCHEMA BINDING;