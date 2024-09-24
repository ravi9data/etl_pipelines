
CREATE OR REPLACE VIEW dm_risk.v_b2b_exposure_report AS
with first_order_approval as
(
SELECT
	o.customer_id,
	min(approved_date) as first_approval_date
FROM
	master.order o
group by
	1
),
subscription_assets as
(
select
	s.customer_id ,
	sum(sa2.outstanding_purchase_price) AS outstanding_purchase_price,
	sum(sa2.outstanding_purchase_price_with_lost) AS outstanding_purchase_price_with_lost,
	sum(sa2.outstanding_residual_asset_value) AS  outstanding_residual_asset_value,
	sum(sa2.asset_valuation_written_off) AS asset_valuation_written_off,
	sum(sa2.asset_valuation_lost) AS  asset_valuation_lost
FROM
ods_production.subscription_assets sa2
	left join
	master.subscription s
	on s.subscription_id = sa2.subscription_id
group by
	1
),
outstanding_payments_adjusted_for_pbi_customer as
(
SELECT
	sp.customer_id,
	SUM(amount_due - COALESCE(amount_paid,0)) AS amount_overdue_pbi_adjusted -- adjustment done IN WHERE CONDITION.
FROM master.subscription_payment sp
LEFT JOIN ods_production."order" oo 
	ON oo.order_id = sp.order_id
WHERE 
	sp.status != 'PLANNED' AND 
	(CASE WHEN sp.customer_type = 'business_customer' AND 
		(sp.payment_method = 'pay-by-invoice' OR oo.is_pay_by_invoice IS TRUE) -- This IS the effective due date. Accounting FOR consolidated billing cases. 
    		THEN DATEADD('day', 14, sp.due_date) ELSE sp.due_date END)  < CURRENT_DATE 
GROUP BY 1
)
select
	c.customer_id,
	c.billing_country ,
	c.shipping_country ,
	c.company_type_name ,
	c.signup_country ,
	first_approval_date,
	c.company_name,
	COALESCE(billing_country, shipping_country) as country,
	c.subscription_limit,
	c.active_subscription_value,
	c.active_subscriptions,
	c.committed_subscription_value,
	c.subscription_revenue_paid,
	sa.outstanding_purchase_price,
	sa.outstanding_purchase_price_with_lost,
	sa.outstanding_residual_asset_value,
	sa.asset_valuation_written_off,
	sa.asset_valuation_lost,
	c.active_subscription_value / CASE WHEN sa.outstanding_purchase_price > 0 THEN sa.outstanding_purchase_price END as asv_over_outstnading_purchase_price,
	c.active_subscription_value / CASE WHEN sa.outstanding_purchase_price_with_lost > 0 THEN sa.outstanding_purchase_price_with_lost END as asv_over_outstnading_purchase_price_with_lost,
	c.active_subscription_value / CASE WHEN sa.outstanding_residual_asset_value > 0 THEN sa.outstanding_residual_asset_value END as asv_over_outstanding_residual_asset_value,
	pbi.amount_overdue_pbi_adjusted
from
master.customer c
	left join
	first_order_approval f
	on f.customer_id = c.customer_id
		left join
		subscription_assets sa
		on sa.customer_id = c.customer_id
			left join
			outstanding_payments_adjusted_for_pbi_customer pbi
			on pbi.customer_id = c.customer_id
where
customer_type = 'business_customer'
and completed_orders > 0 
WITH NO SCHEMA BINDING;




grant all on dm_risk.v_b2b_exposure_report to tableau;

grant all on dm_risk.v_b2b_exposure_report to matillion;
