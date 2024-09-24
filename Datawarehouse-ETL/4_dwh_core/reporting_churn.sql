DROP TABLE IF EXISTS dwh.reporting_churn;
CREATE TABLE dwh.reporting_churn AS
WITH dates AS (
	SELECT
		DISTINCT 
	 	datum AS fact_date,
		date_trunc('month', datum)::DATE AS month_bom,
		LEAST(DATE_TRUNC('MONTH', DATEADD('MONTH', 1, DATUM))::DATE-1, CURRENT_DATE) AS MONTH_EOM
	FROM public.dim_dates
	WHERE datum <= current_date
	ORDER BY 1 DESC 
)
, active_subs AS (
	SELECT
		fact_date,
		month_bom,
		month_eom,
		s.customer_type,
		count(DISTINCT s.subscription_id) AS active_subscriptions,
		count(DISTINCT 
	  	CASE 
	   		WHEN s.cancellation_reason_churn <> 'failed delivery' 
	    	 AND last_valid_payment_category LIKE ('%DEFAULT%') 
	    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
	    	 AND default_date < fact_date 
	   		  THEN s.subscription_id 
	  	END) AS active_default_subscriptions,
		count(DISTINCT s.customer_id) AS active_customers,
		count(DISTINCT 
	  	CASE 
	    	WHEN s.cancellation_reason_churn <> 'failed delivery' 
	  		 AND last_valid_payment_category LIKE ('%DEFAULT%') 
	    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
	    	 AND default_date < fact_date 
	   		  THEN s.customer_id 
	  	END) AS active_default_customers,
		sum(s.subscription_value_eur) AS active_subscription_value,
		sum( 
	  	CASE 
	    	WHEN s.cancellation_reason_churn <> 'failed delivery' 
	   		 AND last_valid_payment_category LIKE ('%DEFAULT%') 
	    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
	    	 AND default_date < fact_date 
	   		  THEN s.subscription_value_eur 
	  	END) AS active_default_subscription_value,
		count(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS active_subscriptions_failed_delivery,
		count(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.customer_id END) AS active_customers_failed_delivery,
		sum(CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_eur END) AS active_subscription_value_failed_delivery
	FROM dates d
	LEFT JOIN ods_production.subscription_phase_mapping s
	 ON d.fact_date::date >= s.fact_day::date
	  AND d.fact_date::date <= COALESCE(s.end_date::date, d.fact_date::date + 1)
	LEFT JOIN ods_production.subscription_cancellation_reason c 
	 ON s.subscription_id = c.subscription_id
	LEFT JOIN ods_production.subscription_cashflow cf 
	 ON s.subscription_id = cf.subscription_id
	GROUP BY 1,2,3,4
	ORDER BY 1 DESC
)
, payments AS (
	SELECT
		sp.paid_date::date AS paid_date,
		sp.customer_type,
		sum(sp.amount_paid) AS collected_subscription_revenue
	FROM master.subscription_payment sp
	LEFT JOIN master.subscription s 
	 ON sp.subscription_id = s.subscription_id
	GROUP BY 1,2
	ORDER BY 1 DESC
)
, acquisition AS (
	SELECT
		s.start_date::date AS start_date,
		customer_type,
		count(s.subscription_id) AS new_subscriptions,
		count( DISTINCT s.order_id) AS new_orders,
		sum(s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS acquired_subscription_value,
		sum((s.committed_sub_value + s.additional_committed_sub_value) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS acquired_committed_sub_value,
		count(CASE WHEN o.retention_group = 'RECURRING, UPSELL' THEN s.subscription_id END) AS upsell_subscriptions,
		count(CASE WHEN o.retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_id END) AS reactivation_subscriptions,
		sum(CASE WHEN o.new_recurring = 'NEW' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS new_subscription_value,
		sum(CASE WHEN o.retention_group = 'RECURRING, UPSELL' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS upsell_subscription_value,
		sum(CASE WHEN o.retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS reactivation_subscription_value,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND o.new_recurring = 'NEW' THEN s.subscription_id END) AS new_subscriptions_failed_delivery,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND o.retention_group = 'RECURRING, UPSELL' THEN s.subscription_id END) AS upsell_subscriptions_failed_delivery,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND o.retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_id END) AS reactivation_subscriptions_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND o.new_recurring = 'NEW' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS new_subscription_value_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND o.retention_group = 'RECURRING, UPSELL' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS upsell_subscription_value_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND o.retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS reactivation_subscription_value_failed_delivery
	FROM master.subscription s
	LEFT JOIN trans_dev.daily_exchange_rate exc
	 ON s.created_date::date = exc.date_
	  AND s.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
	 ON s.currency = exc_last.currency
	LEFT JOIN ods_production.order_retention_group o 
	 ON o.order_id = s.order_id
	LEFT JOIN ods_production.subscription_cancellation_reason c 
	 ON s.subscription_id = c.subscription_id
	GROUP BY 1,2
)
, churn AS (
	SELECT
		s.cancellation_date::date AS cancellation_date,
		s.customer_type,
		count(s.subscription_id) AS cancelled_subscriptions,
		sum(s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS cancelled_subscription_value,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS cancelled_subscription_value_failed_delivery,
		count(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_id END) AS cancelled_subscriptions_customer_request,
		sum(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS cancelled_subscription_value_customer_request
	FROM master.subscription s
	LEFT JOIN trans_dev.daily_exchange_rate exc
	 ON s.created_date::date = exc.date_
	  AND s.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
	 ON s.currency = exc_last.currency
	LEFT JOIN ods_production.subscription_cancellation_reason c 
	 ON s.subscription_id = c.subscription_id
	GROUP BY 1,2
)
, switch AS (
	SELECT
		date::date AS report_date,
		cu.customer_type,
		count(DISTINCT sw.subscription_id) AS switch_subscriptions,
		sum(delta_in_sub_value * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) * -1) AS switch_subscription_value
	FROM ods_production.subscription_plan_switching sw
	LEFT JOIN ods_production.subscription s 
	 ON sw.subscription_id = s.subscription_id
	LEFT JOIN trans_dev.daily_exchange_rate exc
	 ON s.created_date::date = exc.date_
	  AND s.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
	 ON s.currency = exc_last.currency
	LEFT JOIN master.customer cu 
	 ON s.customer_id = cu.customer_id
	GROUP BY 1,2 
)
, historical AS (
	SELECT
		date AS reporting_date,
		s.customer_type,
		sum(CASE WHEN status = 'ACTIVE' THEN (committed_sub_value + COALESCE(s.additional_committed_sub_value,0)) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS committed_sub_value,
		sum(CASE WHEN status = 'ACTIVE' THEN commited_sub_revenue_future * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) END) AS commited_sub_revenue_future
	FROM master.subscription_historical s
	LEFT JOIN trans_dev.daily_exchange_rate exc
	 ON s.created_date::date = exc.date_
	  AND s.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
	 ON s.currency = exc_last.currency
	GROUP BY 1,2
	ORDER BY 1 DESC
), commercial_targets AS 
(
	SELECT
		datum,
		CASE
			WHEN store = 'B2B' 
			 THEN 'business_customer'
			ELSE 'normal_customer'
		END AS customer_type,
		sum(t.active_sub_value_daily_target) AS active_sub_value_daily_target,
		sum(t.incrementalsubsvalue_daily) AS incrementalsubsvalue_daily,
		sum(t.cancelled_subvalue_daily) AS cancelled_subvalue_daily,
		sum(t.acquired_subvalue_daily) AS acquired_subvalue_daily
	FROM dwh.commercial_targets_daily_store_commercial t
	WHERE store_country NOT IN ('B2B-Total','Grover-EU')
	GROUP BY 1,2
)
SELECT DISTINCT 
l.fact_date,
l.month_bom,
l.month_eom,
l.customer_type,
--active customers
COALESCE(active_customers, 0) AS active_customers,
COALESCE(active_default_customers, 0) AS active_default_customers,
COALESCE(active_customers_failed_delivery, 0) AS active_customers_failed_delivery,
--active subs
COALESCE(active_subscriptions, 0) AS active_subscriptions,
COALESCE(active_default_subscriptions, 0) AS active_default_subscriptions,
COALESCE(active_subscriptions_failed_delivery, 0) AS active_subscriptions_failed_delivery,
COALESCE(active_subscription_value, 0) AS active_subscription_value,
COALESCE(active_default_subscription_value, 0) AS active_default_subscription_value,
COALESCE(active_subscription_value_failed_delivery, 0) AS active_subscription_value_failed_delivery,
--new subs
COALESCE(acquired_subscription_value, 0) AS acquired_subscription_value,
COALESCE(acquired_committed_sub_value, 0) AS acquired_committed_sub_value,
	--
COALESCE(new_orders,0) as new_orders,
COALESCE(new_subscriptions, 0) AS new_subscriptions,
COALESCE(new_subscriptions_failed_delivery, 0) AS new_subscriptions_failed_delivery,
COALESCE(new_subscription_value, 0) AS new_subscription_value,
COALESCE(new_subscription_value_failed_delivery, 0) AS new_subscription_value_failed_delivery,
--upsell
COALESCE(upsell_subscriptions, 0) AS upsell_subscriptions,
COALESCE(upsell_subscriptions_failed_delivery, 0) AS upsell_subscriptions_failed_delivery,
COALESCE(upsell_subscription_value, 0) AS upsell_subscription_value,
COALESCE(upsell_subscription_value_failed_delivery, 0) AS upsell_subscription_value_failed_delivery,
-- reactivation
COALESCE(reactivation_subscriptions, 0) AS reactivation_subscriptions,
COALESCE(reactivation_subscriptions_failed_delivery, 0) AS reactivation_subscriptions_failed_delivery,
COALESCE(reactivation_subscription_value, 0) AS reactivation_subscription_value,
COALESCE(reactivation_subscription_value_failed_delivery, 0) AS reactivation_subscription_value_failed_delivery,
-- cancelled subscriptions
COALESCE(cancelled_subscriptions, 0) AS cancelled_subscriptions,
COALESCE(cancelled_subscriptions_failed_delivery, 0) AS cancelled_subscriptions_failed_delivery,
COALESCE(cancelled_subscriptions_customer_request, 0) AS cancelled_subscriptions_customer_request,
COALESCE(cancelled_subscription_value, 0) AS cancelled_subscription_value,
COALESCE(cancelled_subscription_value_failed_delivery, 0) AS cancelled_subscription_value_failed_delivery,
COALESCE(cancelled_subscription_value_customer_request, 0) AS cancelled_subscription_value_customer_request,
--sub switching
COALESCE(switch_subscriptions, 0) AS switch_subscriptions,
COALESCE(switch_subscription_value, 0) AS switch_subscription_value,
--targets
t.active_sub_value_daily_target,
t.incrementalsubsvalue_daily,
t.cancelled_subvalue_daily,
t.acquired_subvalue_daily,
d.day_is_first_of_month,
d.day_is_last_of_month,
COALESCE(h.committed_sub_value, 0) AS committed_sub_value,
COALESCE(h.commited_sub_revenue_future, 0) AS commited_sub_revenue_future,
COALESCE(p.collected_subscription_revenue, 0) AS collected_subscription_revenue
FROM active_subs l
LEFT JOIN acquisition a 
 ON l.fact_date::date = a.start_date::date
  AND l.customer_type = a.customer_type
LEFT JOIN churn c 
 ON l.fact_date::date = c.cancellation_date::date
  AND l.customer_type = c.customer_type
LEFT JOIN commercial_targets t
  ON t.datum::date = l.fact_date::date
   AND l.customer_type = t.customer_type
LEFT JOIN public.dim_dates d 
 ON d.datum::date = l.fact_date::date
LEFT JOIN historical h 
 ON h.reporting_date::date = l.fact_date::date
  AND l.customer_type = h.customer_type
LEFT JOIN payments p 
 ON p.paid_date::date = l.fact_Date::date
  AND l.customer_type = p.customer_type
LEFT JOIN switch s
 ON s.report_date::date = l.fact_date::date
  AND l.customer_type = s.customer_type
ORDER BY l.fact_date DESC
;

GRANT SELECT ON dwh.reporting_churn TO tableau;
