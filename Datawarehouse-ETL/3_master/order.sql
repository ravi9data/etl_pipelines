--Updated at: 04/04/19
--check statuses of columns different orders with ivan 
--check with esteban the order rank process
BEGIN;


DROP TABLE IF EXISTS order_final;
CREATE TEMP TABLE order_final AS
WITH payments_paid AS (
	SELECT 
		customer_id,
		payment_number,
		subscription_id,
		lag(payment_number) over (partition by subscription_id order by payment_number::INT DESC) AS payment_number_1_after,
		lag(payment_number,2) over (partition by subscription_id order by payment_number::INT DESC) AS payment_number_2_after,
		lag(paid_date,2) over (partition by subscription_id order by payment_number::INT DESC) AS paid_date_2_after
	FROM ods_production.payment_subscription sp 
	WHERE status = 'PAID'
)
, good_recurring_customer_number_payments AS ( 
	SELECT DISTINCT 
		customer_id::varchar,
		MIN(paid_date_2_after) AS min_paid_date_good_recurring
	FROM payments_paid pp
	WHERE payment_number::int + 1 = payment_number_1_after 
		AND payment_number::int + 2 = payment_number_2_after
	GROUP BY 1
)
, bad_recurring_customer_dates_failed AS (
SELECT DISTINCT
	customer_id,
	paid_date,
	due_date AS failed_date 
	FROM ods_production.payment_subscription
	WHERE failed_date IS NOT NULL
)
SELECT DISTINCT
	o.order_id,
    o.paid_date, 
    o.customer_id , 
    o.store_id, 
    o.created_date , 
    o.submitted_date, 
    o.updated_date, 
    o.approved_date, 
    o.canceled_date, 
    o.acquisition_date, 
    o.status, 
    o.order_rank, 
    oj.order_journey,
    o.total_orders, 
    o.cancellation_reason, 
    dr.decline_reason_new AS declined_reason, 
    o.order_value, 
    o.voucher_code::VARCHAR(510), 
    o.is_special_voucher::VARCHAR(510) AS voucher_type, 
    o.voucher_value::VARCHAR(510), 
    o.voucher_discount::DOUBLE PRECISION, 
    o.is_in_salesforce, 
    o.store_type, 
	r.new_recurring,
	r.retention_group,
	m.marketing_channel,
	m.marketing_campaign,
	m.devicecategory AS device,
	st.store_name::VARCHAR(510),
	st.store_label::VARCHAR(30),
	st.store_short,
    o.store_commercial,
    st.country_name AS store_country,
	st.store_number,
	o.order_item_count,
	o.basket_size,
	o.is_trial_order,
    1 AS cart_orders,
    ocl.cart_page_orders,
    ocl.cart_logged_in_orders,
    ocl.address_orders,
    ocl.payment_orders,
    ocl.summary_orders,
	ocl.completed_orders,
	ocl.declined_orders,
	ocl.failed_first_payment_orders,
	ocl.cancelled_orders,
	ocl.paid_orders,
	o.avg_plan_duration,
    cs.current_subscription_limit,
 	cs.burgel_risk_category::VARCHAR(15),
    cs.schufa_class,
    os.order_scoring_comments AS scoring_decision,
    os.scoring_reason AS scoring_reason,
    c.customer_type,
    o.initial_scoring_decision,
    o.payment_method,
	CASE
		WHEN r.new_recurring = 'RECURRING'
			AND g.customer_id IS NOT NULL
				AND o.submitted_date >= min_paid_date_good_recurring
					AND fd.customer_id IS NULL
						THEN 'GOOD RECURRING' 
    	ELSE r.new_recurring 
    END AS new_recurring_risk
FROM 
    ods_production."order" o 
LEFT JOIN ods_production.customer c 
    ON c.customer_id=o.customer_id
LEFT JOIN ods_production.order_retention_group r 
    ON r.order_id=o.order_id
LEFT JOIN ods_production.order_marketing_channel m 
    ON o.order_id = m.order_id
LEFT JOIN ods_production.customer_scoring cs 
    ON cs.customer_id=o.customer_id
LEFT JOIN ods_production.order_scoring os 
    ON os.order_id=o.order_id
LEFT JOIN ods_production.store st
    ON st.id=o.store_id
LEFT JOIN ods_production.order_journey oj 
    ON oj.order_id = o.order_id
LEFT JOIN ods_production.order_conversion_labels ocl 
    ON ocl.order_id=o.order_id
LEFT JOIN ods_production.order_decline_reason dr 
    ON dr.order_id=o.order_id
LEFT JOIN good_recurring_customer_number_payments  g
	ON g.customer_id = o.customer_id
LEFT JOIN bad_recurring_customer_dates_failed fd 
	ON o.customer_id = fd.customer_id
		AND o.submitted_date > fd.failed_date
			AND o.submitted_date < COALESCE(fd.paid_date, current_date)
WHERE 
    o.order_id IS NOT NULL; 

   

TRUNCATE TABLE master."order";

INSERT INTO  master."order"
SELECT * FROM order_final;

COMMIT;
