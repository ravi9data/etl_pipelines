WITH funnel_metrics AS
(
	SELECT 
		date_trunc('month', first_card_created_date)::date AS date_,
		COUNT(DISTINCT CASE WHEN first_timestamp_money_received IS NOT NULL THEN customer_id END) AS users_made_first_deposit,
		COUNT(DISTINCT CASE WHEN first_timestamp_payment_successful IS NOT NULL THEN customer_id END) AS users_made_first_purchase,
		COUNT(DISTINCT CASE WHEN first_activated_date IS NOT NULL THEN customer_id END) AS user_activations_technical
	GROUP BY 1
)
, subscription_metrics AS
(
	SELECT 
		date_trunc('month', start_date)::date AS date_,
		COUNT(DISTINCT subscription_id) AS total_subs_after_card,
		COUNT(DISTINCT CASE WHEN start_date > first_timestamp_payment_successful THEN subscription_id END) AS total_subs_after_card_activated,
		ROUND(SUM(subscription_value),2) AS acquired_subs_value_after_card,
		ROUND(SUM(CASE WHEN start_date > first_timestamp_payment_successful THEN subscription_value END),2) AS acquired_subs_value_after_card_activated,
		ROUND(SUM(committed_sub_value),2) AS total_csv_after_card
	WHERE is_after_card = 1 
	GROUP BY 1
)
, transaction_metrics AS
(
SELECT 
		TO_DATE(date_trunc('month', cast(event_timestamp AS datetime)), 'YYYY-MM-DD') AS date_,
		COUNT(customer_id) AS num_transactions,
		SUM(amount_transaction) AS total_trans_volume_euro
	WHERE event_name = 'payment-successful' AND event_timestamp IS NOT NULL
	GROUP BY 1
	SELECT 
		date_trunc('month', event_timestamp)::date AS date_,
		WHERE user_classification='Card_user'
		GROUP BY 1
)
	SELECT 
		date_,
		total_cash_issuance,
		total_cash_redemptions,
		sum(total_cash_issuance) OVER(ORDER BY date_ ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT row  ) AS running_issuance,
		sum(total_cash_redemptions) OVER(ORDER BY date_ ROWS BETWEEN UNBOUNDED PRECEDING AND current row) AS running_redemption
)
, session_metrics_card_users AS
(
	SELECT 
		date_trunc('month', cast(session_start  AS datetime))::date AS date_,
		count(distinct case when session_start >first_event_timestamp then wb.customer_id end) AS users,
		count(distinct case when session_start >first_event_timestamp then session_id end) AS sessions_after_card,
		sum(case when session_start >first_event_timestamp then time_engaged_in_s end) AS time_engaged_after_card
	FROM traffic.sessions wb
	WHERE session_start IS NOT NULL 
		AND wb.session_start >= DATE_TRUNC('month', DATEADD(month, -10, current_date)) -- '2021-04-26' start date of data records
		-- Note: We are computing averages in Tableau between these two groups so want to keep the time constraints the same. TB.
	GROUP BY 1
),
session_metrics_non_card_users AS -- ( Non Card Users with active subs)
(
	SELECT 
		TO_DATE(date_trunc('month', cast(session_start AS datetime)), 'YYYY-MM-DD') AS date_,
		count(distinct wb.customer_id) AS users,
		count(distinct session_id) AS total_sessions,
		sum(time_engaged_in_s) AS total_time_engaged
		FROM traffic.sessions wb
		WHERE wb.session_start >= DATE_TRUNC('month', DATEADD(month, -10, current_date))  -- '2021-04-26' start date OF DATA records. Since THIS query IS v time consuming we have applied a date WINDOW.
		-- Note: We are computing averages in Tableau between these two groups so want to keep the time constraints the same. TB.
								  WHERE wb.customer_id = gcc.customer_id::VARCHAR(55))
			AND EXISTS (SELECT NULL
						FROM
							(SELECT
								customer_id,
								start_date,
								CASE
									WHEN cancellation_date IS NULL THEN CURRENT_DATE
									ELSE DATEADD(DAY, -1, cancellation_date)
								END subs_active_until
							FROM master.subscription) s
						WHERE wb.customer_id = s.customer_id::VARCHAR(55)
							AND	start_date <= date_ AND subs_active_until >= date_)
		GROUP BY 1
),
app_page_views AS (
	SELECT 
		session_id,
		anonymous_id,
		page_view_id,
		customer_id,
		page_view_start,
		ROW_NUMBER() OVER (PARTITION BY anonymous_id ORDER BY page_view_start) AS rowno
	FROM segment.page_views_app
	WHERE page_type_detail = 'card'
),
app_session_user_mapping AS (
	SELECT 
		session_id,
		max(customer_id) AS customer_id
	FROM app_page_views
	GROUP BY 1
),
app_button_press_order_card_raw AS (
	SELECT 
		context_actions_amplitude_session_id AS session_id,
		timestamp AS button_click_ts
	FROM react_native.button_pressed
	WHERE item = 'order_card'
),
app_button_press_order_card AS (
	SELECT
		p.page_view_id,
		ROW_NUMBER() OVER (PARTITION BY p.page_view_id ORDER BY b.button_click_ts) AS rowno
	FROM app_page_views p
	INNER JOIN app_button_press_order_card_raw b
	  ON p.session_id = b.session_id
	  AND p.page_view_start < b.button_click_ts
),
	SELECT 
		TO_DATE(date_trunc('month', cast(p.page_view_start AS datetime)), 'YYYY-MM-DD') AS date_,
		COUNT(DISTINCT p.page_view_id) AS num_page_views,
		COUNT(DISTINCT p.anonymous_id) AS num_users,
		COUNT(DISTINCT su.customer_id) AS num_customers,
		SUM(CASE WHEN c.customer_id IS NOT NULL THEN 1 ELSE 0 END) AS card_accounts,
		SUM(CASE WHEN b.page_view_id IS NOT NULL THEN 1 ELSE 0 END) AS card_ordered		
	FROM app_page_views p
	INNER JOIN app_session_user_mapping su
	ON p.session_id = su.session_id
	ON c.customer_id = su.customer_id
	AND p.rowno = 1
	LEFT JOIN app_button_press_order_card b
	ON b.page_view_id = p.page_view_id
	AND b.rowno = 1
	GROUP BY 1
),
ATM_withdrawals AS
(
	SELECT
		DATE_TRUNC('month', event_timestamp)::date AS date_,
		COUNT(DISTINCT (trace_id || ' ' || payload_id)) AS ATMwithdrawals_count,
		SUM(amount_transaction) AS ATMwithdrawals_transvolume_eur
	WHERE event_name = 'atm-withdrawal' 
		AND transaction_type = 'CASH_ATM' 
		AND amount_transaction > 0
	GROUP BY 1
),
sct_incoming AS
(
	SELECT
		DATE_TRUNC('month', event_timestamp)::date AS date_,
		COUNT(DISTINCT payload_id) AS SCT_incoming_numtransactions,
		SUM(amount_transaction) AS SCT_incoming_transvolume
	WHERE event_name = 'money-received'
		AND amount_transaction > 0 
	GROUP BY 1
), 
sct_outgoing AS
(
	SELECT
		date_trunc('month', event_timestamp)::date AS date_,
		SUM(amount_transaction) AS SCT_outgoing_transvolume
	WHERE event_name = 'money-sent'
		AND amount_transaction > 0
	GROUP BY 1
),
subscription_behaviour_change AS
(
	SELECT DISTINCT
		datum AS date_,
		COUNT(DISTINCT cc.customer_id) AS num_activated_users_plus90,
		-- Aggregates at Onboarding
		COUNT(DISTINCT CASE WHEN cc.first_card_created_date::date 
				BETWEEN s.fact_day::date AND COALESCE(s.end_date::date, cc.first_card_created_date::date+1)
				THEN s.subscription_id END) AS subs_at_onboarding,
		ROUND(SUM(CASE WHEN cc.first_card_created_date::date 
					BETWEEN s.fact_day::date AND COALESCE(s.end_date::date, cc.first_card_created_date::date+1)
					THEN s.subscription_value_eur END),0) AS ASV_at_onboarding_euro,
		-- Aggregates at reporting date (last day of month)
		COUNT(DISTINCT CASE WHEN DATE_ADD('day', -1, DATE_ADD('month', 1, datum)) 
				BETWEEN s.fact_day::date AND COALESCE(s.end_date::date, DATE_ADD('month', 1, dd.datum))
				THEN s.subscription_id END) AS subs_at_reportingdate,
		ROUND(SUM(CASE WHEN DATE_ADD('day', -1, DATE_ADD('month', 1, datum)) 
				BETWEEN s.fact_day::date AND COALESCE(s.end_date::date, DATE_ADD('month', 1, dd.datum))
				THEN s.subscription_value_eur END),0) AS ASV_at_reportingdate_euro
	FROM public.dim_dates dd
		ON cc.first_timestamp_payment_successful < DATE_ADD('day', -90, DATE_ADD('day', -1, DATE_ADD('month', 1, datum))) -- LAST DAY OF MONTH (reporting date) - 90
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON cc.customer_id = s.customer_id
	WHERE dd.datum BETWEEN '2021-06-01' AND CURRENT_DATE
		AND day_is_first_of_month = 1
	GROUP BY 1
),
account_closure AS 
(
    SELECT 
        date_trunc('month',legal_closure_date) AS account_closure_date_,
        count(distinct customer_id) AS closed_accounts
    GROUP BY 1)
SELECT
    dd.datum AS date_month,
    -- Key metrics
    COALESCE(co.users_in_waitlist,0) AS users_in_waitlist,
    SUM(co.users_in_waitlist) OVER( ORDER BY datum rows unbounded preceding) AS total_users_in_waitlist,
    COALESCE(co.total_card_users,0) AS total_new_card_users,
    SUM(co.total_card_users) OVER( ORDER BY datum rows unbounded preceding) AS total_card_users,
    COALESCE(co.card_requested_users,0) AS card_requested_users ,
    COALESCE(co.unique_transaction,0) AS unique_transaction , -- MAU
    COALESCE(co.total_transactions,0) AS total_transactions ,
    COALESCE(ac.closed_accounts,0) AS closed_accounts ,
    -- Funnel metrics
    COALESCE(fm.users_made_first_deposit,0) AS users_made_first_deposit,
    SUM(users_made_first_deposit) OVER( ORDER BY months rows unbounded preceding) AS total_users_made_first_deposit,
    COALESCE(fm.users_made_first_purchase,0) AS users_made_first_purchase,
    SUM(users_made_first_purchase) OVER( ORDER BY months rows unbounded preceding) AS total_users_made_first_purchase,
    COALESCE(fm.user_activations_technical,0) AS users_with_technical_activation,
    -- Subscription metrics  
    COALESCE(sm.total_subs_after_card,0) AS total_subs_after_card,
    COALESCE(sm.total_subs_after_card_activated,0) AS total_subs_after_card_activated,
    COALESCE(sm.acquired_subs_value_after_card,0) AS acquired_subs_value_after_card,
    COALESCE(sm.acquired_subs_value_after_card_activated,0) AS acquired_subs_value_after_card_activated,
    COALESCE(sm.total_csv_after_card,0) AS total_csv_after_card,
    -- Transaction metrics
    COALESCE(tm.num_transactions,0) AS number_card_transactions,
    COALESCE(tm.total_trans_volume_euro,0) AS total_trans_volume_euro,
    -- Grover Cash metrics
    COALESCE(gc.total_cash_issuance,0) AS total_cash_issuance,
    COALESCE(gc.total_cash_redemptions,0) AS total_cash_redemptions,
    COALESCE(gc.running_issuance,0) AS running_issuance,
    COALESCE(gc.running_redemption,0) AS running_redemption,
    -- Session metrics (web)
    COALESCE(wb_card.users,0) AS total_cardusers_browsing,
    COALESCE(wb_card.sessions_after_card,0) AS total_sessions_cardusers,
    COALESCE(wb_card.time_engaged_after_card,0) AS total_sesstime_cardusers_s,
    COALESCE(wb_nocard.users,0) AS total_nocardactivssubs_browsing,
    COALESCE(wb_nocard.total_sessions,0) AS total_sessions_nocardactivssubs,
    COALESCE(wb_nocard.total_time_engaged,0) AS total_sesstime_nocardactivssubs_s,
	-- Page view metrics (app)
	COALESCE(gad.num_page_views,0) AS total_page_views_card_tab,
	COALESCE(gad.num_users,0) AS total_users_on_card_tab,
	COALESCE(gad.num_customers,0) AS total_customers_on_card_tab,
	COALESCE(gad.card_accounts,0) AS total_card_users_app,
	COALESCE(gad.card_ordered,0) AS total_card_orders_app,
    -- ATM metrics
    COALESCE(atmw.ATMwithdrawals_count,0) AS ATMwithdrawals_count,
    COALESCE(atmw.ATMwithdrawals_transvolume_eur,0) AS ATMwithdrawals_transvolume_eur,
    -- SCT transfer metrics 
    COALESCE(scti.SCT_incoming_numtransactions,0) AS SCT_incoming_numtransactions,
    COALESCE(scti.SCT_incoming_transvolume,0) AS SCT_incoming_transvolume,
    COALESCE(scto.SCT_outgoing_transvolume,0) AS SCT_outgoing_transvolume,
    -- Subscription behaviour-change metrics
    COALESCE(sbc.num_activated_users_plus90,0) AS num_activated_users_plus90,
    COALESCE(sbc.subs_at_onboarding,0) AS subs_at_onboarding,
    COALESCE(sbc.subs_at_reportingdate,0) AS subs_at_reportingdate,
    COALESCE(sbc.ASV_at_onboarding_euro,0) AS ASV_at_onboarding_euro,
    COALESCE(sbc.ASV_at_reportingdate_euro,0) AS ASV_at_reportingdate_euro
FROM public.dim_dates dd
    ON dd.datum = co.months 
LEFT JOIN funnel_metrics fm 
    ON dd.datum = fm.date_
LEFT JOIN subscription_metrics sm 
    ON dd.datum = sm.date_  
LEFT JOIN transaction_metrics tm 
    ON dd.datum = tm.date_
    ON dd.datum = gc.date_
LEFT JOIN session_metrics_card_users wb_card 
    ON dd.datum = wb_card.date_
LEFT JOIN session_metrics_non_card_users wb_nocard 
    ON dd.datum = wb_nocard.date_
LEFT JOIN ATM_withdrawals atmw
    ON dd.datum = atmw.date_
LEFT JOIN sct_incoming scti
    ON dd.datum = scti.date_
LEFT JOIN sct_outgoing scto
    ON dd.datum = scto.date_
LEFT JOIN subscription_behaviour_change sbc
    ON dd.datum = sbc.date_
LEFT JOIN account_closure ac 
    ON dd.datum=ac.account_closure_date_
	ON dd.datum = gad.date_
WHERE dd.datum BETWEEN '2019-01-01' AND CURRENT_DATE
    AND day_is_first_of_month = 1
ORDER BY date_month DESC
WITH NO SCHEMA BINDING;
