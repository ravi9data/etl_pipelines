-- Recurring customers limits in production will be updated according to this table:
-- !Test carefully any changes!
DROP TABLE IF EXISTS dm_risk.spain_customer_limit;
CREATE TABLE dm_risk.spain_customer_limit AS
WITH cust_default_date_info AS
(
	SELECT
		customer_id ,
		min(CASE WHEN sp.due_date::DATE <= CURRENT_DATE AND due_date::date >= current_date - 180 THEN due_date ELSE NULL END) AS due_date_l6m_dpd,
		min(CASE WHEN sp.due_date::DATE <= CURRENT_DATE AND due_date::date >= current_date - 180 AND sp.dpd >= 30 THEN due_date ELSE NULL END) AS due_date_l6m_dpd_adj ,
		max(CASE WHEN (sp.paid_date IS NULL AND COALESCE(sp.amount_paid, 0) = 0) OR status = 'FAILED' THEN sp.dpd ELSE 0 END) AS max_dpd_failed_cust,
		CASE
			WHEN max_dpd_failed_cust > 0 THEN current_date - max_dpd_failed_cust
			ELSE NULL
		END AS default_date_failed,
		LEAST(due_date_l6m_dpd_adj, default_date_failed) AS min_default_date
	FROM
		master.subscription_payment sp
	WHERE
		sp.status NOT IN (
			'PLANNED', 'HELD'
		)
		AND customer_type = 'normal_customer'
		AND payment_type = 'RECURRENT'
	GROUP BY
		1
),
cust_dpd AS(
	SELECT
		sp.customer_id AS customer_id_key,
		min(sp.billing_period_start) AS min_billing_start_cust,
		count(DISTINCT o.store_country) AS cnt_unq_country,
		max(CASE WHEN o.store_country = 'Spain' THEN 1 ELSE 0 END) AS spain_flag,
		max(CASE WHEN sp.status NOT IN ('PAID', 'PLANNED') THEN sp.dpd ELSE 0 END) AS max_dpd_failed_cust,
		max(CASE WHEN sp.status NOT IN ('PLANNED') THEN sp.dpd ELSE 0 END) AS max_dpd_ever_cust,
		max(CASE WHEN due_date::date >= current_date - 180 AND sp.status NOT IN ('PLANNED', 'HELD') THEN sp.dpd ELSE 0 END) AS max_dpd_L6M_cust,
		max(CASE WHEN due_date::date >= current_date -90 AND sp.status NOT IN ('PLANNED', 'HELD') THEN sp.dpd ELSE 0 END) AS max_dpd_L3M_cust,
		sum(CASE WHEN sp.status = 'PLANNED' THEN 0 ELSE amount_due END)- sum(CASE WHEN sp.status = 'PLANNED' THEN 0 ELSE amount_paid END) AS total_outstanding_cust,
		sum(CASE WHEN sp.status IN ('FAILED', 'FAILED_FULLY', 'FAILED FULLY') THEN 1 ELSE 0 END) AS n_failed_cust,
		sum(CASE WHEN sp.status = 'PAID' THEN 1 ELSE 0 END) AS n_paid_cust,
		count(DISTINCT date_trunc('month', sp.due_date)) AS cnt_month_payment_cust,
		sum(CASE WHEN sp.status IN ('PLANNED', 'HELD') THEN 0 ELSE amount_due END) AS total_amount_due_cust,
		sum(CASE WHEN sp.status IN ('PLANNED', 'HELD') THEN 0 ELSE amount_paid END) AS total_amount_paid_cust,
		count(DISTINCT CASE WHEN sp.due_date >= cdi.min_default_date THEN NULL ELSE date_trunc('month', sp.due_date) END) AS cnt_month_payment_cust_asofdefault,
		sum(CASE WHEN sp.due_date >= cdi.min_default_date THEN NULL ELSE CASE WHEN sp.status IN ('PLANNED', 'HELD') THEN 0 ELSE amount_due END END) AS total_amount_due_cust_asofdefault,
		sum(CASE WHEN sp.due_date >= cdi.min_default_date THEN NULL ELSE CASE WHEN sp.status IN ('PLANNED', 'HELD') THEN 0 ELSE amount_paid END END) AS total_amount_paid_cust_asofdefault
	FROM
		master.subscription_payment sp
	LEFT JOIN 
	master."order" o 
	ON
		o.order_id = sp.order_id
	LEFT JOIN 
		cust_default_date_info cdi 
		ON
		cdi.customer_id = sp.customer_id
	WHERE
		sp.status NOT IN (
			'PLANNED', 'HELD'
		)
			AND sp.due_date::DATE <= CURRENT_DATE
			AND o.customer_type = 'normal_customer'
		GROUP BY
			1
		HAVING
			max(CASE WHEN o.store_country = 'Spain' THEN 1 ELSE 0 END) = 1
				AND count(DISTINCT o.store_country) = 1
),
equifax_unq_pre AS 
(
	SELECT
		customer_id,
		created_at,
		score_value,
		ROW_NUMBER() OVER (
			PARTITION BY customer_id
		ORDER BY
			created_at ASC
		) AS row_no
	FROM
		s3_spectrum_rds_dwh_order_approval.equifax_risk_score_data ersd
),
equifax_unq AS 
(
	SELECT 
		*
	FROM 
		equifax_unq_pre
	WHERE 
		row_no = 1
),
delphi_unq_pre AS
(
	SELECT
		customer_id,
		created_at,
		score_value,
		score_note,
		ROW_NUMBER() OVER (
			PARTITION BY customer_id
		ORDER BY
			created_at ASC
		) AS row_no
	FROM
		s3_spectrum_rds_dwh_order_approval.experian_es_delphi_data esd
),
delphi_unq AS 
(
	SELECT
		*
	FROM
		delphi_unq_pre
	WHERE
		row_no = 1
),
result_ AS
(
	SELECT
		cd.customer_id_key AS customer_id,
		c.customer_type ,
		CAST(eu.score_value AS float) AS equifax_score,
		COALESCE (
			CAST(eu.score_value AS float),
			sb.score
		) AS equifax_score_imp,
		CASE
			WHEN equifax_score_imp < 101 THEN 0
			WHEN equifax_score_imp < 293 THEN 30
			WHEN equifax_score_imp < 521 THEN 70
			WHEN equifax_score_imp < 629 THEN 85
			WHEN equifax_score_imp < 780 THEN 100
			WHEN equifax_score_imp < 828 THEN 175
			WHEN equifax_score_imp >= 828 THEN 200
			WHEN equifax_score_imp IS NULL THEN 
	   	CASE
				WHEN du.score_note IN (
					'H', 'I'
				) THEN 0
				WHEN du.score_note IN ('G') THEN 30
				WHEN du.score_note IN ('F') THEN 70
				WHEN du.score_note IN ('E') THEN 85
				WHEN du.score_note IN ('D') THEN 100
				WHEN du.score_note IN ('C') THEN 175
				WHEN du.score_note IN (
					'A', 'B'
				) THEN 200
				WHEN du.score_note IS NULL THEN 0
				ELSE 0
			END
			ELSE 0
		END AS new_initial_limit,
		CASE
			WHEN equifax_score_imp < 101 THEN 0
			WHEN equifax_score_imp < 293 THEN 1
			WHEN equifax_score_imp < 521 THEN 2
			WHEN equifax_score_imp < 629 THEN 3
			WHEN equifax_score_imp < 780 THEN 4
			WHEN equifax_score_imp < 828 THEN 5
			WHEN equifax_score_imp >= 828 THEN 6
			WHEN equifax_score_imp IS NULL THEN 
	   	CASE
				WHEN du.score_note IN (
					'H', 'I'
				) THEN 0
				WHEN du.score_note IN ('G') THEN 1
				WHEN du.score_note IN ('F') THEN 2
				WHEN du.score_note IN ('E') THEN 3
				WHEN du.score_note IN ('D') THEN 4
				WHEN du.score_note IN ('C') THEN 5
				WHEN du.score_note IN (
					'A', 'B'
				) THEN 6
				WHEN du.score_note IS NULL THEN 0
				ELSE 0
			END
			ELSE 0
		END AS new_initial_limit_counter,
		du.score_note AS delphi_note,
		c.shipping_country,
		---	   c.initial_subscription_limit ,
		---	   c.subscription_limit_change_date::date ,
		---	   c.subscription_limit ,
		c.active_subscription_value ,
		date_trunc('month', cd.min_billing_start_cust)::date AS min_subs_start_month,
		cd.*,
		cdi.default_date_failed,
		cdi.due_date_l6m_dpd_adj,
		cdi.due_date_l6m_dpd,
		cdi.min_default_date,
		floor(cd.cnt_month_payment_cust / 3::float) AS month_subcounter_3m_asofnow,
		floor(cd.total_amount_paid_cust / 90::float) AS amount_subcounter_90eur_asofnow,
		floor(COALESCE(cd.cnt_month_payment_cust_asofdefault / 3::float, 0)) AS month_subcounter_3m,
		floor(COALESCE(cd.total_amount_paid_cust_asofdefault, 0) / 90::float) AS amount_subcounter_90eur,
		---	   floor(cd.total_amount_paid_cust_asofdefault / 120::float) as amount_subcounter_120eur,
		---	   floor(cd.total_amount_paid_cust_asofdefault / 150::float) as amount_subcounter_150eur,
	   LEAST(month_subcounter_3m, amount_subcounter_90eur) AS increment_base,
		new_initial_limit_counter + increment_base AS final_limit_counter_pre,
		LEAST(10, new_initial_limit_counter + increment_base) AS final_limit_counter_adj,
		CASE
			WHEN final_limit_counter_adj = 0 THEN 0
			WHEN final_limit_counter_adj = 1 THEN 30
			WHEN final_limit_counter_adj = 2 THEN 70
			WHEN final_limit_counter_adj = 3 THEN 85
			WHEN final_limit_counter_adj = 4 THEN 100
			WHEN final_limit_counter_adj = 5 THEN 175
			WHEN final_limit_counter_adj = 6 THEN 200
			WHEN final_limit_counter_adj = 7 THEN 250
			WHEN final_limit_counter_adj = 8 THEN 300
			WHEN final_limit_counter_adj = 9 THEN 350
			WHEN final_limit_counter_adj = 10 THEN 400
			ELSE -9999
		END AS new_final_limit
	FROM
		cust_dpd cd
	LEFT JOIN 
	master.customer c 
	ON
		c.customer_id = cd.customer_id_key
	LEFT JOIN 
		equifax_unq eu 
		ON
		c.customer_id = eu.customer_id
	LEFT JOIN 
			delphi_unq du 
			ON
		c.customer_id = du.customer_id
	LEFT JOIN 
				backup_tables.risk_spain_backtesting_20230614 sb 
				ON
		sb.customer_id = cd.customer_id_key
	LEFT JOIN cust_default_date_info cdi 
					ON
		cd.customer_id_key = cdi.customer_id
	WHERE
		1 = 1
)
SELECT
	customer_id,
	shipping_country AS country_name,
	equifax_score_imp,
	new_initial_limit_counter AS initial_limit_counter,
	new_initial_limit AS initial_limit,
	default_date_failed,
	due_date_l6m_dpd_adj AS dpd30_hit_date,
	min_default_date,
	cnt_month_payment_cust_asofdefault,
	total_amount_paid_cust_asofdefault,
	increment_base,
	final_limit_counter_adj,
	new_final_limit AS subscription_limit,
	'initial limit: ' || new_initial_limit || ', paid >=' || (amount_subcounter_90eur) * 90 || '€ in >= ' || month_subcounter_3m * 3 || 'M, ' || CASE
		WHEN due_date_l6m_dpd_adj IS NOT NULL THEN 'unstable payment history'
		ELSE ''
	END AS "comment"
FROM
	result_
where month_subcounter_3m != 0 -- Too limited payment history
;

-- Recurring customers limits in production will be upgraded according to this table:
-- !Test carefully any changes!
DROP TABLE IF EXISTS dm_risk.customer_limits_upgrades;
CREATE TABLE dm_risk.customer_limits_upgrades AS
SELECT
	scl.customer_id,
	scl.country_name,
	scl.subscription_limit,
	scl.subscription_limit * 25 AS credit_limit,
	scl."comment"
FROM
	dm_risk.spain_customer_limit AS scl
LEFT JOIN ods_production.customer_scoring AS cs
ON
	scl.customer_id = cs.customer_id
WHERE
	scl.subscription_limit != cs.current_subscription_limit
	OR scl."comment" != cs.customer_scoring_result
;

GRANT SELECT ON dm_risk.customer_limits_upgrades TO risk_credit_limit;
