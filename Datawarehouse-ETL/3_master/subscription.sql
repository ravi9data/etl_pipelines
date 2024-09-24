BEGIN;
DROP TABLE IF EXISTS subscription_final;
CREATE TEMP TABLE subscription_final as
WITH min_date AS ( --WHEN NEW infra switch TABLE IS incorporated IN phase_mapping -> CHANGE whole orig_sub_value logic TO use phase_mapping WITH phase_indx = 1
	SELECT
		sh.subscription_id,
    min(CASE WHEN sh.subscription_value IS NOT NULL THEN date END) AS min_date,
		min(CASE WHEN sh.subscription_value = 0 THEN date END) AS free_year_offer_start_date
	FROM master.subscription_historical sh
	WHERE sh.subscription_value IS NOT NULL --certain subs do not have subs_value in the first date  
	GROUP BY 1
)
, original_sub_value_historical AS (
	SELECT 
		d.subscription_id,
		sh.subscription_value AS original_subscription_value,
		(case
	 	when upper(sh.currency) !='EUR' and exc.date_ is not null then (subscription_value::double precision * exc.exchange_rate_eur)
	 	when upper(sh.currency) !='EUR' and exc.date_ is null then (subscription_value::double precision * exc.exchange_rate_eur)
		when upper(sh.currency) ='EUR' then subscription_value::double precision
	 	else sh.subscription_value_euro::double precision
		end)::decimal(10,2) AS original_subscription_value_euro
	FROM min_date d
	INNER JOIN master.subscription_historical sh
		ON sh.subscription_id = d.subscription_id 
			AND sh.date = d.min_date
	LEFT JOIN trans_dev.daily_exchange_rate exc  
		ON upper(sh.currency)  = exc.currency
   			AND exc.date_ = CASE 
								WHEN COALESCE(sh.start_date,sh.date) >= '2022-08-01' 
									THEN ((cast(date_trunc('month',COALESCE(sh.start_date,sh.date)) - interval '1 day' as date)))
			   					WHEN COALESCE(sh.start_date,sh.date) < '2021-03-31' 
			   						THEN '2021-03-31'  --using this cutoff date as this is the min date of the exchange table
			   					WHEN COALESCE(sh.start_date,sh.date) < '2022-08-01'
									THEN sh.start_date	
			   					END
)
, original_sub_value_yesterday AS ( 
	SELECT 
		subscription_id,
		subscription_value AS original_subscription_value,
		subscription_value_euro::decimal(10,2) AS original_subscription_value_euro
	FROM ods_production.subscription s 
	WHERE start_date::date >= CURRENT_DATE - 1
)
, original_sub_value AS ( 
SELECT 
	subscription_id,
	original_subscription_value,
	original_subscription_value_euro
FROM original_sub_value_yesterday
UNION 
SELECT 
	subscription_id,
	original_subscription_value,
	original_subscription_value_euro
FROM original_sub_value_historical
)
, clean_switches AS (
	SELECT 
		subscription_id,
		"date",
		duration_after,
		ROW_NUMBER() OVER (PARTITION BY subscription_id, "date"::date ORDER BY "date" DESC) AS rowno
	FROM ods_production.subscription_plan_switching
)
, total_csv AS (
  SELECT 
    p.subscription_id,
    SUM(
    CASE 
      WHEN p.country_name = 'United States' THEN 
          CASE 
          	--we have the end_date, but subs was cancel before the rental_period
	          WHEN p.end_date IS NOT NULL AND p.latest_phase_idx = 1 AND (DATEDIFF('day', p.start_date, p.end_date)/30.0) < p.rental_period
	        	  THEN p.rental_period - CEILING(DATEDIFF('day', p.start_date, p.fact_day)/30.0) 
            --WHEN we have end_date -> length of that phase
            WHEN p.end_date IS NOT NULL 
              THEN CEILING(DATEDIFF('day', p.fact_day, p.end_date::date)/30.0) 
             -- when end_date NULL and phase length exceeds rental period -> complete timeframe of phase
            WHEN (DATEDIFF('day', p.start_date, CURRENT_DATE)/30.0) > p.rental_period
              THEN CEILING(DATEDIFF('day', p.fact_day, CURRENT_DATE)/30.0) 
            -- end_date NULL AND within rental_period -> rental_period - the month the customer paid for already
            ELSE p.rental_period - CEILING(DATEDIFF('day', p.start_date, p.fact_day)/30.0) 
          END
      --phase switch and not last phase or sub with end_date and not early cancellation -> length of phase
	    WHEN (s1.subscription_id IS NOT NULL AND p.latest_phase_idx <> 1) 
        OR (p.end_date IS NOT NULL AND CEILING(DATEDIFF('day', p.fact_day, p.end_date::date)/30.0) >= COALESCE(s.duration_after, p.rental_period))
				THEN CEILING(DATEDIFF('day', p.fact_day, p.end_date::date)/30.0)
      --last/actual phase is within rental period -> take rental period or latest committed duration
	    WHEN (DATEDIFF('day', p.fact_day, COALESCE(p.end_date::date, CURRENT_DATE))/30.0) < COALESCE(s.duration_after, p.rental_period)
        THEN COALESCE(s.duration_after, p.rental_period)
      --last/actual phase exceeded rental period -> length of phase
	    ELSE CEILING(DATEDIFF('day', p.fact_day, CURRENT_DATE)/30.0) 
	  END * p.subscription_value_eur
    ) AS total_csv
  FROM ods_production.subscription_phase_mapping p
  LEFT JOIN clean_switches s
    ON p.subscription_id = s.subscription_id
    AND p.fact_day::date = s."date"::date
    AND s.rowno = 1
  LEFT JOIN (SELECT DISTINCT subscription_id FROM clean_switches) s1
    ON p.subscription_id = s1.subscription_id
  GROUP BY 1
)
SELECT DISTINCT
    s.subscription_id,
    s.subscription_name AS subscription_sf_id,
    o.created_date AS order_created_date,
    s.created_date,
    s.updated_date,
    s.start_date,
    s.rank_subscriptions,
    min(s.start_date) OVER (PARTITION BY s.customer_id) AS first_subscription_start_date,
    s.subscriptions_per_customer,
    s.customer_id,
    coalesce(c.customer_type,'normal_customer') AS customer_type,
    cust.customer_acquisition_cohort,
    c.subscription_limit,
    s.order_id,
    s.store_id,
    st.store_name::VARCHAR(510),
    st.store_label::VARCHAR(30),
    st.store_type,
    st.store_number,
    s.account_name,
    s.status,
    s.variant_sku,
    s.allocation_status,
    s.allocation_tries,
    s.cross_sale_attempts,
    s.replacement_attempts,
    sa.allocated_assets,
    sa.delivered_assets,
    sa.returned_packages,
    sa.returned_assets,
    COALESCE(sa.outstanding_assets,0) AS outstanding_assets,
    COALESCE(sa.outstanding_purchase_price,0) AS outstanding_asset_value,
    coalesce(sa.outstanding_residual_asset_value,0) AS outstanding_residual_asset_value,
    COALESCE(sa.outstanding_rrp,0) AS outstanding_rrp,
    coalesce(sa.first_asset_delivered,s.first_asset_delivery_date) AS first_asset_delivery_date,
    sa.last_return_shipment_at,
    s.subscription_plan,
    -- s.subscription_value AS monthly_subscription_payment,
    s.rental_period,
    s.subscription_value,
     s.subscription_value_euro,
      s.reporting_subscription_value_euro,
    s.committed_sub_value,
    sc.default_date AS next_due_date,
    CASE
      WHEN s.status::TEXT = 'ACTIVE'::TEXT 
      AND
          CASE
            WHEN (s.committed_sub_value::DOUBLE PRECISION - COALESCE(sc.subscription_revenue_paid, 0::NUMERIC)::DOUBLE PRECISION) < s.subscription_value THEN 0::DOUBLE PRECISION
            ELSE s.committed_sub_value::DOUBLE PRECISION - COALESCE(sc.subscription_revenue_paid, 0::NUMERIC)::DOUBLE PRECISION
          END = 0::DOUBLE PRECISION THEN s.subscription_value
      ELSE
        CASE
          WHEN (s.committed_sub_value::DOUBLE PRECISION - COALESCE(sc.subscription_revenue_paid, 0::NUMERIC)::DOUBLE PRECISION) < s.subscription_value THEN 0::DOUBLE PRECISION
          ELSE s.committed_sub_value::DOUBLE PRECISION - COALESCE(sc.subscription_revenue_paid, 0::NUMERIC)::DOUBLE PRECISION
        END
    END AS commited_sub_revenue_future,
    s.currency,
    s.subscription_duration,
    CASE
      WHEN s.status = 'CANCELLED' THEN greatest(payment_count,1)
      WHEN s.status = 'ACTIVE' THEN greatest(minimum_term_months,payment_count+1)
    END AS effective_duration,
    CASE
      WHEN s.status = 'CANCELLED' THEN NULL
      WHEN s.status = 'ACTIVE' THEN greatest(minimum_term_months,payment_count+1)-payment_count
    END AS outstanding_duration,
    s.months_required_TO_OWN,
    sc.max_payment_number,
    sc.payment_count,
    sc.paid_subscriptions,
    sc.last_valid_payment_category,
    sc.dpd,
    sc.subscription_revenue_due,
    sc.subscription_revenue_paid,
    sc.outstanding_subscription_revenue,
    sc.subscription_revenue_refunded,
    sc.subscription_revenue_chargeback,
    sc.net_subscription_revenue_paid,
    s.cancellation_date,
    s.cancellation_note,
    cr.cancellation_reason,
    cr.cancellation_reason_new,
    cr.cancellation_reason_churn,
    cr.is_widerruf,
    s.payment_method,
    s.debt_collection_handover_date,  
    s.dc_status::VARCHAR(21),
    s.result_debt_collection_contact,
    sa.avg_asset_purchase_price,
    p.product_sku,
    p.product_name,
    p.category_name,
    p.subcategory_name,
    p.brand,
    nr.new_recurring,
    nr.retention_group,
    s.minimum_cancellation_date,
    s.minimum_term_months,
    asset_cashflow_from_old_subscriptions,
    exposure_to_default,
    CASE 
      WHEN net_subscription_revenue_paid- (outstanding_rrp+3*s.subscription_value)<=0 
        OR coalesce(outstanding_rrp,0) = 0 
        THEN NULL 
      ELSE net_subscription_revenue_paid- (outstanding_rrp+3*s.subscription_value) 
    END AS mietkauf_amount_overpayment,
    CASE 
      WHEN (months_required_to_own IS NULL) OR (months_required_to_own LIKE '%.%')
        THEN 
          CASE WHEN mietkauf_amount_overpayment > 0 THEN True ELSE false END 
      ELSE  
          CASE WHEN max_payment_number::INT > months_required_to_own::INT THEN True ELSE false END
    END is_eligible_for_mietkauf,
    s.trial_days,
    s.trial_variant,
    sc.is_not_triggered_payments,
    (CASE 
      WHEN sa.asset_recirculation_status='New' THEN 'New'::VARCHAR(65535)
      WHEN sa.asset_recirculation_status LIKE ('%Re-circulated%') THEN 'Re-circulated'::VARCHAR(65535)
      WHEN sa.asset_recirculation_status IS NULL THEN 'N/A' ::VARCHAR(65535)
    END)::VARCHAR(65535) AS asset_recirculation_status,
    s.store_short,
    s.country_name,
    s.store_commercial,
    s.subscription_bo_id,
    s.buyout_disabled,
    s.buyout_disabled_at,
    s.buyout_disabled_reason,
    sv.original_subscription_value,
    sv.original_subscription_value_euro,
    f.free_year_offer_start_date,
    s.reactivated_date,
    CASE 
      WHEN tc.total_csv >= s.committed_sub_value 
        THEN tc.total_csv - s.committed_sub_value
      ELSE 0 --to avoid negative additional_csv (edge cases)
    END as additional_committed_sub_value,
    s.state
    -- ,o.is_special_voucher
  FROM ods_production.subscription s
    LEFT JOIN ods_production.store st 
      ON s.store_id = st.id
    LEFT JOIN ods_production.order o 
      ON o.order_id=s.order_id 
    LEFT JOIN ods_production.order_retention_group nr 
      ON nr.order_id = s.order_id
    LEFT JOIN ods_production.customer c 
      ON c.customer_id = s.customer_id
    LEFT JOIN ods_production.subscription_cashflow sc 
      ON s.subscription_id = sc.subscription_id
    LEFT JOIN ods_production.subscription_assets sa 
      ON s.subscription_id = sa.subscription_id
    LEFT JOIN ods_production.subscription_cancellation_reason cr 
      ON cr.subscription_id=s.subscription_id
    LEFT JOIN ods_production.customer_acquisition_cohort cust 
      ON cust.customer_id=s.customer_id
    LEFT JOIN ods_production.variant v 
      ON v.variant_sku=s.variant_sku
    LEFT JOIN ods_production.product p
      ON p.product_id=v.product_id
    LEFT JOIN total_csv tc
      ON tc.subscription_id = s.subscription_id
    LEFT JOIN original_sub_value sv
      ON sv.subscription_id = s.subscription_id	
    LEFT JOIN min_date f
      ON f.subscription_id = s.subscription_id
;

TRUNCATE TABLE master.subscription;

INSERT INTO master.subscription
SELECT * FROM subscription_final;

COMMIT;