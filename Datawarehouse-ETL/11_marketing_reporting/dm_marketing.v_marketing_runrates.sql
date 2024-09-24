CREATE OR REPLACE VIEW dm_marketing.v_marketing_runrates AS 
WITH  total_days_in_month AS (
	SELECT 
		date_trunc('month',dd.datum)::date AS month_,
		count(DISTINCT dd.datum) AS number_days_in_a_month
	FROM public.dim_dates dd 
	WHERE  date_trunc('month',dd.datum) <= date_trunc('month',current_date)	
		AND date_trunc('month',dd.datum) >= date_trunc('month',dateadd('month',-4,current_date))
	GROUP BY 1	
)
,  mkt_channels AS (
	SELECT DISTINCT 
		channel AS marketing_channel,
		channel AS marketing_channel_detailed,
		'n/a'::varchar AS marketing_source,
		'n/a'::varchar AS marketing_medium
	FROM staging.budget_runrates_input
	--
	UNION 
	--
	SELECT 
		'n/a'::varchar AS marketing_channel,
		'n/a'::varchar AS marketing_channel_detailed,
		'n/a'::varchar AS marketing_source,
		'n/a'::varchar AS marketing_medium
	--
	UNION 
	--
    SELECT DISTINCT
    	omc.marketing_channel,
		COALESCE(um.marketing_channel, 
				CASE WHEN omc.marketing_channel IN ('Paid Search Brand', 
												'Paid Search Non Brand', 
												'Paid Social Branding',
                                                'Paid Social Performance',
                                                'Display Branding',
                                                'Display Performance')
                                              THEN omc.marketing_channel || ' Other'
					END, omc.marketing_channel,'n/a') AS marketing_channel_detailed,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium
    FROM master.ORDER o
             LEFT JOIN ods_production.order_marketing_channel omc
                       ON omc.order_id = o.order_id
             LEFT JOIN marketing.utm_mapping um
                       ON omc.marketing_channel = um.marketing_channel_grouping
                           AND omc.marketing_source = um.marketing_source
)
, countries AS (
	SELECT 
		country
	FROM staging.budget_runrates_input
	UNION
	SELECT 
		country
	FROM marketing.marketing_cost_daily_base_data
	WHERE "date"::date >= '2023-01-01'
)
, dimensions_combined_with_mkt_source_and_medium AS (
	SELECT DISTINCT 
		dd.datum AS reporting_date,
		COALESCE(m.marketing_channel, 'n/a') AS channel,
		COALESCE(c.country, 'n/a') AS country,
		COALESCE(m.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
		COALESCE(m.marketing_source, 'n/a') AS marketing_source,
		COALESCE(m.marketing_medium, 'n/a') AS marketing_medium
	FROM public.dim_dates dd
	CROSS JOIN mkt_channels m
	CROSS JOIN countries c
	WHERE dd.datum <= current_date -1
		AND date_trunc('month',dd.datum) >= date_trunc('month',dateadd('month',-4,current_date))
		AND (dd.datum = last_day(dd.datum) 
		       OR dd.datum = current_date -1	)				       
)
, mkt_channel_for_cost AS (
	SELECT DISTINCT 
		CASE WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Search' THEN
                   CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
                        WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
                        WHEN co.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
                        WHEN co.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
                        ELSE 'Paid Search Non Brand' END
               WHEN COALESCE(co.channel_grouping,'n/a') = 'Display' THEN
                   CASE WHEN co.campaign_name ILIKE '%awareness%' OR co.campaign_name ILIKE '%traffic%' THEN 'Display Branding'
                        ELSE 'Display Performance' END
               WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Social' THEN
                   CASE WHEN co.campaign_name ILIKE '%brand%' THEN 'Paid Social Branding'
                        ELSE 'Paid Social Performance' END
               ELSE COALESCE(co.channel_grouping,'n/a')
        END AS marketing_channel,
        COALESCE(CASE WHEN marketing_channel = 'Paid Search Non Brand' then co.channel || ' ' || 'Non Brand'
                WHEN marketing_channel = 'Paid Search Brand' then co.channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Display Branding'  then co.channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Display Performance' then co.channel || ' ' || 'Performance'
                WHEN marketing_channel = 'Paid Social Branding' then co.channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Paid Social Performance' then co.channel || ' ' || 'Performance'
                ELSE co.channel END, 'n/a') AS marketing_channel_detailed,
        'n/a'::varchar AS marketing_source,
		'n/a'::varchar AS marketing_medium
	FROM marketing.marketing_cost_daily_combined co
	LEFT JOIN marketing.campaigns_brand_non_brand b
		ON b.campaign_name = co.campaign_name
)
, dimensions_combined AS (
	SELECT DISTINCT 
		reporting_date,
		channel,
		country,
		marketing_channel_detailed
	FROM dimensions_combined_with_mkt_source_and_medium
	--
	UNION
	--
	SELECT DISTINCT 
		dd.datum AS reporting_date,
		COALESCE(m.marketing_channel, 'n/a') AS channel,
		COALESCE(c.country, 'n/a') AS country,
		COALESCE(m.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
	FROM public.dim_dates dd
	CROSS JOIN mkt_channel_for_cost m
	CROSS JOIN countries c
	WHERE dd.datum <= current_date -1
		AND date_trunc('month',dd.datum) >= date_trunc('month',dateadd('month',-4,current_date))
		AND (dd.datum = last_day(dd.datum) 
		       OR dd.datum = current_date -1	)		       
)
, last_month AS (
	SELECT DISTINCT 
		max(reporting_date) AS max_reporting_date
	FROM dimensions_combined
)
, budget AS (
	SELECT 
		date_trunc('month',s."month"::date)::date AS reporting_date,
		COALESCE(s.channel, 'n/a') AS channel,
		COALESCE(s.country, 'n/a') AS country,
		COALESCE(replace(replace(replace(s.budget,'€',''),',',''),'.',','),'0')::float AS budget_full_month
	FROM staging.budget_runrates_input s 
	WHERE date_trunc('month',s."month"::date) >= date_trunc('month',dateadd('month',-4,current_date))
		AND s."month" <= date_trunc('month', current_date)
)
, orders AS (
	SELECT 
		COALESCE(o.marketing_channel, 'n/a') AS channel,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium,
		COALESCE(o.store_country, 'n/a') AS country,
		date_trunc('month',o.submitted_date::date)::date AS reporting_date,
		COALESCE(COUNT(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END),0) AS paid_orders,
		COALESCE(COUNT(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END),0) AS submitted_orders,
		COALESCE(COUNT(DISTINCT CASE WHEN o.paid_orders >= 1 AND o.new_recurring = 'NEW' THEN o.customer_id END),0) AS new_customers
	FROM master.ORDER o 
	LEFT JOIN ods_production.order_marketing_channel omc
        ON o.order_id = omc.order_id
    WHERE date_trunc('month',o.submitted_date::date) >= date_trunc('month',dateadd('month',-4,current_date))
    	AND o.submitted_date::date < current_date
	GROUP BY 1,2,3,4,5	
)
, subs AS (
	SELECT 
		COALESCE(o.marketing_channel, 'n/a') AS channel,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium,
		COALESCE(o.store_country, 'n/a') AS country,
		date_trunc('month',s.start_date::date)::date AS reporting_date,
		SUM(s.subscription_value_euro) as acquired_subscription_value
	FROM master.order o
	LEFT JOIN master.subscription s
	    ON s.order_id = o.order_id
	LEFT JOIN ods_production.order_marketing_channel omc
        ON o.order_id = omc.order_id
    WHERE date_trunc('month',s.start_date::date) >= date_trunc('month',dateadd('month',-4,current_date))
    	AND s.start_date::date < current_date
    GROUP BY 1,2,3,4,5	
)
, cancel_subs AS ( 
	SELECT 
		COALESCE(o.marketing_channel, 'n/a') AS channel,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium,
		COALESCE(o.store_country, 'n/a') AS country,
		date_trunc('month',s.cancellation_date::date)::date AS reporting_date,
		SUM(s.subscription_value_euro) as cancelled_subscription_value
	FROM master.order o
	LEFT JOIN master.subscription s
	    ON s.order_id = o.order_id
	LEFT JOIN ods_production.order_marketing_channel omc
        ON o.order_id = omc.order_id
    WHERE date_trunc('month',s.cancellation_date::date) >= date_trunc('month',dateadd('month',-4,current_date))
    	AND s.cancellation_date::date < current_date
    GROUP BY 1,2,3,4,5
)
, cost_with_budget AS (
	SELECT 
		CASE WHEN COALESCE(m.channel_grouping,'n/a') = 'Paid Search' THEN
		      CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
		           WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
		           WHEN m.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
		           WHEN m.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
		        ELSE 'Paid Search Non Brand' END
		      WHEN COALESCE(m.channel_grouping,'n/a') = 'Display' THEN 
		      CASE WHEN m.campaign_name ILIKE '%awareness%' OR m.campaign_name ILIKE '%traffic%' THEN 'Display Branding' 
		        ELSE 'Display Performance' END 
		      WHEN COALESCE(m.channel_grouping,'n/a') = 'Paid Social' THEN 
		      CASE WHEN m.campaign_name ILIKE '%brand%' THEN 'Paid Social Branding'
		      ELSE 'Paid Social Performance' END
		    ELSE COALESCE(m.channel_grouping,'n/a') END AS marketing_channel,
		COALESCE(CASE WHEN marketing_channel = 'Paid Search Non Brand' then m.channel || ' ' || 'Non Brand'
                        WHEN marketing_channel = 'Paid Search Brand' then m.channel || ' ' || 'Brand'
                        WHEN marketing_channel = 'Display Branding'  then m.channel || ' ' || 'Brand'
                        WHEN marketing_channel = 'Display Performance' then m.channel || ' ' || 'Performance'
                        WHEN marketing_channel = 'Paid Social Branding' then m.channel || ' ' || 'Brand'
                        WHEN marketing_channel = 'Paid Social Performance' then m.channel || ' ' || 'Performance'
                        ELSE m.channel END, 'n/a') AS marketing_channel_detailed,
        date_trunc('month',m.reporting_date::date)::date AS reporting_date,
		m.country,
		COALESCE(SUM(CASE WHEN m.cash_non_cash = 'Cash' THEN m.total_spent_eur END),0) AS cash_cost,
		COALESCE(SUM(CASE WHEN m.cash_non_cash = 'Non-Cash' THEN m.total_spent_eur END),0) AS non_cash_cost,
		COALESCE(SUM(m.total_spent_eur),0) AS cost
	FROM marketing.marketing_cost_daily_combined m
	LEFT JOIN marketing.campaigns_brand_non_brand b
		ON b.campaign_name = m.campaign_name
	WHERE date_trunc('month',m.reporting_date::date) >= date_trunc('month',dateadd('month',-4,current_date))
		AND m.reporting_date::date < current_date
	GROUP BY 1,2,3,4	
)
, active_metrics_dates AS (
	SELECT DISTINCT 
		d.reporting_date
	FROM dimensions_combined d
)
, active_metrics AS ( 
	SELECT 
		d.reporting_date,
		COALESCE(o.marketing_channel, 'n/a') AS channel,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium,
		COALESCE(o.store_country, 'n/a') AS country,
		COALESCE(SUM(ss.subscription_value_eur),0) AS active_subscription_value
	FROM active_metrics_dates d
	LEFT JOIN ods_production.subscription_phase_mapping ss
		ON d.reporting_date::date >= ss.fact_day::date
		AND d.reporting_date::date <= coalesce(ss.end_date::date, d.reporting_date::date + 1)
	LEFT JOIN master.ORDER o 
		ON ss.order_id = o.order_id 
	LEFT JOIN ods_production.order_marketing_channel omc
        ON o.order_id = omc.order_id
	WHERE TRUE
      AND ss.store_label NOT ilike '%old%'
      AND ss.country_name <> 'United Kingdom'
	GROUP BY 1,2,3,4,5
)
, is_paid AS (
	SELECT DISTINCT marketing_channel, is_paid
	FROM  traffic.sessions s
	WHERE is_paid IS NOT NULL
		AND session_start > '2023-05-01'
	UNION 
	SELECT 
		'Paid Social', TRUE
	UNION 
	SELECT 
		'Vouchers', TRUE
	UNION 
	SELECT 
		'App', TRUE
	UNION 
	SELECT 
		'Grover Cash', TRUE
	UNION 
	SELECT 
		'Display', TRUE
	UNION 
	SELECT 
		'n/a', FALSE
)
, final_for_metrics_with_mkt_source_and_medium AS (
	SELECT 
		d.reporting_date,
		d.channel,
		d.country,
		d.marketing_channel_detailed,
		CASE WHEN lm.max_reporting_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_last_month,
		COALESCE(sum(o.paid_orders),0) AS paid_orders,
		COALESCE(sum(o.submitted_orders),0) AS submitted_orders,
		COALESCE(sum(o.new_customers),0) AS new_customers,
		COALESCE(sum(s.acquired_subscription_value),0) AS acquired_subscription_value,
		COALESCE(sum(cs.cancelled_subscription_value),0) AS cancelled_subscription_value,
		COALESCE(sum(am.active_subscription_value),0) AS active_subscription_value,
		LAG(COALESCE(sum(am.active_subscription_value),0)) OVER (PARTITION BY d.channel, d.country, d.marketing_channel_detailed ORDER BY d.reporting_date) AS previous_month_active_subscription_value
	FROM dimensions_combined_with_mkt_source_and_medium d
	LEFT JOIN orders o 
		ON o.channel = d.channel
		AND o.marketing_source = d.marketing_source
		AND o.marketing_medium = d.marketing_medium
		AND o.country = d.country
		AND date_trunc('month',o.reporting_date) = date_trunc('month',d.reporting_date)
	LEFT JOIN subs s
		ON s.channel = d.channel
		AND s.marketing_source = d.marketing_source
		AND s.marketing_medium = d.marketing_medium
		AND s.country = d.country
		AND date_trunc('month',s.reporting_date) = date_trunc('month',d.reporting_date)
	LEFT JOIN cancel_subs cs
		ON cs.channel = d.channel
		AND cs.marketing_source = d.marketing_source
		AND cs.marketing_medium = d.marketing_medium
		AND cs.country = d.country
		AND date_trunc('month',cs.reporting_date) = date_trunc('month',d.reporting_date)
	full JOIN active_metrics am
		ON am.channel = d.channel
		AND am.marketing_source = d.marketing_source
		AND am.marketing_medium = d.marketing_medium
		AND am.country = d.country
		AND date_trunc('month',am.reporting_date) = date_trunc('month',d.reporting_date)
	LEFT JOIN last_month lm 
		ON d.reporting_date = lm.max_reporting_date
	GROUP BY 1,2,3,4,5 
)
, final_for_metrics_without_mkt_source_and_medium AS (
	SELECT 
		d.reporting_date,
		d.channel,
		d.country,
		d.marketing_channel_detailed,
		CASE WHEN lm.max_reporting_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_last_month,
		COALESCE(sum(o.budget_full_month),0) AS budget_full_month,	
		COALESCE(sum(s.cash_cost),0) AS cash_cost,
		COALESCE(sum(s.non_cash_cost),0) AS non_cash_cost,
		COALESCE(sum(s.cost),0) AS cost	
	FROM dimensions_combined d
	LEFT JOIN budget o 
		ON o.channel = d.channel
		AND o.channel = d.marketing_channel_detailed
		AND o.country = d.country
		AND date_trunc('month',o.reporting_date) = date_trunc('month',d.reporting_date)
		AND o.reporting_date::date <= d.reporting_date
	LEFT JOIN cost_with_budget s
		ON s.marketing_channel = d.channel
		AND s.marketing_channel_detailed = d.marketing_channel_detailed
		AND s.country = d.country
		AND date_trunc('month',s.reporting_date) = date_trunc('month',d.reporting_date)
		AND s.reporting_date::date <= d.reporting_date
	LEFT JOIN last_month lm 
		ON d.reporting_date = lm.max_reporting_date
	GROUP BY 1,2,3,4,5
)	
SELECT DISTINCT
	d.channel,
	d.marketing_channel_detailed,
	d.country,
	d.reporting_date,
	ip.is_paid,
	--budget
	COALESCE(a.budget_full_month,0) AS budget_month,
	round(aa.budget_full_month,0) AS budget_current_month,
	--cost
	a.cash_cost,
	round(COALESCE(aa.cash_cost,0),0) AS cash_cost_current_month, --TO make it easier ON tableau
	a.cash_cost::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_cash_cost,
	a.non_cash_cost,
	round(COALESCE(aa.non_cash_cost,0),0) AS non_cash_cost_current_month,  --TO make it easier ON tableau
	a.non_cash_cost::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_non_cash_cost,
	a.cost,
	round(COALESCE(aa.cost,0),0) AS cost_current_month,  --TO make it easier ON tableau
	a.cost::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_cost,
	--paid orders 
	b.paid_orders,
	round(bb.paid_orders,0) AS paid_orders_current_month, --TO make it easier ON tableau
	b.paid_orders::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_paid_orders,
	--submitted orders 
	b.submitted_orders,
	round(bb.submitted_orders,0) AS submitted_orders_current_month, --TO make it easier ON tableau
	b.submitted_orders::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_submitted_orders,
	--new customers
	b.new_customers,
	round(bb.new_customers,0) AS new_customers_current_month, --TO make it easier ON tableau
	b.new_customers::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_new_customers,	
	--acq subs value 
	b.acquired_subscription_value,
	round(bb.acquired_subscription_value,0) AS acquired_subscription_value_current_month, --TO make it easier ON tableau
	b.acquired_subscription_value::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_acquired_subscription_value,
	--cancelled subs value
	b.cancelled_subscription_value,
	round(bb.cancelled_subscription_value,0) AS cancelled_subscription_value_current_month, --TO make it easier ON tableau
	b.cancelled_subscription_value::float / date_part('day',d.reporting_date) * t.number_days_in_a_month AS running_rate_cancelled_subscription_value,
	-- asv
	b.active_subscription_value,
	round(bb.active_subscription_value,0) AS active_subscription_value_current_month, --TO make it easier ON tableau
 	CASE WHEN d.reporting_date = last_day(d.reporting_date) THEN b.active_subscription_value 
 		ELSE b.previous_month_active_subscription_value + running_rate_acquired_subscription_value - running_rate_cancelled_subscription_value END AS running_rate_active_subscription_value
FROM dimensions_combined d
LEFT JOIN total_days_in_month t 
	ON t.month_ = date_trunc('month',d.reporting_date)
LEFT JOIN final_for_metrics_with_mkt_source_and_medium b
	ON b.reporting_date = d.reporting_date
	AND b.country = d.country
	AND b.channel = d.channel
	AND b.marketing_channel_detailed = d.marketing_channel_detailed
LEFT JOIN final_for_metrics_with_mkt_source_and_medium bb
	ON bb.country = d.country
	AND bb.channel = d.channel
	AND bb.is_last_month IS TRUE
	AND bb.marketing_channel_detailed = d.marketing_channel_detailed
LEFT JOIN final_for_metrics_without_mkt_source_and_medium a
	ON a.reporting_date = d.reporting_date
	AND a.country = d.country
	AND a.channel = d.channel
	AND a.marketing_channel_detailed = d.marketing_channel_detailed
LEFT JOIN final_for_metrics_without_mkt_source_and_medium aa
	ON aa.country = d.country
	AND aa.channel = d.channel
	AND aa.is_last_month IS TRUE
	AND aa.marketing_channel_detailed = d.marketing_channel_detailed
LEFT JOIN is_paid ip
	ON ip.marketing_channel = d.channel
WHERE 
		(
		COALESCE(a.budget_full_month,0) <> 0 OR
		COALESCE(aa.budget_full_month, 0) <> 0 OR
		COALESCE(a.cash_cost, 0) <> 0 OR
		COALESCE(aa.cash_cost,0) <> 0 OR
		COALESCE(a.non_cash_cost,0) <> 0 OR
		COALESCE(aa.non_cash_cost,0) <> 0 OR
		COALESCE(a.cost, 0) <> 0 OR
		COALESCE(aa.cost, 0) <> 0 OR
		COALESCE(b.paid_orders,0) <> 0 OR
		COALESCE(bb.paid_orders,0) <> 0 OR
		COALESCE(b.submitted_orders,0) <> 0 OR
		COALESCE(bb.submitted_orders,0) <> 0 OR
		COALESCE(b.new_customers,0) <> 0 OR
		COALESCE(bb.new_customers,0) <> 0 OR
		COALESCE(b.acquired_subscription_value, 0) <> 0 OR
		COALESCE(bb.acquired_subscription_value, 0) <> 0 OR
		COALESCE(b.cancelled_subscription_value, 0) <> 0 OR
		COALESCE(bb.cancelled_subscription_value, 0) <> 0 OR
		COALESCE(b.active_subscription_value, 0) <> 0 OR 
		COALESCE(bb.active_subscription_value, 0) <> 0 OR
		COALESCE(b.previous_month_active_subscription_value, 0) <> 0 
		)			
WITH NO SCHEMA BINDING	  
;

GRANT SELECT ON dm_marketing.v_marketing_runrates TO tableau;
