-- dm_commercial.v_partnership_asv source

CREATE OR REPLACE VIEW dm_commercial.v_partnership_asv AS
WITH fact_days AS 
(
	SELECT DISTINCT 
		datum AS fact_day,
		day_is_last_of_month, 
	CASE	
		WHEN week_day_number = 7 THEN 1
		WHEN datum = current_date THEN 1
		ELSE 0 END AS day_is_end_of_week,
	CASE 
		WHEN datum = current_date THEN 1
		ELSE day_is_last_of_month 
	END AS day_is_end_of_month
	FROM public.dim_dates
	WHERE datum <= CURRENT_DATE	
)
, partnership_orders AS ( 
	SELECT
		o.created_date::date AS fact_day,
		o.store_id,
		o.store_country,
		o.store_label,
		o.store_name, 
		o.order_id,
		o.voucher_code,
	lower(coalesce(pv.partnership_name, 
		    (CASE 
		        WHEN sim.order_id_ IS NOT NULL 
		        	THEN 'sim_add_on'
		        WHEN o.store_id = 619 
		        	THEN 'Mobilcom Debitel'
		        WHEN o.store_id = 631 
		        	THEN 'bob.at'
		    	END), 
		    	(CASE WHEN m.marketing_source = 'n/a' THEN COALESCE(mm.partnership_name_name, m.marketing_campaign)
		    		  				ELSE COALESCE(mm.partnership_name_name,m.marketing_source) END)
		    	)) AS partnership_name_,
		   lower(coalesce(pv.partnership_group,
		    	(CASE WHEN  sim.order_id_ IS NOT NULL 
		        		THEN 'sim_add_on'
		        	WHEN o.store_id = 619 
		        		THEN 'Mobilcom Debitel'
		        	WHEN o.store_id = 631 
		        		THEN 'bob.at'
		    		END),
		    		CASE WHEN m.marketing_source = 'n/a' THEN COALESCE(mm.partnership_group_name, m.marketing_campaign)
		    		  				ELSE COALESCE(mm.partnership_group_name,m.marketing_source) END
		    		)) AS partnership_group
	FROM master.order o  
	LEFT JOIN stg_external_apis.partnerships_vouchers pv
	 ON (CASE WHEN o.voucher_code LIKE 'BIDROOM%' THEN o.voucher_code+o.store_country
				ELSE o.voucher_code END)
		ILIKE 
		    (CASE WHEN pv.partnership_name = 'N26 Spain' THEN '%'+voucher_prefix_code+'%' 		
				WHEN pv.partnership_name = 'N26 Netherlands' THEN '%'+voucher_prefix_code+'%'
				WHEN pv.partnership_name  LIKE 'Bidroom%' THEN '%'+voucher_prefix_code+split_part(pv.partnership_name,' ',2)+'%'
		        ELSE voucher_prefix_code+'%' END)
	LEFT JOIN dwh.sim_add_on_order_ids sim
		ON o.order_id = sim.order_id_
	LEFT JOIN ods_production.order_marketing_channel m
	ON o.order_id = m.order_id
	LEFT JOIN staging.partnership_names_mapping mm 
		ON mm.partnership_name_original = (CASE WHEN m.marketing_source = 'n/a' THEN m.marketing_campaign
	    		  									ELSE m.marketing_source END)	
	   	AND mm.b2c_b2b = (CASE WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END)
	   	AND mm.country = o.store_country
	WHERE sim.order_id_ IS NOT NULL             --sim_add_on orders
	   OR pv.partnership_name IS NOT NULL       --orders FROM voucher usage
	   OR o.marketing_channel = 'Partnerships'  --orders FROM LAST click attribution
	   OR o.store_id IN (631, 619)              --orders FROM bob.at ARE NOT FOLLOWING the above conditions
	GROUP BY 1,2,3,4,5,6,7,8,9
)
,partnerships_asv AS ( 
	SELECT 
		f.fact_day,
		f.day_is_last_of_month,
		f.day_is_end_of_week,
		f.day_is_end_of_month,
		po.partnership_name_,
		po.partnership_group,
		sum(subscription_value_eur) AS active_subscription_value
	FROM fact_days f
	LEFT JOIN partnership_orders po 
	 ON f.fact_day >= po.fact_day
	INNER JOIN ods_production.subscription_phase_mapping spm 
	 ON spm.order_id = po.order_id
	  AND f.fact_day::date >= spm.fact_day::date
  	   AND f.fact_day::date < COALESCE(spm.end_date::date, f.fact_day::date + 1)
  	GROUP BY 1,2,3,4,5,6
)
, partnership_acquired_subs_value AS (
	SELECT 
		s.start_date::date AS fact_day,
		po.partnership_name_,
		po.partnership_group,
		sum(subscription_value) AS acquired_subscription_value
	FROM partnership_orders po 
	LEFT JOIN ods_production.subscription s 
	 ON po.order_id = s.order_id
	WHERE s.start_date::date IS NOT NULL
	GROUP BY 1,2,3
)
, partnership_cancelled_subs_value AS (
	SELECT 
		s.cancellation_date::date AS fact_day,
		po.partnership_name_,
		po.partnership_group,
		sum(subscription_value) AS cancelled_subscription_value
	FROM partnership_orders po 
	LEFT JOIN ods_production.subscription s 
	 ON po.order_id = s.order_id
	WHERE s.cancellation_date::date IS NOT NULL
	GROUP BY 1,2,3
)
SELECT
	p.fact_day,
	day_is_last_of_month,
	day_is_end_of_week,
	day_is_end_of_month,
	p.partnership_name_,
	p.partnership_group,
	sum(active_subscription_value) AS active_subscription_value,
	sum(acquired_subscription_value) AS acquired_subscription_value,
	sum(cancelled_subscription_value) AS cancelled_subscription_value
FROM partnerships_asv p
LEFT JOIN partnership_acquired_subs_value sv 
 ON sv.fact_day = p.fact_day
  AND sv.partnership_name_ = p.partnership_name_
LEFT JOIN partnership_cancelled_subs_value cv
 ON cv.fact_day = p.fact_day
  AND cv.partnership_name_ = p.partnership_name_
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING
;