CREATE OR REPLACE VIEW dm_marketing.v_partnership_reporting AS
WITH orders_data AS (
	SELECT DISTINCT 
	    lower(coalesce(pv.partnership_name, 
		    (CASE 
		        WHEN o.store_id = 619 
		        	THEN 'Mobilcom Debitel'
		        WHEN o.store_id = 631 
		        	THEN 'bob.at'
		    	END), 
		    	(CASE WHEN m.marketing_source = 'n/a' THEN COALESCE(mm.partnership_name_name, m.marketing_campaign)
		    		  				ELSE COALESCE(mm.partnership_name_name,m.marketing_source) END)
		    	)) AS partnership_name,
		   lower(coalesce(pv.partnership_group,
		    	(CASE WHEN o.store_id = 619 
		        		THEN 'Mobilcom Debitel'
		        	WHEN o.store_id = 631 
		        		THEN 'bob.at'
		    		END),
		    		CASE WHEN m.marketing_source = 'n/a' THEN COALESCE(mm.partnership_group_name, m.marketing_campaign)
		    		  				ELSE COALESCE(mm.partnership_group_name,m.marketing_source) END
		    		)) AS partnership_group, 
			oi.order_item_id, 
			o.order_id, 
			o.customer_type,
			o.customer_id, 
		    o.new_recurring,
			oi.plan_duration,
			oi.variant_id,
			oi.variant_sku,
			oi.product_sku,
			oi.category_name,
			oi.subcategory_name,
			oi.product_name, 
			oi.brand,
			o.created_date,
			o.submitted_date,
			o.paid_date, 
			o.completed_orders,
			o.paid_orders,
			o.payment_method, 
			o.burgel_risk_category, 
			o.scoring_decision,
			o.scoring_reason, 
			o.voucher_code, 
			o.retention_group, 
			o.status, 
			o.store_short, 
			CASE 
				WHEN o.store_label ILIKE 'Grover - %' 
					THEN 'Grover online'
			    WHEN o.store_id = 619 
			    	THEN 'Mobilcom Debitel'
		        WHEN o.store_id = 631 
		        	THEN 'bob.at'
		        ELSE 'Retail'
		    END AS store_label,
			o.store_name, 
			s.subscription_id, 
			s.subscription_name,
			s.subscription_value,
			s.status AS subscription_status,
			CASE 
				WHEN pv.partnership_name IS NOT NULL
					THEN 'voucher'
				 WHEN o.marketing_channel = 'Partnerships'
				 	THEN 'last click attribution'
				 WHEN o.store_id IN (631, 619) THEN 'bob.at and Mobilcom Debitel'
			END AS order_FROM,
			CASE WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' 
				END AS b2c_b2b_partnership_name,
			o.store_country,
			0::int AS sessions
	FROM master.order o
	LEFT JOIN stg_external_apis.partnerships_vouchers pv 
		ON (CASE WHEN o.voucher_code LIKE 'BIDROOM%' THEN o.voucher_code+o.store_country
				 WHEN o.voucher_code LIKE 'MILITARY%' THEN 'MILITARY'
				 WHEN o.voucher_code ILIKE 'iam%'
                    	 AND o.voucher_code NOT ILIKE 'iamat%'
                    	 AND o.voucher_code NOT ILIKE 'iamde%' THEN 'iam-oldvoucher'
                 WHEN o.voucher_code ILIKE 'n26-%' --TO CONNECT an OLD n26 voucher
                         AND o.voucher_code NOT ILIKE 'n26-esp%'
                         AND o.voucher_code NOT ILIKE 'n26-nld%' THEN 'n26-oldvoucher'
				ELSE o.voucher_code END)
				ILIKE 
				    (CASE 
					    WHEN lower(pv.voucher_prefix_code) = 'n26-' THEN 'n26-oldvoucher'
					    WHEN pv.partnership_name = 'N26 Spain' THEN '%'+voucher_prefix_code+'%' 		
						WHEN pv.partnership_name = 'N26 Netherlands' THEN '%'+voucher_prefix_code+'%'
						WHEN lower(pv.voucher_prefix_code) = 'iam' THEN 'iam-oldvoucher'
						WHEN pv.partnership_name  LIKE 'Bidroom%' THEN '%'+voucher_prefix_code+split_part(pv.partnership_name,' ',2)+'%'
				        ELSE voucher_prefix_code+'%' END)
	LEFT JOIN ods_production.order_item oi 
		ON o.order_id = oi.order_id 
	LEFT JOIN ods_production.subscription s 
		ON o.order_id = s.order_id 
		AND oi.variant_sku = s.variant_sku 
	LEFT JOIN ods_production.order_marketing_channel m
		ON o.order_id = m.order_id
	LEFT JOIN staging.partnership_names_mapping mm 
		ON mm.partnership_name_original = (CASE WHEN m.marketing_source = 'n/a' THEN m.marketing_campaign
	    		  									ELSE m.marketing_source END)	
	   	AND mm.b2c_b2b = (CASE WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END)
	   	AND mm.country = o.store_country
	WHERE pv.partnership_name IS NOT NULL       --orders FROM voucher usage
	   OR o.marketing_channel = 'Partnerships'  --orders FROM LAST click attribution
	   OR o.store_id IN (631, 619)              --orders FROM bob.at ARE NOT FOLLOWING the above conditions	  	
)
, session_data AS (
	   SELECT
		   lower(COALESCE(CASE WHEN smm.store_id = 619 
			        		THEN 'Mobilcom Debitel'
			        	WHEN smm.store_id = 631 
			        		THEN 'bob.at'
			    		END,
			    	CASE WHEN COALESCE(smm.marketing_source,'n/a') ='n/a' 
			    	THEN COALESCE(mm.partnership_name_name, smm.marketing_campaign)
			    		ELSE COALESCE(mm.partnership_name_name,smm.marketing_source) END) 
			    		) AS partnership_name, 		
		    lower(COALESCE(CASE WHEN smm.store_id = 619 
			        		THEN 'Mobilcom Debitel'
			        	WHEN smm.store_id = 631 
			        		THEN 'bob.at'
			    		END,
			    	CASE WHEN COALESCE(smm.marketing_source,'n/a') ='n/a' 
			    	THEN COALESCE(mm.partnership_group_name, smm.marketing_campaign)
			    		ELSE COALESCE(mm.partnership_group_name,smm.marketing_source) END) 
			    		) AS partnership_group,
			NULL::int AS order_item_id, 
			NULL::varchar AS order_id, 
			NULL::varchar AS customer_type,
			NULL::int AS customer_id, 
		    CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS new_recurring,
			NULL::int AS plan_duration,
			NULL::int AS variant_id,
			NULL::varchar AS variant_sku,
			NULL::varchar AS product_sku,
			NULL::varchar AS category_name,
			NULL::varchar AS subcategory_name,
			NULL::varchar AS product_name, 
			NULL::varchar AS brand,
			smm.session_start::date AS created_date,
			NULL::timestamp AS submitted_date,
			NULL::timestamp AS paid_date, 
			NULL::int AS completed_orders,
			NULL::int AS paid_orders,
			NULL::varchar AS payment_method, 
			NULL::varchar AS burgel_risk_category, 
			NULL::varchar AS scoring_decision,
			NULL::varchar AS scoring_reason, 
			NULL::varchar AS voucher_code, 
			NULL::varchar AS retention_group, 
			NULL::varchar AS status, 
			st.store_short, 
			smm.store_label,
			smm.store_name,
			NULL::varchar AS subscription_id, 
			NULL::varchar AS subscription_name,
			NULL::float AS subscription_value,
			NULL::varchar AS subscription_status,
			CASE WHEN smm.store_id IN (631, 619) THEN 'bob.at and Mobilcom Debitel'
				ELSE 'last click attribution' END AS order_FROM,
			CASE WHEN smm.store_name ILIKE '%b2b%' 
				THEN 'B2B' ELSE 'B2C' END AS b2c_b2b_partnership_name,
			COALESCE(st.country_name, 'null') AS store_country,
			COUNT(distinct smm.session_id) AS sessions
	FROM traffic.sessions smm 
	LEFT JOIN ods_production.store st 
		ON st.id = smm.store_id
	LEFT JOIN staging.partnership_names_mapping mm 
		ON mm.partnership_name_original = (CASE WHEN COALESCE(smm.marketing_source,'n/a') = 'n/a' 
		THEN smm.marketing_campaign
	    		  									ELSE smm.marketing_source END)	
	   	AND mm.b2c_b2b = (CASE WHEN smm.store_name ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END)
	   	AND COALESCE(mm.country, 'null') = COALESCE(st.country_name, 'null')
	LEFT JOIN traffic.snowplow_user_mapping cu
	    ON smm.anonymous_id = cu.anonymous_id 
	    AND smm.session_id = cu.session_id
	WHERE (smm.marketing_channel ='Partnerships' 
		OR smm.marketing_campaign = '2326786:student beans us (+other)')
		AND smm.marketing_source NOT IN ('medifox', 'vattenfall')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
)
SELECT * FROM orders_data
UNION ALL
SELECT * FROM session_data
WITH NO SCHEMA BINDING;