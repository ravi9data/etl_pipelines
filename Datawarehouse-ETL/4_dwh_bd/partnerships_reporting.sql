--fetching the order ids for the partnership_reporting query and for the join on Tableau

drop table if exists dwh.sim_add_on_order_ids;
create table dwh.sim_add_on_order_ids as 
select 
		distinct 
		pv.page_view_date as reporting_date,
		pv.session_id,
		'sim_add_on_q2' as partner_name,
		lower(COALESCE(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(se.se_property, 'productData'), 'sub_category'), 
			JSON_EXTRACT_PATH_text(te.properties, 'sub_category') )) AS subcategory,
		o.store_label, 
		o.store_name,
		COALESCE(JSON_EXTRACT_PATH_text(se.se_property, 'orderID'), te.order_id) as order_id_,
		oi.subcategory_name,
		o.created_date,
		o.submitted_date,
		o.paid_date,
		st.country_name
	from traffic.page_views pv
	left join scratch.se_events se --snowplow data
		on se.domain_sessionid  = pv.session_id
		AND collector_tstamp < '2023-05-01'
	LEFT JOIN segment.track_events te  --segment DATA
		ON te.session_id = pv.session_id
		AND te.event_time >= '2023-05-01'
		AND te.order_id IS NOT NULL
	LEFT JOIN ods_production.store st 
		ON st.id = pv.store_id 
	left join master.order o
		on COALESCE(JSON_EXTRACT_PATH_text(se.se_property, 'orderID'), te.order_id) = o.order_id 
	left join ods_production.order_item oi 
		ON  oi.order_id = COALESCE(JSON_EXTRACT_PATH_text(se.se_property, 'orderID'), te.order_id)
		and oi.subcategory_name in ('Smartphones', 'Tablets', 'Apple-watches', 'Smartwatches')
	left join stg_external_apis.partnerships_vouchers v
		on o.voucher_code ilike ( case when partnership_name ='N26 Spain' then '%'+voucher_prefix_code+'%' else voucher_prefix_code+'%' end)
	where pv.page_urlpath ilike '%mobilcom-debitel%'
			AND (se.se_action ilike '%addtocart%' OR te.event_name IN ('Product Added','Product Added to Cart'))
			and o.created_date is not null 
			and lower(subcategory) in ('smartphones', 'tablets', 'apple-watches', 'smartwatches')
			and o.customer_type != 'business_customer'
			and partnership_name is null 
			and oi.subcategory_name is not NULL		
			AND order_id_ IS NOT NULL
order by 1 DESC;



drop table if exists dwh.partnership_reporting;
create table dwh.partnership_reporting as 
--fetching the order ids for the partnership_reporting query and for the join on Tableau
WITH sim_agg AS (
	SELECT DISTINCT  
		created_date::DATE AS reporting_date,
		partner_name, 
		store_label, 
		store_name,
		'B2C' AS b2b_b2c,
		country_name,
		'sim_add_on' AS info_from,
		COUNT(DISTINCT session_id) AS sessions, 
		COUNT(DISTINCT order_id_) AS created_orders, 
		COUNT(DISTINCT CASE WHEN submitted_date IS NOT NULL THEN order_id_ END) AS submitted_orders,
		COUNT(DISTINCT CASE WHEN paid_date IS NOT NULL THEN order_id_ END) AS paid_orders
	FROM dwh.sim_add_on_order_ids
	GROUP BY 1, 2, 3, 4, 5, 6, 7
	ORDER BY 1 DESC
)
, sessions AS (
	SELECT DISTINCT 
		smm.session_start::date AS reporting_date,
		smm.store_label, 
		smm.store_name, 
		COALESCE(st.country_name, 'null') AS country_name,
		CASE WHEN smm.store_name ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END AS b2b_b2c,
		COALESCE(CASE WHEN smm.store_id = 619 
		        		THEN 'Mobilcom Debitel'
		        	WHEN smm.store_id = 631 
		        		THEN 'bob.at'
		    		END,
		    	CASE WHEN COALESCE(smm.marketing_source,'n/a') ='n/a' THEN COALESCE(mm.partnership_name_name, smm.marketing_campaign)
		    		ELSE COALESCE(mm.partnership_name_name,smm.marketing_source) END) AS partner_name,
		CASE WHEN smm.store_id IN (631, 619) THEN 'bob.at and Mobilcom Debitel'
			ELSE 'last click attribution' END AS info_from,
		COUNT(distinct smm.session_id) AS sessions
	FROM traffic.sessions smm 
	LEFT JOIN ods_production.store st 
		ON st.id = smm.store_id
	LEFT JOIN staging.partnership_names_mapping mm 
		ON mm.partnership_name_original = (CASE WHEN COALESCE(smm.marketing_source,'n/a') = 'n/a' THEN smm.marketing_campaign
	    		  									ELSE smm.marketing_source END)	
	   	AND mm.b2c_b2b = (CASE WHEN smm.store_name ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END)
	   	AND COALESCE(mm.country, 'null') = COALESCE(st.country_name, 'null')
	WHERE (smm.marketing_channel ='Partnerships' 
		OR smm.marketing_campaign = '2326786:student beans us (+other)')
		AND smm.marketing_source NOT IN ('medifox', 'vattenfall')
	GROUP BY 1, 2, 3, 4, 5, 6, 7 ORDER BY 1 DESC)
,orders AS (
	SELECT DISTINCT  
		o.created_date::date AS reporting_date,
		o.store_label,
		o.store_name, 
		CASE WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END AS b2b_b2c,
		o.store_country AS country_name,
		lower(COALESCE(pv.partnership_name,
					CASE 
						WHEN o.store_id = 619 THEN 'Mobilcom Debitel'
        				WHEN o.store_id = 631 THEN 'bob.at'
        				END,
        			CASE WHEN m.marketing_source = 'n/a' THEN COALESCE(mm.partnership_name_name, m.marketing_campaign)
        				 ELSE COALESCE(mm.partnership_name_name,m.marketing_source) END
        				 )) AS partner_name,
        CASE WHEN pv.partnership_name IS NOT NULL THEN 'voucher'
        	 WHEN o.store_id IN (631, 619) THEN 'bob.at and Mobilcom Debitel'
			 ELSE 'last click attribution' END AS info_from,
		COUNT(DISTINCT o.order_id) AS created_orders, 
		COUNT(DISTINCT CASE WHEN submitted_date::Date IS NOT NULL THEN o.order_id END ) AS submitted_orders,
		COUNT(DISTINCT CASE WHEN paid_date::Date IS NOT NULL THEN o.order_id  END ) AS paid_orders
	FROM master.order o
	LEFT JOIN stg_external_apis.partnerships_vouchers pv
		ON (CASE WHEN o.voucher_code LIKE 'BIDROOM%' THEN o.voucher_code+o.store_country
				ELSE o.voucher_code END)
		ILIKE 
		    (CASE WHEN pv.partnership_name = 'N26 Spain' THEN '%'+voucher_prefix_code+'%' 		
				WHEN pv.partnership_name = 'N26 Netherlands' THEN '%'+voucher_prefix_code+'%'
				WHEN pv.partnership_name  LIKE 'Bidroom%' THEN '%'+voucher_prefix_code+split_part(pv.partnership_name,' ',2)+'%'
		        ELSE voucher_prefix_code+'%' END)	
	LEFT JOIN ods_production.order_marketing_channel m
		ON o.order_id = m.order_id
	LEFT JOIN staging.partnership_names_mapping mm 
		ON mm.partnership_name_original = (CASE WHEN m.marketing_source = 'n/a' THEN m.marketing_campaign
	    		  									ELSE m.marketing_source END)	
	   	AND mm.b2c_b2b = (CASE WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END)
	   	AND COALESCE(mm.country, 'null') = COALESCE(o.store_country, 'null')
	WHERE ((partnership_name IS NOT NULL
	       AND o.voucher_code IS NOT NULL)
	       OR o.marketing_channel = 'Partnerships'
	       OR o.store_id IN (631, 619))
	       AND o.order_id NOT IN (SELECT DISTINCT order_id_ FROM dwh.sim_add_on_order_ids)
	GROUP BY 1, 2, 3, 4, 5, 6, 7
	ORDER BY 1 DESC
)
, first_join as(
	SELECT DISTINCT
		COALESCE(sa.reporting_date::date, s.reporting_date::date) AS reporting_date,
		COALESCE(sa.partner_name,s.partner_name)  AS partner_name, 
		COALESCE(sa.store_label, s.store_label) AS store_label,
		COALESCE(sa.store_name, s.store_name) AS store_name, 
		COALESCE(sa.country_name, s.country_name) AS country_name,
		COALESCE(sa.b2b_b2c, s.b2b_b2c) AS b2b_b2c,
		COALESCE(sa.info_from, s.info_from) AS info_from,
		COALESCE((CASE WHEN sa.partner_name = 'sim_add_on_q2' THEN sa.sessions ELSE s.sessions END), 0) AS sessions,
		(CASE WHEN sa.partner_name = 'sim_add_on_q2' THEN sa.created_orders ELSE 0 END) AS created_orders,
		(CASE WHEN sa.partner_name = 'sim_add_on_q2' THEN sa.submitted_orders ELSE 0 END) AS submitted_orders,
		(CASE WHEN sa.partner_name = 'sim_add_on_q2' THEN sa.paid_orders ELSE 0 END) AS paid_orders
	FROM sim_agg sa 
	FULL JOIN sessions s 
		ON s.reporting_date = sa.reporting_date
		AND s.partner_name = sa.partner_name
		AND s.store_name = sa.store_name
		AND s.store_label = sa.store_label
		AND s.country_name = sa.country_name
		AND s.b2b_b2c = sa.b2b_b2c
		AND s.info_from = sa.info_from
	ORDER BY 1 DESC
)
, second_join  AS (
	SELECT DISTINCT 
		COALESCE(c.reporting_date::date, o.reporting_date::date) AS reporting_date,
		COALESCE(c.partner_name, o.partner_name)  AS partnership_name,
		COALESCE(c.store_label, o.store_label) AS store_label,
		COALESCE(c.store_name, o.store_name) AS store_name,
		COALESCE(c.country_name, o.country_name, 'null') AS country_name,
		COALESCE(c.b2b_b2c, o.b2b_b2c) AS b2b_b2c,
		COALESCE(c.info_from, o.info_from) AS info_from,
		COALESCE(c.sessions,0) AS sessions,
		COALESCE((CASE WHEN c.partner_name = 'sim_add_on_q2' THEN c.created_orders ELSE o.created_orders END), 0) AS created_orders,
		COALESCE((CASE WHEN c.partner_name = 'sim_add_on_q2' THEN c.submitted_orders ELSE o.submitted_orders END), 0) AS submitted_orders,
		COALESCE((CASE WHEN c.partner_name = 'sim_add_on_q2' THEN c.paid_orders ELSE o.paid_orders END), 0) AS paid_orders
	FROM first_join c
	FULL JOIN orders o
		ON (o.reporting_date = c.reporting_date
			AND o.partner_name = c.partner_name
			AND o.store_name = c.store_name
			AND o.store_label = c.store_label
			AND o.country_name = c.country_name
			AND o.b2b_b2c = c.b2b_b2c
			AND o.info_from = c.info_from
			)
)
SELECT *
FROM second_join 
WHERE (sessions != 0 OR created_orders!= 0)	
;

GRANT SELECT ON dwh.partnership_reporting TO tableau;
