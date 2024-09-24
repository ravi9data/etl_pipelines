CREATE temp table temp_web_session_events_lists (like web.session_events_lists); 


INSERT INTO temp_web_session_events_lists
SELECT
	trim(p.session_id) AS session_id,
	listagg(DISTINCT CASE
		WHEN se_category = 'search'
		AND se_action IN ('product_click', 'cta_click') THEN p.product_sku
		ELSE 'n/a'
	END,
	', ' ) search_click_skus,
	listagg(DISTINCT CASE
		WHEN se_category = 'search'
		AND se_action IN ('product_click', 'cta_click') THEN p.subcategory_name
		ELSE 'n/a'
	END,
	', ' ) search_click_subcategories,
	listagg(DISTINCT CASE
		WHEN se_category = 'checkout' THEN p.current_flow
		ELSE NULL
	END,
	', ' ) checkout_flow
FROM
	web.event_properties p
	INNER JOIN (SELECT DISTINCT session_id FROM staging.event_properties_dl) e ON p.session_id = e.session_id 
GROUP BY
	1 ;
	

begin transaction;

delete from web.session_events_lists
using temp_web_session_events_lists s
where session_events_lists.session_id = s.session_id; 

INSERT INTO web.session_events_lists 
SELECT * FROM temp_web_session_events_lists;

end transaction;


drop table if exists web.session_events_features;
create table web.session_events_features as
SELECT
trim(domain_sessionid) as session_id,
sum(case 
 when e.se_action = 'tooltip' 
  and e.se_label = 'mietKauf' 
  then 1 
    else  0 
  end) as mietkauf_tooltip_events,
sum(case 
 	when e.se_category = 'internal_test' 
  	and e.se_action = 'id_check' 
  	and e.se_label = 'active'
   then 1 
    else  0 
   end) as failed_delivery_tracking_events,
   sum(case 
	when e.se_category ='search'
	and e.se_action in ('enter')
	then 1 
	else 0 end
	) search_enter,
sum(case 
	when e.se_category ='search'
	and e.se_action in ('product_click','cta_click')
	then 1 
	else 0 end
	) search_click,
sum(case 
	when e.se_category ='search'
	and e.se_action='product_click'
	then 1 
	else 0 end
	) search_product_click,	
sum(case 
	when e.se_category ='search'
	and e.se_action='cta_click'
	then 1 
	else 0 end
	) search_cta_click,
sum(case 
	when e.se_category ='search'
	and e.se_action='exit'
	then 1 
	else 0 end
	) search_exit,
sum(case 
	when e.se_category ='checkout'
	and e.se_action='availability_service'
	and se_label = 'enter'
	then 1 
	else 0 end
	) availability_service,
min(case 
	when e.se_category ='search'
	and e.se_action='enter'
	then e.collector_tstamp 
	else null end
	) as min_search_enter_timestamp
FROM scratch.se_events e 
where e.se_category <> ('utilityEvent')
group by 1;