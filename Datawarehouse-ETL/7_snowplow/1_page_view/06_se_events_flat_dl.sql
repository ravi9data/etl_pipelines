drop table if exists se_events_flat_dl;

create temp table se_events_flat_dl as
WITH events as (
SELECT
	*
FROM
	atomic.events e
WHERE
	se_action NOT IN ('heartbeat')
	AND IS_VALID_JSON(se_property)
	AND se_action IS NOT NULL 
	and collector_tstamp::date > DATEADD(day, -3, CURRENT_DATE)
	and useragent not like '%Datadog%'		/*exclude events created by Datadog testing*/
	)
select 
	event_id,
	collector_tstamp,
	platform,
	domain_userid,
	COALESCE(NULLIF(json_extract_path_text(se_property,'userID'),''),NULLIF(json_extract_path_text(se_property,'userId'),''),user_id) AS user_id,
	se_action,
	se_category,
	se_label,
	se_property,
	se_value,
	domain_sessionid,
	s.is_qa_url,
	CASE
		WHEN dvce_type IN ('Computer', 'Mobile') THEN dvce_type
		ELSE 'Other'
	END AS dvce_type,
	CASE
		WHEN os_family IN ('Windows', 'Android', 'iOS', 'Mac OS X', 'Linux') THEN os_family
		ELSE 'Others'
	END AS os_family,
	CASE
		WHEN br_family IN ('Chrome', 'Safari', 'Firefox', 'Microsoft Edge', 'Opera', 'Apple WebKit', 'Internet Explorer') THEN br_family
		ELSE 'Others'
	END AS br_family,
	nullif(json_extract_path_text(se_property, 'current_flow'),'')::varchar(666) AS se_current_flow,
	COALESCE(NULLIF(json_extract_path_text(se_property, 'orderId'), ''), 
		 NULLIF(json_extract_path_text(se_property, 'orderID'), ''),
		 NULLIF(json_extract_path_text(se_property, 'order_id'), '')) AS order_id,
	COALESCE(NULLIF(json_extract_path_text(se_property, 'productData', 'productSKU'), ''),
		 NULLIF(json_extract_path_text(se_property, 'product_sku'), ''),
		 NULLIF(json_extract_path_text(se_property, 'productData', 'productSku'), '')) AS product_sku,
	nullif(json_extract_path_text(se_property,'productData','productVariant'),'') product_variant,
	nullif(json_extract_path_text(se_property,'productData','interacting_with_mix'),'') is_mix,
	nullif(json_extract_path_text(se_property,'productData','sub_category'),'') sub_category,
	nullif(json_extract_path_text(se_property ,'productData','price'),'') order_value,
	nullif(json_extract_path_text(se_property ,'price'),'') order_value2,
	nullif(json_extract_path_text(se_property,'flags','recommendation_engine'),'') recommendation_engine,
	nullif(json_extract_path_text(se_property,'list'),'') product_click,
	nullif(json_extract_path_text(se_property,'position'),'') pc_position,
	nullif(json_extract_path_text(se_property,'tracing_id'),'') tracing_id,
	e.geo_country,
	nullif(json_extract_path_text(se_property,'store'),'') as store
FROM
	events e
LEFT JOIN web.sessions_snowplow s ON
	e.domain_sessionid = s.session_id;
	

update se_events_flat_dl
set sub_category  = p.subcategory_name  
from ods_production.product p 
WHERE p.product_sku = se_events_flat_dl.product_sku 
	AND se_events_flat_dl.sub_category is null
	AND se_events_flat_dl.product_sku is not null;


begin transaction;

delete from scratch.se_events_flat
using se_events_flat_dl b
where se_events_flat.event_id = b.event_id;

insert into scratch.se_events_flat 
select distinct * from se_events_flat_dl;

end transaction;

drop table if exists se_events_flat_dl;
