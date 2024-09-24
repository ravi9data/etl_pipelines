DROP TABLE IF EXISTS stg_kafka_order_item;
CREATE TEMP TABLE stg_kafka_order_item as
with numbers as
	(
  	select 
		* 
	from public.numbers
  	where ordinal < 20
	)
	,order_items_all as 
	(
	select
		DISTINCT JSON_EXTRACT_PATH_text(payload, 'order_number') AS order_number,
		JSON_EXTRACT_PATH_text(payload, 'line_items') AS line_items, -- Note we ARE ONLY extracting this AS it IS used IN other columns.
		json_extract_array_element_text(line_items,numbers.ordinal::int, true) as line_items_json,
		JSON_EXTRACT_PATH_text(line_items_json,'line_item_id') order_item_id,
		JSON_EXTRACT_PATH_text(payload, 'order_mode') AS order_mode,
		case when order_mode  = 'MIX' and order_item_id = 0  then 1 else JSON_ARRAY_LENGTH(line_items,true) end as total_order_items, 
		-- Note 1: No need to cross join the old mix now as we will do in the next cte
		-- Note 2: Keep this case when on a line below the order_mode extraction line.  
		to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as event_timestamp_adj,
		JSON_EXTRACT_PATH_text(line_items_json,'variants') variants,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'total'), 'in_cents') AS total_in_cents, -- NOTE WE ARE TAKING THE TOTAL, NOT NET TOTAL
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'total'), 'currency') AS total_currency,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'variant'),'variant_id') variant_id,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'variant'),'product_sku') product_sku
	from stg_kafka_events_full.stream_internal_order_placed_v2 op
	cross join numbers 
	where numbers.ordinal < total_order_items
	--and  order_number in ('M0298944577', 'F9135210278' , 'M4669994727')
	)
	,order_items_mix_old as -- old mix order in 2020 that doesn't follow the same path and needs to be fetched from vairants
	(
	select 
		order_number,
		JSON_ARRAY_LENGTH(JSON_EXTRACT_PATH_text(line_items_json,'variants'),true) total_order_items_old_mix,
		json_extract_array_element_text(variants,numbers.ordinal::int, true) as variant,
		numbers.ordinal ,
		JSON_EXTRACT_PATH_text(variant,'variant_id') variant_id,
		JSON_EXTRACT_PATH_text(variant,'product_sku') product_sku
	from order_items_all
	cross join numbers where numbers.ordinal < total_order_items_old_mix
	and (order_item_id = 0 and order_mode = 'MIX')
	)
	,order_items as
	(
	select 
		oi.order_number,
		oi.line_items_json,
		oi.order_item_id,
		oi.total_in_cents,
		oi.order_mode,
		oi.total_currency,
		oi.event_timestamp_adj,
		case when oi.order_item_id = 0  AND event_timestamp_adj < to_timestamp('2023-01-01', 'yyyy-mm-dd') then oimw.total_order_items_old_mix else oi.total_order_items end as total_order_items,
		case when oi.order_item_id = 0  AND event_timestamp_adj < to_timestamp('2023-01-01', 'yyyy-mm-dd') then oimw.variant_id else oi.variant_id end as variant_id,
		case when oi.order_item_id = 0  AND event_timestamp_adj < to_timestamp('2023-01-01', 'yyyy-mm-dd') then oimw.product_sku else oi.product_sku end as product_sku
	from order_items_all oi
	left join order_items_mix_old oimw
	on oi.order_number = oimw.order_number
	)

select
	oi.order_item_id::INTEGER,
	NULL as order_item_sf_id,
	oi.order_number as order_id,
	oi.variant_id::INTEGER,
	NULL::int as rental_plan_id,
	event_timestamp_adj as created_at,
	event_timestamp_adj as updated_at,
	JSON_EXTRACT_PATH_text(line_items_json,'quantity')::int as quantity,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_price'),'in_cents')/100.0 price,
	case when order_mode = 'FLEX' then (JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_total'),'in_cents')/100.0) else price end as total_price,--CHECK	
	nullif(JSON_EXTRACT_PATH_text(line_items_json,'committed_months'),'')::INTEGER as plan_duration,
	oi.total_currency as currency,
	NULL::int as trial_days,
	False as any_variant,
	variant_sku,
	oi.product_sku,
	product_name,
	category_name,
	subcategory_name,
	brand,
	NULL::bigint as pricing_campaign_id,
	risk_label
from order_items oi
left join ods_production.variant v
on v.variant_id = oi.variant_id
left join ods_production.product p
on v.product_id=p.product_id
where order_mode != 'SWAP';

DROP TABLE IF EXISTS ods_order_item_legacy ;
CREATE TEMP TABLE ods_order_item_legacy as
select
	i.id as order_item_id,
	sf.id as order_item_sf_id,
	o."number" as order_id,
	i.variant_id,
	i.rental_plan_id,
	i.created_at,
	i.updated_at,
	i.quantity,
	i.price,
	i.price*i.quantity as total_price,
	case when COALESCE(i.minimum_term_months,0)=0 then 1 else i.minimum_term_months end as plan_duration,
	i.currency,
	i.trial_days,
	i.any_variant,
	vm.variant_sku,
	p.product_sku,
	p.product_name,
	p.category_name,
	p.subcategory_name,
	p.brand
	,i.campaign_id as pricing_campaign_id,
	p.risk_label
-- ,case when sf.id is null then 'removed' else 'kept' end as remove_from_ods,
from stg_api_production.spree_line_items i
inner join stg_api_production.spree_orders o
 on i.order_id=o.id
left join stg_salesforce."order" osf
 on osf.spree_order_number__c=o."number"
left join stg_salesforce.orderitem sf
 on i.id=sf.spree_order_line_id__c
-- MAPPING VARIANT_ID
left join ods_production.variantid_sku_historical_mapping vm
on vm.variant_id = i.variant_id
-- PRODUCT AND VARIANT from ODS
left join ods_production.variant v
 on v.variant_sku=vm.variant_sku
left join ods_production.product p
 on v.product_id=p.product_id
where true and o.number<>'R384074650'
-- Adding condition to remove carts that do not exist in sf (provided that order exists in sf
and  (case
  when osf.spree_order_number__c is not null  --when order exists in sf
   and sf.id is null -- but order item doesn't exist in sf
 then false else true end);
--order by created_at desc


DROP TABLE IF EXISTS order_item_cart_orders ;
CREATE TEMP TABLE order_item_cart_orders AS
SELECT
	((RAND() * 100000000)::INT + o.variant_id::INT)::INT AS order_item_id,
	NULL::varchar AS order_item_sf_id,
	o.order_id AS order_id,
	o.variant_id::int,
	NULL::int AS rental_plan_id,
	to_timestamp (o.created_date, 'yyyy-mm-dd HH24:MI:SS') AS created_at,
	to_timestamp (o.updated_date, 'yyyy-mm-dd HH24:MI:SS') AS updated_at,
	o.quantity::INT,
	o.price,
	o.price::float * o.quantity::float as total_price,
	CASE WHEN COALESCE(o.committed_months::int,0) = 0 THEN 1 ELSE o.committed_months::int END AS plan_duration,
	o.currency,
	NULL::int AS trial_days,
	FALSE AS any_variant,
	COALESCE(o.variant_sku, v.variant_sku) AS variant_sku,
	p.product_sku,
	p.product_name,
	p.category_name,
	p.subcategory_name,
	p.brand,
	NULL::bigint AS pricing_campaign_id,
	NULL::varchar AS risk_label
FROM stg_curated.checkout_eu_us_cart_orders_updated_v1 o
LEFT JOIN ods_production.variant v
	ON v.variant_sku = o.variant_sku
LEFT JOIN ods_production.product p
	ON v.product_id = p.product_id
LEFT JOIN ods_order_item_legacy oil 
	ON o.order_id = oil.order_id
LEFT JOIN stg_kafka_order_item koi 
	ON o.order_id = koi.order_id
WHERE oil.order_id IS NULL AND koi.order_id IS NULL -- IN ORDER TO DELETE the orders we already have
;


DROP TABLE IF EXISTS tmp_variant_sku_rrp_metrics;
CREATE TEMP TABLE tmp_variant_sku_rrp_metrics
AS (
WITH dates AS (
SELECT
	datum AS fact_date
FROM
	public.dim_dates
WHERE
	datum <= current_date
ORDER BY
	1 DESC 
)
,
rrp AS (
SELECT 
	a.purchased__c::date AS purchase_date,
	a.f_product_sku_variant__c AS product_sku_variant,
	a.amount_rrp__c,
	avg(a.amount_rrp__c) OVER (PARTITION BY a.f_product_sku_variant__c ORDER BY	a.purchased__c ROWS UNBOUNDED PRECEDING) AS avg_rrp,
	max(a.amount_rrp__c) OVER (PARTITION BY a.f_product_sku_variant__c ORDER BY	a.purchased__c ROWS UNBOUNDED PRECEDING) AS max_rrp,
	min(a.amount_rrp__c) OVER (PARTITION BY a.f_product_sku_variant__c ORDER BY	a.purchased__c ROWS UNBOUNDED PRECEDING) AS min_rrp,
	ROW_NUMBER () OVER (PARTITION BY purchase_date,product_sku_variant ORDER BY a.purchased__c DESC ) AS rn
FROM
	stg_salesforce.asset a
),
rrp_final AS (
SELECT *,lag(amount_rrp__c,1) OVER ( PARTITION by product_sku_variant ORDER BY	purchase_date ) AS last_rrp
FROM rrp WHERE rn = 1
)
,
psv AS 
(
SELECT
	product_sku_variant,
	min(purchase_date::date) AS min_purchase_date
FROM
	rrp_final
GROUP BY
	1
)
,
_final AS (
SELECT
	   d.fact_date,
	   psv.product_sku_variant,
	   rrp.avg_rrp,
	   rrp.max_rrp,
  	 rrp.min_rrp,
		 rrp.last_rrp,
		 rrp.amount_rrp__c
		FROM
	dates d
CROSS JOIN psv
LEFT JOIN rrp_final AS rrp
	ON
	rrp.product_sku_variant = psv.product_sku_variant
	AND d.fact_date = rrp.purchase_date
WHERE
	d.fact_date >= psv.min_purchase_date
)
SELECT 
	fact_date,
	product_sku_variant,
	COALESCE(avg_rrp, LAST_VALUE(avg_rrp IGNORE NULLS) OVER (PARTITION BY product_sku_variant ORDER BY fact_date ROWS UNBOUNDED PRECEDING)) AS avg_rrp,
	COALESCE(max_rrp, LAST_VALUE(max_rrp IGNORE NULLS) OVER (PARTITION BY product_sku_variant ORDER BY fact_date ROWS UNBOUNDED PRECEDING)) AS max_rrp,
	COALESCE(min_rrp, LAST_VALUE(min_rrp IGNORE NULLS) OVER (PARTITION BY product_sku_variant ORDER BY fact_date ROWS UNBOUNDED PRECEDING)) AS min_rrp,
    COALESCE(last_rrp, LAST_VALUE(amount_rrp__c IGNORE NULLS) OVER (PARTITION BY product_sku_variant ORDER BY fact_date ROWS UNBOUNDED PRECEDING)) AS last_rrp
FROM
	_final
ORDER BY 1
);


DROP TABLE IF EXISTS order_item_final;
CREATE TEMP TABLE order_item_final
AS
WITH _final AS (
SELECT * FROM ods_order_item_legacy
UNION ALL
SELECT * FROM stg_kafka_order_item
UNION ALL 
SELECT * FROM order_item_cart_orders)
SELECT f.* ,asset_rrp.avg_rrp, asset_rrp.max_rrp,asset_rrp.min_rrp,asset_rrp.last_rrp 
FROM
	_final f
LEFT JOIN tmp_variant_sku_rrp_metrics asset_rrp
	ON
	asset_rrp.product_sku_variant = f.variant_sku
	AND asset_rrp.fact_date =cast(f.created_at as date);


delete from ods_production.order_item where 1=1;
insert into ods_production.order_item
select * from order_item_final;

grant select on  ods_production.order_item to group risk_users;


