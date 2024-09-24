truncate table recommendation.rec_sys_remove_from_cart;

insert into recommendation.rec_sys_remove_from_cart
SELECT
	collector_tstamp,
	domain_userid,
	b.customer_id AS user_id ,
	product_sku AS remove_from_cart,
	EXTRACT(quarter FROM collector_tstamp) AS quarter ,
	EXTRACT(YEAR FROM collector_tstamp) AS YEAR
FROM scratch.se_events_flat se
LEFT OUTER JOIN scratch.user_encoded_ids b
	ON se.user_id = b.encoded_id
WHERE se_action = 'removeFromCart'
  AND collector_tstamp::date >= '2020-01-01';

truncate table recommendation.rec_sys_add_to_cart;

insert into recommendation.rec_sys_add_to_cart
SELECT
	collector_tstamp,
	domain_userid,
	b.customer_id AS user_id ,
	product_sku AS add_to_cart,
	EXTRACT(quarter	FROM collector_tstamp) AS quarter ,
	EXTRACT(YEAR FROM collector_tstamp) AS YEAR
FROM scratch.se_events_flat se
LEFT OUTER JOIN scratch.user_encoded_ids b
	ON se.user_id = b.encoded_id
WHERE se_action = 'addToCart'
  AND collector_tstamp::date >= '2020-01-01';

truncate table recommendation.rec_sys_id_mapping;

insert into recommendation.rec_sys_id_mapping
SELECT
	user_id,
	domain_userid
FROM scratch.se_events_flat se
WHERE user_id IS NOT NULL
  AND collector_tstamp::date >= '2020-01-01';

truncate table recommendation.rec_sys_submit_order;

insert into recommendation.rec_sys_submit_order
SELECT
	o.customer_id,
	o.order_id,
	o.status,
	submitted_date,
	oi.product_sku,
	oi.category_name,
	oi.subcategory_name ,
	oi.brand
FROM master.order o
JOIN ods_production.order_item oi
	ON o.order_id = oi.order_id
JOIN master.customer c
	ON o.customer_id = c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping b
	ON b.company_type_name = c.company_type_name
WHERE submitted_date IS NOT NULL
  AND (c.customer_type = 'normal_customer'
       OR (c.customer_type = 'business_customer'
		   AND b.is_freelancer = 1))
  AND o.status NOT IN ('CART', 'ADDRESS');
