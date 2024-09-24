DROP VIEW dm_risk.v_recommendation_engine_widget_info;
CREATE OR REPLACE VIEW dm_risk.v_recommendation_engine_widget_info AS
SELECT DISTINCT
	w.pdp_event_timestamp,
	w.pdp_event_id ,
	w.pdp_subcategory,
	w.pdp_product_sku,
	p2.product_name AS pdp_product_name,
	w.recom_event_id,
	--w.recom_event_timestamp,
	COALESCE(w.recom_subcategory, p.subcategory_name) AS recom_subcategory,
	w.recom_product_sku,
	p.product_name AS recom_product_name,
	w.is_recommended_by_model,
	w.widget_position,
	w.session_id,
	w.snowplow_user_id,
	w.customer_id,
	w.order_id,
	w.is_recomm_order_paid,
	w.diff_in_sec_until_click
FROM dm_risk.recommendation_engine_widget_info w
LEFT JOIN ods_production.product p 
  ON p.product_sku = w.recom_product_sku 
LEFT JOIN ods_production.product p2
  ON p2.product_sku = w.pdp_product_sku 
WITH NO SCHEMA BINDING; 