DROP TABLE IF EXISTS tmp_campaign_names;
CREATE TEMP TABLE tmp_campaign_names
SORTKEY(campaign_id)
DISTKEY(campaign_id)
AS
SELECT DISTINCT 
	campaign_id, 
	max(campaign_name) AS campaign_name
FROM dm_marketing.braze_campaign_mapping
GROUP BY 1
;



DROP TABLE IF EXISTS tmp_message_names;
CREATE TEMP TABLE tmp_message_names
SORTKEY(message_variation_id)
DISTKEY(message_variation_id)
AS
SELECT DISTINCT 
	message_variation_id, 
	max(message_variation_name) AS message_variation_name
FROM dm_marketing.braze_campaign_mapping
GROUP BY 1
;


BEGIN TRANSACTION;

DELETE FROM dm_marketing.braze_campaign_events_all
WHERE braze_campaign_events_all.sent_date::date >= DATEADD('day', -15, current_date);

INSERT INTO dm_marketing.braze_campaign_events_all
SELECT 
	cn.campaign_name,
	mn.message_variation_name,
	b.campaign_id, 
	b.message_variation_id, 
	b.customer_id, 
	b.source_type, 
	b.os, 
	b.os_version, 
	b.awareness_date, 
	b.awareness_date_inf, 
	b.sent_date, 
	b.first_time_opened, 
	b.first_time_clicked, 
	b.is_sent, 
	b.is_sent_overall, 
	b.is_delivered, 
	b.is_delivered_overall, 
	b.is_opened_overall, 
	b.is_opened, 
	b.is_clicked_overall, 
	b.is_clicked, 
	b.is_unsubscribed, 
	b.is_unsubscribed_overall, 
	b.is_bounced_overall, 
	b.is_bounced, 
	b.seconds_til_first_open, 
	b.seconds_til_first_click, 
	-- Order details
	b.order_id, 
	b.created_date, 
	b.submitted_date, 
	b.approved_order, 
	b.paid_order,
	b.converted_order, 
	b.influenced_order,
	-- Order enriching
	b.marketing_channel, 
	b.device_used_to_order, 
	b.store_country,  
	b.category_name, 
	b.product_name, 
	b.total_committed_subs_value_eur, 
	b.new_recurring, -- NEW mapped version
	b.order_time_lag_days, 
	b.order_time_lag_hours, 
	b.order_time_lag_days_influenced, 
	-- Customer details
	b.customer_type, 
	b.customer_age, 
	b.rfm_segment, 
	b.crm_label, 
	b.crm_label_braze,
	b.customer_acq_cohort, 
	b.signup_country, 
	b.afl_next_payment_date,
	b.id_campaign_hash
FROM dm_marketing.braze_order_attribution b
LEFT JOIN tmp_message_names mn
	ON mn.message_variation_id = b.message_variation_id
LEFT JOIN tmp_campaign_names cn 
	ON cn.campaign_id = b.campaign_id
WHERE b.campaign_id IS NOT NULL 
	AND b.sent_date::date >= DATEADD('day', -15, current_date);

END TRANSACTION;

