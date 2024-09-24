CREATE OR REPLACE VIEW dm_marketing.v_crm_report AS
WITH customers_email_subscribe AS (
	SELECT 
		"date" AS reporting_date,
		customer_id,
		LAG("date") OVER (PARTITION BY customer_id ORDER BY "date") AS previous_day_was_subscribed
	FROM master.customer_historical ch 
	WHERE date >= DATEADD('day',-1,DATE_TRUNC('month',DATEADD('month',-12,current_date))) --this date should be one DAY BEFORE than the FILTER IN the NEXT subquery
		AND email_subscribe IN ('Unsubscribed','unsubscribed')
	ORDER BY 2, 1 
)
, canvas_tags AS (
	SELECT DISTINCT 
		canvas_id, 
		REPLACE(TRANSLATE(lower(tags), '[]', ''), '"', '')::VARCHAR(100) AS canvas_tags
	FROM stg_external_apis.braze_canvas_details bcd
	WHERE draft = 0 -- Removes draft Canvas versions which may have different campaign tags. 
)
, campaign_tags AS (
	SELECT DISTINCT 
		campaign_id, 
		REPLACE(TRANSLATE(tags, '[]', ''), '"', '')::VARCHAR(100) AS campaign_tags
	FROM stg_external_apis.braze_campaign_details bcd 
)
, new_recurring_data AS (
	SELECT 
		customer_id,
		MIN(CASE WHEN new_recurring = 'RECURRING' THEN created_date::date END) AS first_recurring_date,
		MIN(paid_date::date) AS first_paid_date
	FROM master."order"
	GROUP BY 1
)
, order_by_submitted_date_campaign AS (
	SELECT 
		cg.campaign_name AS campaign_canvas_name, -- campaign/canvas name
		lower(campaign_tags) AS canvas_campaign_tags,
		cg.customer_type,
		cg.signup_country,
		cg.source_type,
		cg.os,
		cg.crm_label_braze, 
		cg.submitted_date::date,
		cg.paid_order,
		cg.new_recurring, --customer status
		cg.customer_id,
		cg.is_sent,
		cg.is_delivered,
		cg.is_opened,
		cg.is_clicked,
		row_number() over (order by campaign_name,customer_type,signup_country,source_type,os,crm_label_braze,
					submitted_date::date, paid_order,new_recurring,
					customer_id, is_sent, cg.is_delivered, cg.is_opened,
						cg.is_clicked) AS rn,
		count(DISTINCT cg.order_id) AS submitted_orders_by_submitted_date,
		SUM(cg.total_committed_subs_value_eur) AS total_committed_subs_value_eur_by_submitted_date
	FROM dm_marketing.braze_campaign_events_all cg
	LEFT JOIN campaign_tags ct
		ON ct.campaign_id = cg.campaign_id
	WHERE cg.converted_order > 0
		AND cg.submitted_date >= DATE_TRUNC('month',DATEADD('month',-12,current_date))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
, order_by_submitted_date_canvas AS (
	SELECT 
		cg.canvas_name, -- campaign/canvas name
		lower(campaign_tags) AS canvas_campaign_tags,
		cg.customer_type,
		cg.signup_country,
		cg.source_type,
		cg.os,
		cg.crm_label_braze, 
		cg.submitted_date::date,
		cg.paid_order,
		cg.new_recurring, --customer status
		cg.customer_id,
		cg.canvas_step_name,
		cg.is_sent,
		cg.is_delivered,
		cg.is_opened,
		cg.is_clicked,
		row_number() over (order by canvas_name,customer_type,signup_country,source_type,os,crm_label_braze,
					submitted_date::date, paid_order,new_recurring,
					customer_id, canvas_step_name, is_sent, cg.is_delivered, cg.is_opened,
						cg.is_clicked) AS rn,
		count(DISTINCT cg.order_id) AS submitted_orders_by_submitted_date,
		SUM(cg.total_committed_subs_value_eur) AS total_committed_subs_value_eur_by_submitted_date
	FROM dm_marketing.braze_canvas_events_all cg
	LEFT JOIN campaign_tags ct
		ON ct.campaign_id = cg.campaign_id
	WHERE cg.converted_order > 0
		AND cg.submitted_date >= DATE_TRUNC('month',DATEADD('month',-12,current_date))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
SELECT 
	'campaign'::varchar AS campaign_or_canvas,
	COALESCE(cg.campaign_name, osd.campaign_canvas_name) AS campaign_canvas_name, -- campaign/canvas name
	COALESCE(cg.customer_type, osd.customer_type) AS customer_type,
	COALESCE(cg.sent_date::date, osd.submitted_date) AS sent_date,
	COALESCE(cg.signup_country, osd.signup_country) AS signup_country,
	COALESCE(cg.source_type, osd.source_type) AS source_type,
	COALESCE(cg.os, osd.os) AS os,
	COALESCE(cg.crm_label_braze, osd.crm_label_braze) AS crm_label_braze, --customer_label
	COALESCE(CASE WHEN cg.converted_order > 0 THEN cg.submitted_date::date END,osd.submitted_date)  AS submitted_date,
	COALESCE(CASE WHEN cg.converted_order > 0 THEN cg.paid_order END, osd.paid_order) AS paid_order,
	COALESCE(cg.new_recurring, osd.new_recurring) AS new_recurring, --customer status
	COALESCE(CASE WHEN cg.converted_order > 0 THEN cg.customer_id END, osd.customer_id) AS customer_id,
	COALESCE(cg.is_sent, osd.is_sent) AS is_sent,
	COALESCE(cg.is_delivered, osd.is_delivered) AS is_delivered,
	COALESCE(cg.is_opened, osd.is_opened) AS is_opened,
	COALESCE(cg.is_clicked, osd.is_clicked) AS is_clicked,
	NULL::varchar AS canvas_step_name,
	CASE WHEN s.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_customer_unsubscribed,
	COALESCE(lower(campaign_tags), osd.canvas_campaign_tags) AS canvas_campaign_tags,
	NULL::varchar AS cta,
	count(DISTINCT CASE WHEN cg.converted_order > 0 THEN cg.order_id END) AS submitted_orders,
	SUM(CASE WHEN cg.converted_order > 0 THEN cg.total_committed_subs_value_eur END) AS total_committed_subs_value_eur,
	SUM(cg.is_sent_overall) AS is_sent_overall,
	sum(cg.is_delivered_overall) AS is_delivered_overall,
	SUM(cg.is_opened_overall) AS is_opened_overall,
	SUM(cg.is_clicked_overall) AS is_clicked_overall,
	count(DISTINCT cg.id_campaign_hash) AS id_hash,
	count(DISTINCT cg.customer_id) AS customers, --IN ORDER TO calculate SOME metrics that involve ALL customers
	NULL::float AS number_cta,
	sum(osd.submitted_orders_by_submitted_date) AS submitted_orders_by_submitted_date,
	sum(osd.total_committed_subs_value_eur_by_submitted_date) AS total_committed_subs_value_eur_by_submitted_date
FROM dm_marketing.braze_campaign_events_all cg
FULL JOIN order_by_submitted_date_campaign osd
	ON COALESCE(osd.campaign_canvas_name, 'a') = COALESCE(cg.campaign_name, 'b')
	AND COALESCE(osd.customer_type, 'a') = COALESCE(cg.customer_type, 'b')
	AND COALESCE(osd.signup_country, 'a') = COALESCE(cg.signup_country, 'b')
	AND COALESCE(osd.source_type, 'a') = COALESCE(cg.source_type, 'b')
	AND COALESCE(osd.os, 'a') = COALESCE(cg.os, 'b')
	AND COALESCE(osd.crm_label_braze, 'a') = COALESCE(cg.crm_label_braze, 'b')
	AND osd.submitted_date = cg.sent_date::date
	AND osd.paid_order = cg.paid_order
	AND COALESCE(osd.new_recurring, 'a') = COALESCE(cg.new_recurring, 'b')
	AND osd.customer_id = cg.customer_id
	AND COALESCE(osd.is_sent,2) = COALESCE(cg.is_sent,3)
	AND COALESCE(osd.is_delivered,2) = COALESCE(cg.is_delivered,3)
	AND COALESCE(osd.is_opened,2) = COALESCE(cg.is_opened,3)
	AND COALESCE(osd.is_clicked,2) = COALESCE(cg.is_clicked,3)
	AND cg.submitted_date::date = osd.submitted_date::date
LEFT JOIN customers_email_subscribe s
	ON s.customer_id = cg.customer_id
	AND cg.sent_date::date <= s.reporting_date
	AND cg.sent_date::date + 2 >= s.reporting_date -- considering that the customer can unsubscribed UNTIL 2 days AFTER receiving the message
	AND dateadd('day',-1,s.reporting_date)::date <> COALESCE(s.previous_day_was_subscribed::date , '1900-01-01')
		 --if the lag day is not in sequence, means that we had a change on the email_subscribe. If they are 2 dates in a row, so, means the customer was
		 --already unsubscribed.
LEFT JOIN campaign_tags ct
	ON ct.campaign_id = cg.campaign_id
WHERE COALESCE(cg.sent_date::date, osd.submitted_date) >= DATE_TRUNC('month',DATEADD('month',-12,current_date))				
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
--
UNION 
--
SELECT DISTINCT 
	'canvas'::varchar AS campaign_or_canvas,
	COALESCE(cg.canvas_name, osc.canvas_name) AS campaign_canvas_name,
	COALESCE(cg.customer_type, osc.customer_type) AS customer_type,
	COALESCE(cg.sent_date::date, osc.submitted_date) AS sent_date,
	COALESCE(cg.signup_country, osc.signup_country) AS signup_country,
	COALESCE(cg.source_type, osc.source_type) AS source_type,
	COALESCE(cg.os, osc.os) AS os,
	COALESCE(cg.crm_label_braze, osc.crm_label_braze) AS crm_label_braze,
	COALESCE(CASE WHEN cg.converted_order > 0 THEN cg.submitted_date::date END, osc.submitted_date) AS submitted_date,
	COALESCE(CASE WHEN cg.converted_order > 0 THEN cg.paid_order END, osc.paid_order) AS paid_order,
	COALESCE(cg.new_recurring, osc.new_recurring) AS new_recurring,
	COALESCE(CASE WHEN cg.converted_order > 0 THEN cg.customer_id END, osc.customer_id) AS customer_id, --they wanna know customer info ONLY FOR submitted orders
	COALESCE(cg.is_sent, osc.is_sent) AS is_sent,
	COALESCE(cg.is_delivered, osc.is_delivered) AS is_delivered,
	COALESCE(cg.is_opened, osc.is_opened) AS is_opened,
	COALESCE(cg.is_clicked, osc.is_clicked) AS is_clicked,
	COALESCE(cg.canvas_step_name, osc.canvas_step_name) AS canvas_step_name,
	CASE WHEN s.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_customer_unsubscribed,
	COALESCE(lower(ct.canvas_tags), osc.canvas_campaign_tags) AS canvas_campaign_tags,
	NULL::varchar AS cta,
	count(DISTINCT CASE WHEN cg.converted_order > 0 THEN cg.order_id END) AS submitted_orders,
	SUM(CASE WHEN cg.converted_order > 0 THEN cg.total_committed_subs_value_eur END) AS total_committed_subs_value_eur,
	SUM(cg.is_sent_overall) AS is_sent_overall,
	sum(cg.is_delivered_overall) AS is_delivered_overall,
	SUM(cg.is_opened_overall) AS is_opened_overall,
	SUM(cg.is_clicked_overall) AS is_clicked_overall,
	count(DISTINCT cg.id_canva_hash) AS id_hash,
	count(DISTINCT cg.customer_id) AS customers,
	NULL::int AS number_cta,
	SUM(osc.submitted_orders_by_submitted_date) AS submitted_orders_by_submitted_date,
	SUM(osc.total_committed_subs_value_eur_by_submitted_date) AS total_committed_subs_value_eur_by_submitted_date
FROM dm_marketing.braze_canvas_events_all cg
FULL JOIN order_by_submitted_date_canvas osc
	ON COALESCE(cg.canvas_name, 'b') = COALESCE(osc.canvas_name, 'a')
	AND COALESCE(cg.customer_type, 'b') = COALESCE(osc.customer_type, 'a')
	AND COALESCE(cg.signup_country, 'b') = COALESCE(osc.signup_country, 'a')
	AND COALESCE(cg.source_type, 'b') = COALESCE(osc.source_type, 'a')
	AND COALESCE(cg.os, 'b') = COALESCE(osc.os, 'a')
	AND COALESCE(cg.crm_label_braze, 'b') = COALESCE(osc.crm_label_braze, 'a')
	AND cg.sent_date::date = osc.submitted_date
	AND cg.paid_order = osc.paid_order
	AND COALESCE(cg.new_recurring, 'b') = COALESCE(osc.new_recurring, 'a')
	AND cg.customer_id = osc.customer_id
	AND COALESCE(cg.canvas_step_name, 'b') = COALESCE(osc.canvas_step_name, 'a')
	AND COALESCE(cg.is_sent,2) = COALESCE(osc.is_sent,3)
	AND COALESCE(cg.is_delivered,2) = COALESCE(osc.is_delivered,3)
	AND COALESCE(cg.is_opened,2) = COALESCE(osc.is_opened,3)
	AND COALESCE(cg.is_clicked,2) = COALESCE(osc.is_clicked,3)
	AND cg.submitted_date::date = osc.submitted_date
LEFT JOIN customers_email_subscribe s
	ON s.customer_id = cg.customer_id
	AND cg.sent_date::date <= s.reporting_date
	AND cg.sent_date::date + 2 >= s.reporting_date
	AND dateadd('day',-1,s.reporting_date)::date <> COALESCE(s.previous_day_was_subscribed::date , '1900-01-01')
		-- if the lag day is not in sequence, means that we had a change on the email_subscribe. If they are 2 dates in a row, so, means the customer was
		-- already unsubscribed.
LEFT JOIN canvas_tags ct
	ON ct.canvas_id = cg.canvas_id
WHERE COALESCE(cg.sent_date::date, osc.submitted_date) >= DATE_TRUNC('month',DATEADD('month',-12,current_date))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20--,21
--
UNION
--
SELECT -- TO have information about the button_id TO calculate CTA
	'canvas'::varchar AS campaign_or_canvas,
	a.canvas_name AS campaign_canvas_name,
	c.customer_type,
	LEFT(a.date,10)::date AS sent_date,
	c.signup_country,
	'In-App Message'::varchar AS source_type,
	a.platform AS os,
	c.crm_label_braze,
	NULL::date AS submitted_date,
	NULL::int AS paid_order,
	CASE
		WHEN sent_date > nr.first_paid_date
			 THEN 'RECURRING'
		WHEN nr.first_recurring_date IS NULL
			THEN 'NEW'
		WHEN sent_date < nr.first_recurring_date
			THEN 'NEW'
		WHEN sent_date >= nr.first_recurring_date
			THEN 'RECURRING'
	END AS new_recurring,
	NULL::varchar AS customer_id,
	0::int AS is_sent,
	0::int AS is_delivered,
	1::int AS is_opened,
	1::int AS is_clicked,
	NULL::varchar AS canvas_step_name,
	CASE WHEN a.external_user_id IS NOT NULL THEN 1 ELSE 0 END AS is_customer_unsubscribed,
	lower(ct.canvas_tags) AS canvas_campaign_tags,
	CASE WHEN lower(a.button_id) IN ('deals', 'to-tech', 'to the deals', 'find my faves', 'see the deals', 'alles mitbekommen', 'stay up to date',
									'sluiten', 'op de hoogte blijven') THEN 'CTA 1'
		 WHEN lower(a.button_id) IN ('maybe later', 'close', 'back' , 'not right now')THEN 'CTA 2'
		 END AS cta,
	NULL::int AS submitted_orders,
	NULL::float AS total_committed_subs_value_eur,
	NULL::int AS is_sent_overall,
	NULL::int AS is_delivered_overall,
	NULL::int AS is_opened_overall,
	NULL::int AS is_clicked_overall,
	NULL::int AS id_hash,
	NULL::int AS customers,
	count(DISTINCT id) AS number_cta,
	NULL::int AS submitted_orders_by_submitted_date,
	NULL::int AS total_committed_subs_value_eur_by_submitted_date
FROM stg_external_apis.braze_inappmessage_click_event a
LEFT JOIN master.customer c
	ON a.external_user_id::varchar = c.customer_id::varchar
LEFT JOIN new_recurring_data nr
		ON a.external_user_id::varchar = nr.customer_id::varchar
LEFT JOIN canvas_tags ct
	ON a.canvas_id = ct.canvas_id
WHERE sent_date >= DATE_TRUNC('month',DATEADD('month',-12,current_date))
	AND cta IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
WITH NO SCHEMA BINDING;