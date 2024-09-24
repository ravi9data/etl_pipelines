DROP VIEW IF EXISTS dm_marketing.v_partnership_reporting_costs;
CREATE VIEW dm_marketing.v_partnership_reporting_costs AS
WITH marketing_costs AS (
	SELECT 
		CASE 
			WHEN country = 'Germany' THEN 'de'
			WHEN country = 'Netherlands' THEN 'nl'
			WHEN country = 'Austria' THEN 'at'
			WHEN country = 'Spain' THEN 'es'
			WHEN country = 'United States' THEN 'us'
		END AS country_code,
		CASE 
			WHEN lower(partner_name) IN ('corporate benefits', 'isic', 'student beans', 'unidays', 'iamstudent', 
			'studentenrabatt','heja', 'hellofresh', 'inspiring benefits', 'lidl', 'sweatcoin', 'vivid', 'wow')
				THEN lower(partner_name) + ' ' + country_code
			WHEN lower(partner_name) = 'hello fresh'
				THEN 'hellofresh'
			WHEN lower(partner_name) = 'seven.ove audio'
				THEN 'baywatch berlin'
			WHEN lower(partner_name) = 'id.me'
				THEN 'idme'
			WHEN lower(partner_name) ilike 'note buddy%'
				THEN 'notebuddys'
			ELSE lower(partner_name)
		END AS partner_name_mapped,
		"date"::date AS cost_date_week,
		SUM(total_spent_local_currency) marketing_costs	
	FROM marketing.marketing_cost_daily_partnership
	GROUP BY 1,2,3
)
, measures AS (
	SELECT  
		DATE_TRUNC('week', p.created_date)::date order_created_week,
		lower(p.partnership_name) AS partner_name_mapped,
		count(DISTINCT p.order_id) AS created_orders,
		count(DISTINCT CASE WHEN p.completed_orders >= 1 THEN p.order_id END) AS submitted_orders,
		count(DISTINCT CASE WHEN p.paid_orders >= 1 THEN p.order_id END) AS paid_orders,
		count(DISTINCT CASE WHEN p.new_recurring = 'NEW' AND p.paid_orders >= 1 THEN p.customer_id END) AS new_customers,
		SUM(p.plan_duration * p.subscription_value) AS commited_subscription_value
	FROM dm_marketing.v_partnership_reporting p
	GROUP BY 1,2
)
SELECT DISTINCT 
	m.order_created_week,
	m.partner_name_mapped,
	m.created_orders,
	m.submitted_orders,
	m.paid_orders,
	m.new_customers,
	m.commited_subscription_value,
	mc.marketing_costs
FROM measures m
INNER JOIN marketing_costs mc
  ON mc.cost_date_week = m.order_created_week
  AND mc.partner_name_mapped = m.partner_name_mapped  
WITH NO SCHEMA BINDING;