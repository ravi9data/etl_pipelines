DROP VIEW IF EXISTS dm_marketing.v_google_ads_data;
CREATE VIEW dm_marketing.v_google_ads_data AS
WITH
    orders AS (
    select a.customer_id,
           a.order_id,
           a.submitted_date::DATE AS submitted_date,
           a.completed_orders,
           a.paid_orders,
           a.new_recurring,
           a.marketing_campaign,
           a.marketing_channel,
           SUM(b.committed_sub_value + b.additional_committed_sub_value) AS committed_sub_value
    FROM master."order" a
    LEFT JOIN master.subscription b 
    	ON a.order_id = b.order_id
    LEFT JOIN ods_production.order_marketing_channel omc
		ON a.order_id = omc.order_id
    WHERE a.submitted_date >= '2023-01-01'
    	AND omc.marketing_source ILIKE '%google%'
    GROUP BY 1,2,3,4,5,6,7,8
),

campaign_data AS (
    SELECT marketing_campaign AS campaign_name,
           submitted_date,
           COUNT(DISTINCT CASE WHEN new_recurring = 'NEW' AND paid_orders >=1 THEN customer_id END) AS new_customers,
           COUNT(DISTINCT customer_id) AS total_customers,
           COUNT(DISTINCT CASE WHEN completed_orders >= 1 THEN order_id END) AS submitted_orders,
           COUNT(DISTINCT CASE WHEN paid_orders >= 1 THEN order_id END) AS paid_orders,
           SUM(committed_sub_value) AS committed_sub_value
    FROM orders
    GROUP BY 1,2
),

unique_google_campaigns AS (
    SELECT DISTINCT campaign_name, account, advertising_channel_type
    FROM staging.google_ads_data
),

final AS (
SELECT COALESCE(a.date::DATE,b.submitted_date) AS reporting_date,
       COALESCE(a.campaign_name,b.campaign_name) AS campaign_name,
       a.account,
       a.cost_eur::DOUBLE PRECISION AS cost,
       a.impressions,
       a.advertising_channel_type,
       a.clicks,
       a.budget::DOUBLE PRECISION AS budget,
       a.budget_percentage_used::DOUBLE PRECISION AS budget_percentage_used,
       b.new_customers,
       b.total_customers,
       b.submitted_orders,
       b.paid_orders,
       b.committed_sub_value,
       a.conversions
FROM staging.google_ads_data a 
FULL JOIN campaign_data b on a.campaign_name = b.campaign_name AND a.date::DATE = b.submitted_date
)

SELECT reporting_date,
       campaign_name,
       COALESCE(a.account,b.account) AS account,
       cost,
       impressions,
       COALESCE(a.advertising_channel_type, b.advertising_channel_type) AS advertising_channel_type,
       clicks,
       budget,
       budget_percentage_used,
       new_customers,
       total_customers,
       submitted_orders,
       paid_orders,
       committed_sub_value,
       conversions
FROM final a
INNER JOIN unique_google_campaigns b USING (campaign_name)
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_marketing.v_google_ads_data TO tableau;
--data coming from supermetrics: https://docs.google.com/spreadsheets/d/1dQ-9m81hbQWd0-vzrJTAL-vBAQhTQkRh7_cnn0ID-3A/edit#gid=0
