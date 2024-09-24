DROP TABLE IF EXISTS marketing.supermetrics_extracts_config;

CREATE TABLE IF NOT EXISTS marketing.supermetrics_extracts_config
(	
	config_id int identity(1,1),
	extract_name varchar(255),
	sheet_name varchar(255),
	datasource  varchar(255),
	target_table varchar(255),
	extract_sql varchar(65535),
	is_incremental int,
	upsert_query varchar(65535),
	is_active bool
)
DISTSTYLE auto
sortkey(extract_name) ;
GRANT SELECT ON marketing.supermetrics_extracts_config to matillion;


INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
values
('Marketing Cost Channel Mappings','Marketing Costs Channel Mappings','Marketing Costs Channel Mappings_Sheet1','marketing_cost_channel_mapping',
'SELECT 
	"country", 
	"customer_type", 
	"brand_non_brand", 
	"cash_non_cash", 
	"channel_grouping", 
	"channel", 
	"channel_detailed", 
	"account", 
	"advertising_channel_type", 
	"campaign_name_contains", 
	"campaign_name_contains2", 
	"campaign_name_does_not_contain", 
	"campaign_group_name_contains", 
	"reporting_grouping_name"
FROM "Marketing Costs Channel Mappings_Sheet1"',
0,null,true
);

INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
values
('Apple Last 4 Weeks Extract',
 'Apple_Search_All_Countries_Last_4_Weeks_Supermetric_Extract',
 'Apple_Search_All_Countries_Last_4_Weeks_Supermetric_Extract_Sheet1',
 'marketing_cost_daily_apple_last_4_week',
'SELECT 
	"date", 
	"account", 
	"campaign_name", 
	"impressions", 
	"installs", 
	"total_spent_local_currency", 
	"total_spent_EUR"
FROM "Apple_Search_All_Countries_Last_4_Weeks_Supermetric_Extract_Sheet1"',
1,
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_apple
USING marketing.marketing_cost_daily_apple_last_4_week cur
WHERE marketing_cost_daily_apple.date::date = cur.date::date
  AND cur.date IS NOT NULL  
; 


INSERT INTO marketing.marketing_cost_daily_apple
SELECT 
  date::TIMESTAMP
  ,account
  ,campaign_name
  ,impressions::BIGINT
  ,installs::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
FROM marketing.marketing_cost_daily_apple_last_4_week
WHERE date IS NOT NULL
;

END TRANSACTION;'
,true
);

INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
values
('Tiktok Last 4 Weeks Extract',
'TikTok_All_Countries_Last_4_Weeks_Supermetric_Extract',
'TikTok_All_Countries_Last_4_Weeks_Supermetric_Extract_Dynamic',
'marketing_cost_daily_tiktok_last_4_weeks',
'SELECT 
	"date", 
	"campaign_name", 
	"ad_name", 
	"impressions", 
	"clicks", 
	"total_spent_local_currency", 
	"total_spent_EUR", 
	"conversions"
FROM "TikTok_All_Countries_Last_4_Weeks_Supermetric_Extract_Dynamic Moving Window"',
1,
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_tiktok
USING marketing.marketing_cost_daily_tiktok_last_4_weeks cur
WHERE marketing_cost_daily_tiktok.date::date = cur.date::date
; 

INSERT INTO marketing.marketing_cost_daily_tiktok
SELECT 
  date::TIMESTAMP
  ,campaign_name
  ,ad_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
FROM marketing.marketing_cost_daily_tiktok_last_4_weeks
WHERE date IS NOT NULL
;  
END TRANSACTION;'
,true
);


INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
VALUES('Linkedin Last 12 Weeks Extract', 
'Linkedin_All_Countries_Last_12_Weeks_Supermetric_Extract', 
'Linkedin_All_Countries_Last_12_Weeks_Supermetric_Extract_Dynamic Moving Window', 
'marketing_cost_daily_linkedin_last_12_weeks', 
'SELECT 
	"date", 
	"campaign_name", 
	"campaign_group_name", 
	"impressions", 
	"clicks", 
	"total_spent_local_currency", 
	"total_spent_EUR", 
	"conversions"
FROM "Linkedin_All_Countries_Last_12_Weeks_Supermetric_Extract_Dynamic Moving Window"', 1, 
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_linkedin
USING marketing.marketing_cost_daily_linkedin_last_12_weeks cur
WHERE marketing_cost_daily_linkedin.date::date = cur.date::date; 

INSERT INTO marketing.marketing_cost_daily_linkedin
SELECT 
  date::TIMESTAMP
  ,campaign_name
  ,campaign_group_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
FROM marketing.marketing_cost_daily_linkedin_last_12_weeks
WHERE date IS NOT NULL;
  
END TRANSACTION;',true);

INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
VALUES('Bing Last 15 Weeks Extract',
'Bing_All_Countries_Last_15_Weeks_Supermetric_Extract',
'Bing_All_Countries_Last_15_Weeks_Supermetric_Extract_Dynamic',
'marketing_cost_daily_bing_last_15_weeks',
'SELECT 
	"date", 
	"account", 
	"advertising_channel_type", 
	"campaign_name", 
	"impressions", 
	"clicks", 
	"total_spent_local_currency", 
	"total_spent_EUR", 
	"conversions"
FROM "Bing_All_Countries_Last_15_Weeks_Supermetric_Extract_Dynamic Moving Window"',
1,
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_bing
USING marketing.marketing_cost_daily_bing_last_15_weeks cur
WHERE marketing_cost_daily_bing.date::date = cur.date::date
; 

INSERT INTO marketing.marketing_cost_daily_bing
SELECT 
  date::TIMESTAMP
  ,account
  ,advertising_channel_type
  ,campaign_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
FROM marketing.marketing_cost_daily_bing_last_15_weeks
WHERE date IS NOT NULL
  AND campaign_name IS NOT NULL
;  
END TRANSACTION;',
true);

INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
VALUES(
'Outbrain Last 12 Weeks Extract',
'Outbrain_All_Countries_Last_12_Weeks_Supermetric_Extract',
'Outbrain_All_Countries_Last_12_Weeks_Supermetric_Extract_Sheet1',
'marketing_cost_daily_outbrain_last_12_weeks',
'SELECT 
	"date", 
	"campaign_name", 
	"impressions", 
	"clicks", 
	"total_spent_local_currency", 
	"total_spent_EUR", 
	"conversions"
FROM "Outbrain_All_Countries_Last_12_Weeks_Supermetric_Extract_Sheet1"',
1,
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_outbrain
USING marketing.marketing_cost_daily_outbrain_last_12_weeks cur
WHERE marketing_cost_daily_outbrain.date::date = cur.date::date
  AND cur.date IS NOT NULL  
; 


INSERT INTO marketing.marketing_cost_daily_outbrain
SELECT 
  date::TIMESTAMP
  ,campaign_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::DOUBLE PRECISION
FROM marketing.marketing_cost_daily_outbrain_last_12_weeks
WHERE date IS NOT NULL
;

END TRANSACTION;',
true
);

INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
VALUES('Snapchat Last 12 Weeks Extract',
'Snapschat_All_Countries_Last_12_Weeks_Supermetric_Extract',
'Snapschat_All_Countries_Last_12_Weeks_Supermetric_Extract_Dynamic',
'marketing_cost_daily_snapchat_last_12_weeks',
'SELECT 
	"date", 
	"campaign_name", 
	"ad_name", 
	"impressions", 
	"clicks", 
	"total_spent_local_currency", 
	"total_spent_EUR", 
	"conversions"
FROM "Snapschat_All_Countries_Last_12_Weeks_Supermetric_Extract_Dynamic Moving Window"',
1,
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_snapchat
USING marketing.marketing_cost_daily_snapchat_last_12_weeks cur
WHERE marketing_cost_daily_snapchat.date::date = cur.date::date
; 

INSERT INTO marketing.marketing_cost_daily_snapchat
SELECT 
  date::TIMESTAMP
  ,campaign_name
  ,ad_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
FROM marketing.marketing_cost_daily_snapchat_last_12_weeks
WHERE date IS NOT NULL
;

END TRANSACTION;',
true);


INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
values (
'Google Last 4 Weeks Extract',
'Google_All_Countries_Last_4_Weeks_Supermetric_Extract',
'Google_All_Countries_Last_4_Weeks_Supermetric_Extract_Dynamic',
'marketing_cost_daily_google_last_4_weeks',
'SELECT 
	"date", 
	"account", 
	"advertising_channel_type", 
	"campaign_name", 
	"impressions", 
	"clicks", 
	"total_spent_local_currency", 
	"total_spent_EUR", 
	"conversions"
FROM "Google_All_Countries_Last_4_Weeks_Supermetric_Extract_Dynamic Moving Window"',
1,
'BEGIN TRANSACTION;

DELETE FROM marketing.marketing_cost_daily_google
USING marketing.marketing_cost_daily_google_last_4_weeks cur
WHERE marketing_cost_daily_google.date::date = cur.date::date
  AND cur.date IS NOT NULL  
; 


INSERT INTO marketing.marketing_cost_daily_google
SELECT 
  date::TIMESTAMP
  ,account
  ,advertising_channel_type
  ,campaign_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::DOUBLE PRECISION
FROM marketing.marketing_cost_daily_google_last_4_weeks
WHERE date IS NOT NULL
;

END TRANSACTION;',
true
);


INSERT INTO marketing.supermetrics_extracts_config
(extract_name, sheet_name, datasource, target_table, extract_sql, is_incremental, upsert_query,is_active)
values ('SEO Costs','SEO Costs','SEO Costs_Sheet1','marketing_cost_daily_seo','SELECT 
	"date", 
	"country", 
	"total_spent_local_currency", 
	"currency", 
	"url",
	"is_test_and_learn"
FROM "SEO Costs_Sheet1"',0,null,true),
('Influencer Cost Weekly Summary','Influencer Cost Weekly Summary','Influencer Cost Weekly Summary_Sheet1','marketing_cost_daily_influencer_summary_2021_10_14',
'SELECT 
	"country", 
	"customer_type", 
	"week_date", 
	"total_spent_local_currency",
	"currency"
FROM "Influencer Cost Weekly Summary_Sheet1"',0,null,true),
('Affiliate Fixed Costs','Affiliate Costs','Affiliate Costs_Fixed_Costs','marketing_cost_daily_affiliate_fixed',
'SELECT 
	"date", 
	"country", 
	"total_spent_local_currency", 
	"currency", 
	"affiliate_network", 
	"affiliate"
FROM "Affiliate Costs_Fixed_Costs"',0,null,true),
('Affiliate Order Costs','Affiliate Costs','Affiliate Costs_Order_Costs','marketing_cost_daily_affiliate_order',
'SELECT 
	"date", 
	"country", 
	"total_spent_local_currency", 
	"currency", 
	"order_id", 
	"affiliate_network", 
	"affiliate"
FROM "Affiliate Costs_Order_Costs"',0,null,true),
('Partnership Costs','Partnership Costs','Partnership Costs_Sheet1','marketing_cost_daily_partnership',
'SELECT 
	"date", 
	"country", 
	"total_spent_local_currency", 
	"currency", 
	"partner_name", 
	"cost_type",
	"is_test_and_learn"
FROM "Partnership Costs_Sheet1"',0,null,true),
('Retail Costs','Retail Costs','Retail Costs_Sheet1','marketing_cost_daily_retail',
'SELECT 
	"date", 
	"country", 
	"total_spent_local_currency", 
	"currency", 
	"retail_name", 
	"channel", 
	"cost_type", 
	"placement", 
	"is_test_and_learn"
FROM "Retail Costs_Sheet1"',0,null,true),
('TV Costs','TV Costs','TV Costs_Sheet1','marketing_cost_tv',
'SELECT 
	"date", 
	"country", 
	"gross_budget_spend", 
	"gross_actual_spend", 
	"cash_percentage", 
	"non_cash_percentage", 
	"currency", 
	"brand_non_brand", 
FROM "TV Costs_Sheet1"',0,null,true),
('Billboard Costs','Billboard Costs','Billboard Costs_Sheet1','marketing_cost_billboard',
'SELECT 
	"date", 
	"country", 
	"gross_budget_spend", 
	"gross_actual_spend", 
	"cash_percentage", 
	"non_cash_percentage", 
	"currency", 
	"brand_non_brand", 
FROM "Billboard Costs_Sheet1"',0,null,true);