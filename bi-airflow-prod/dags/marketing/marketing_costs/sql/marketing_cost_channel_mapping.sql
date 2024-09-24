
TRUNCATE TABLE marketing.marketing_cost_channel_mapping;
INSERT INTO marketing.marketing_cost_channel_mapping
SELECT
	country,
	customer_type,
	brand_non_brand,
	cash_non_cash,
	channel_grouping,
	channel,
	channel_detailed,
	account,
	advertising_channel_type,
	medium,
	campaign_name_contains,
	campaign_name_contains2,
	campaign_name_does_not_contain,
	campaign_group_name_contains,
	reporting_grouping_name
FROM staging.marketing_cost_channel_mapping_sheet1;
