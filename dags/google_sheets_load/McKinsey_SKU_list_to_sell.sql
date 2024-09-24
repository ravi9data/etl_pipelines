Truncate table staging.sku_decision_to_sell_list;

insert into staging.sku_decision_to_sell_list
select * from staging_google_sheet.sku_decision_to_sell_list;