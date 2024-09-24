BEGIN TRANSACTION;

Truncate TABLE staging.sku_decision_to_sell_list;

INSERT INTO staging.sku_decision_to_sell_list
SELECT * FROM staging_google_sheet.sku_decision_to_sell_list;

END TRANSACTION;
