--snapshot of everyday
DELETE FROM master.purchase_request_item_historical
WHERE date = current_date - 1;

INSERT INTO master.purchase_request_item_historical
SELECT purchase_request_item_id,
purchase_request_item_sfid,
purchase_item_created_date,
request_id,
request_date,
purchase_date,
purchased_date,
in_delivery_date,
delivery_number,
delivered_to_warehouse,
supplier_name,
supplier_account,
eta,
purchase_order_number,
supplier_country,
wh_goods_receipt,
delivered,
total_price,
pending_delivery,
request_partner,
purchase_price,
rrp,
purchase_quantity,
final_quantity,
effective_quantity,
delivered_quantity,
net_quantity,
booked_quantity,
request_status,
is_pri_booked,
total_pris_in_pr,
unbooked_pri_in_pr,
partially_booked_pri_in_pr,
fully_booked_pri_in_pr,
is_pr_booked,
first_asset_in_pri_booked_at,
first_asset_in_pr_booked_at,
variant_sku,
product_sku,
product_name,
product_brand,
capital_source_name,
category,
current_date - 1 AS date
FROM ods_production.purchase_request_item
WHERE purchase_item_created_date < current_date;

GRANT SELECT ON master.purchase_request_item_historical TO treasury_redash;
