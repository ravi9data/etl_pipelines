#potential_price, sold_price, sold, sold_all, date_in, date_testing, date_sold, product_sku, variant_sku, date_updated)
#SELECT count(*) FROM
SELECT 
main.inventory_id,
#mast.inventory_id as test,
a.asset_id,
#b.inventory_id as b_inv,
#b.asset_id as b_asset,
main.device,
main.comment,
#'alter als oktober 2020' as comment,
main.condition,
main.channel as channel_original,
CASE 
#when main.comment = 'Rückversand Grover / SaSe' THEN 'Return'
when main.channel = 'Recyling' THEN 'Recycling'
when main.channel = 'Recycling' THEN 'Recycling'
when main.channel = 'zurück an Grover - tbd' THEN 'Return'
when main.channel = 'Repair (Preisliste abwarten)' THEN 'Repair'
when main.channel = 'In Reparatur - danach SaSe' THEN 'Repair'
when main.channel = 'gewerblicher Verkauf' THEN 'B2B'
when main.channel = 'SaSe' THEN 'B2C'
ELSE main.channel
END as channel,
ROUND(main.potential_price,2) as potential_price,
ROUND(main.sold_price,2) as sold_price,
CASE 
when main.date_sold < '2021-04-01' THEN 1
ELSE 0
END as sold,
CASE 
when main.date_sold is not NULL THEN 1
ELSE 0
END as sold_all,
main.date_in,
main.date_testing,
main.date_sold,
a.product_sku,
a.variant_sku,
main.date_created as date_updated
FROM flip_masterliste_raw main
#LEFT JOIN flip_masterliste b on b.inventory_id = main.inventory_id
LEFT JOIN flip_masterliste_master mast on mast.inventory_id = main.inventory_id

where version IN(12) and mast.inventory_id is null
