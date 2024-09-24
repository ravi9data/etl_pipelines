CREATE TEMP TABLE last_runs AS
SELECT * FROM data_engineer.price_drop_r_num WHERE rn = 1;

CREATE TEMP TABLE prev_runs AS
SELECT * FROM data_engineer.price_drop_r_num WHERE rn = 2;

DROP TABLE IF EXISTS data_engineer.price_drop_last_two_checks;
CREATE TABLE data_engineer.price_drop_last_two_checks
AS
SELECT
	l.external_id,
	l.mtl_wishlist_price_drop_names AS last_mtl_wishlist_price_drop_names,
	p.mtl_wishlist_price_drop_names AS prev_mtl_wishlist_price_drop_names,
	l.mtl_wishlist_price_drop_full_prices AS last_mtl_wishlist_price_drop_full_prices,
	p.mtl_wishlist_price_drop_full_prices AS prev_mtl_wishlist_price_drop_full_prices,
	l.mtl_wishlist_price_drop_sale_prices AS last_mtl_wishlist_price_drop_sale_prices,
	p.mtl_wishlist_price_drop_sale_prices AS prev_mtl_wishlist_price_drop_sale_prices,
	l.mtl_wishlist_price_drop_links AS last_mtl_wishlist_price_drop_links,
	p.mtl_wishlist_price_drop_links AS prev_mtl_wishlist_price_drop_links
FROM
	last_runs AS l 
	LEFT JOIN prev_runs AS p ON l.external_id=p.external_id;
