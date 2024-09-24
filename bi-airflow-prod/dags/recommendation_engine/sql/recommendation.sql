DROP TABLE IF EXISTS dm_consideration.recommendations;
CREATE TABLE dm_consideration.recommendations AS
WITH subs_count AS (
    SELECT
        product_sku,
        CASE
	        WHEN store_id = 1 THEN 'de'
	        WHEN store_id = 5 THEN 'nl'
	        WHEN store_id = 4 THEN 'at'
	        WHEN store_id = 618 THEN 'es'
	        WHEN store_id = 621 THEN 'us'
    	END AS store_parent,
        COUNT(DISTINCT subscription_id) AS subscription_count
    FROM master.subscription
    WHERE customer_type = 'normal_customer'
        AND store_id in (1,4,5,621,618)
        AND start_date >= DATEADD('month', -6, current_date)
    GROUP BY 1,2
)
, order_count AS (
	SELECT
		oi.product_sku,
        CASE
	        WHEN o.store_id = 1 THEN 'de'
	        WHEN o.store_id = 5 THEN 'nl'
	        WHEN o.store_id = 4 THEN 'at'
	        WHEN o.store_id = 618 THEN 'es'
	        WHEN o.store_id = 621 THEN 'us'
    	END AS store_parent,
    	sum(oi.quantity) AS order_count
	FROM ods_production.order_item oi
	INNER JOIN master.ORDER o
	  ON oi.order_id = o.order_id
	  AND o.store_id in (1,4,5,621,618)
	WHERE oi.created_at >= DATEADD('month', -6, current_date)
	GROUP BY 1,2
)
, cheapest_price AS (
	SELECT
		product_sku,
		store_parent,
		CASE
			WHEN POSITION(',' IN rental_plan_price_1_month) > 0
				THEN LEFT(rental_plan_price_1_month, POSITION(',' IN rental_plan_price_1_month) -1)
			ELSE rental_plan_price_1_month
		END::float m1_price,
		CASE
			WHEN POSITION(',' IN rental_plan_price_3_months) > 0
				THEN LEFT(rental_plan_price_3_months, POSITION(',' IN rental_plan_price_3_months) -1)
			ELSE rental_plan_price_3_months
		END::float m3_price,
		CASE
			WHEN POSITION(',' IN rental_plan_price_6_months) > 0
				THEN LEFT(rental_plan_price_6_months, POSITION(',' IN rental_plan_price_6_months) -1)
			ELSE rental_plan_price_6_months
		END::float m6_price,
		CASE
			WHEN POSITION(',' IN rental_plan_price_12_months) > 0
				THEN LEFT(rental_plan_price_12_months, POSITION(',' IN rental_plan_price_12_months) -1)
			ELSE rental_plan_price_12_months
		END::float m12_price,
		CASE
			WHEN POSITION(',' IN rental_plan_price_18_months) > 0
				THEN LEFT(rental_plan_price_18_months, POSITION(',' IN rental_plan_price_18_months) -1)
			ELSE rental_plan_price_18_months
		END::float m18_price,
		CASE
			WHEN POSITION(',' IN rental_plan_price_24_months) > 0
				THEN LEFT(rental_plan_price_24_months, POSITION(',' IN rental_plan_price_24_months) -1)
			ELSE rental_plan_price_24_months
		END::float m24_price,
		COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(m24_price, m18_price), m12_price), m6_price), m3_price), m1_price) AS cheapest_price
	FROM pricing.all_pricing_grover
	WHERE store_parent IN ('de', 'es', 'nl', 'at', 'us')
)
, product_ranking_raw AS (
	SELECT product_id, austria, germany, spain, netherlands, united_states
	FROM pricing.product_rankings_by_store_snapshot
	WHERE date = (SELECT max(date) FROM pricing.product_rankings_by_store_snapshot)
)
, product_ranking AS (
	SELECT
		product_id,
		'de' AS store_parent,
		germany AS product_ranking
	FROM product_ranking_raw
	UNION ALL
	SELECT
		product_id,
		'at' AS store_parent,
		austria AS product_ranking
	FROM product_ranking_raw
	UNION ALL
	SELECT
		product_id,
		'es' AS store_parent,
		spain AS product_ranking
	FROM product_ranking_raw
	UNION ALL
	SELECT
		product_id,
		'nl' AS store_parent,
		netherlands AS product_ranking
	FROM product_ranking_raw
	UNION ALL
	SELECT
		product_id,
		'us' AS store_parent,
		united_states AS product_ranking
	FROM product_ranking_raw
)
, basis AS (
	SELECT
		pph.product_sku,
		p.product_id,
		p.product_name,
		p.category_name,
		p.subcategory_name,
		CASE WHEN p.brand = 'APPLE' THEN 'APPLE' ELSE 'NON-APPLE' END AS brand_family,
		pph.store_parent,
	    CASE
	        WHEN pph.store_parent = 'de' THEN 1
	        WHEN pph.store_parent = 'nl' THEN 5
	        WHEN pph.store_parent = 'at' THEN 4
	        WHEN pph.store_parent = 'es' THEN 618
	        WHEN pph.store_parent = 'us' THEN 621
	    END AS store_id,
	    COALESCE(sc.subscription_count, 0) AS subscription_count,
	    COALESCE(oc.order_count, 0) AS order_count,
	    COALESCE(pr.product_ranking, '-1')::INT AS product_ranking,
	    pph.cheapest_price,
	    CASE WHEN pph.cheapest_price - 10 < 0 THEN 0 ELSE pph.cheapest_price - 10 END AS lower_border,
	    CASE WHEN pph.cheapest_price - 20 < 0 THEN 0 ELSE pph.cheapest_price - 20 END AS lower_border_loosen,
	    CASE WHEN pph.cheapest_price < 10 THEN 30 ELSE ROUND(pph.cheapest_price + 20, 0) END AS upper_border,
	    CASE WHEN pph.cheapest_price < 10 THEN 40 ELSE ROUND(pph.cheapest_price + 30, 0) END AS upper_border_loosen
	FROM cheapest_price pph
	LEFT JOIN ods_production.product p
	  ON pph.product_sku = p.product_sku
	LEFT JOIN subs_count sc
	  ON pph.product_sku = sc.product_sku
	  AND pph.store_parent = sc.store_parent
	LEFT JOIN order_count oc
	  ON pph.product_sku = oc.product_sku
	  AND pph.store_parent = oc.store_parent
	LEFT JOIN product_ranking pr
	  ON p.product_id = pr.product_id
	  AND pph.store_parent = pr.store_parent
)
, recommendations1 AS (
	SELECT
		a.store_id,
		a.product_id,
		b.product_id AS recommendation_product_id,
		CASE
			WHEN b.product_id IS NULL THEN 0
			ELSE ROW_NUMBER() OVER (PARTITION BY a.product_id, a.store_id ORDER BY b.subscription_count DESC, b.product_ranking DESC, b.order_count DESC, b.product_id DESC)
		END AS rowno
	FROM basis a
	LEFT JOIN basis b
	  ON a.store_id = b.store_id
	  AND a.product_id <> b.product_id
	  AND a.brand_family = b.brand_family
	  AND a.subcategory_name = b.subcategory_name
	  AND b.cheapest_price BETWEEN a.lower_border AND a.upper_border
)
, recommendations2 AS (
	SELECT
		a.store_id,
		a.product_id,
		b.product_id AS recommendation_product_id,
		c.last_rowno + ROW_NUMBER() OVER (PARTITION BY a.product_id, a.store_id ORDER BY b.subscription_count DESC, b.product_ranking DESC, b.order_count DESC, b.product_id DESC) AS rowno
	FROM basis a
	INNER JOIN basis b
	  ON a.store_id = b.store_id
	  AND a.product_id <> b.product_id
	  AND a.brand_family = b.brand_family
	  AND a.category_name = b.category_name
	  AND b.cheapest_price BETWEEN a.lower_border AND a.upper_border
	INNER JOIN (SELECT product_id, store_id, LISTAGG(recommendation_product_id, ',') AS rec_list, MAX(rowno) AS last_rowno FROM recommendations1 GROUP BY 1,2) c
	  ON a.store_id = c.store_id
	  AND a.product_id = c.product_id
	  AND POSITION(b.product_id::varchar IN COALESCE(c.rec_list,'n/a')) = 0
)
, recommendations3 AS (
	SELECT
		a.store_id,
		a.product_id,
		b.product_id AS recommendation_product_id,
		c2.last_rowno + ROW_NUMBER() OVER (PARTITION BY a.product_id, a.store_id ORDER BY b.subscription_count DESC, b.product_ranking DESC, b.order_count DESC, b.product_id DESC) AS rowno
	FROM basis a
	INNER JOIN basis b
	  ON a.store_id = b.store_id
	  AND a.product_id <> b.product_id
	  AND a.brand_family = b.brand_family
	  AND a.category_name = b.category_name
	  AND b.cheapest_price BETWEEN a.lower_border_loosen AND a.upper_border_loosen
	INNER JOIN (SELECT product_id, store_id, LISTAGG(recommendation_product_id, ',') AS rec_list, MAX(rowno) AS last_rowno FROM recommendations2 GROUP BY 1,2) c2
	  ON a.store_id = c2.store_id
	  AND a.product_id = c2.product_id
	  AND POSITION(b.product_id::varchar IN COALESCE(c2.rec_list,'n/a')) = 0
)
--, final_ AS (
SELECT
  r.store_id,
  r.product_id,
  COALESCE(COALESCE(r1_1.recommendation_product_id, r2_1.recommendation_product_id), r3_1.recommendation_product_id) AS rec1,
  COALESCE(COALESCE(r1_2.recommendation_product_id, r2_2.recommendation_product_id), r3_2.recommendation_product_id) AS rec2,
  COALESCE(COALESCE(r1_3.recommendation_product_id, r2_3.recommendation_product_id), r3_3.recommendation_product_id) AS rec3,
  COALESCE(COALESCE(r1_4.recommendation_product_id, r2_4.recommendation_product_id), r3_4.recommendation_product_id) AS rec4,
  COALESCE(COALESCE(r1_5.recommendation_product_id, r2_5.recommendation_product_id), r3_5.recommendation_product_id) AS rec5,
  COALESCE(COALESCE(r1_6.recommendation_product_id, r2_6.recommendation_product_id), r3_6.recommendation_product_id) AS rec6,
  COALESCE(COALESCE(r1_7.recommendation_product_id, r2_7.recommendation_product_id), r3_7.recommendation_product_id) AS rec7,
  COALESCE(COALESCE(r1_8.recommendation_product_id, r2_8.recommendation_product_id), r3_8.recommendation_product_id) AS rec8,
  COALESCE(COALESCE(r1_9.recommendation_product_id, r2_9.recommendation_product_id), r3_9.recommendation_product_id) AS rec9,
  COALESCE(COALESCE(r1_10.recommendation_product_id, r2_10.recommendation_product_id), r3_10.recommendation_product_id) AS rec10,
  COALESCE(COALESCE(r1_11.recommendation_product_id, r2_11.recommendation_product_id), r3_11.recommendation_product_id) AS rec11,
  COALESCE(COALESCE(r1_12.recommendation_product_id, r2_12.recommendation_product_id), r3_12.recommendation_product_id) AS rec12,
  COALESCE(COALESCE(r1_13.recommendation_product_id, r2_13.recommendation_product_id), r3_13.recommendation_product_id) AS rec13
FROM basis r
LEFT JOIN recommendations1 r1_1
  ON r.store_id = r1_1.store_id
  AND r.product_id = r1_1.product_id
  AND r1_1.rowno = 1
LEFT JOIN recommendations1 r1_2
  ON r.store_id = r1_2.store_id
  AND r.product_id = r1_2.product_id
  AND r1_2.rowno = 2
LEFT JOIN recommendations1 r1_3
  ON r.store_id = r1_3.store_id
  AND r.product_id = r1_3.product_id
  AND r1_3.rowno = 3
LEFT JOIN recommendations1 r1_4
  ON r.store_id = r1_4.store_id
  AND r.product_id = r1_4.product_id
  AND r1_4.rowno = 4
LEFT JOIN recommendations1 r1_5
  ON r.store_id = r1_5.store_id
  AND r.product_id = r1_5.product_id
  AND r1_5.rowno = 5
LEFT JOIN recommendations1 r1_6
  ON r.store_id = r1_6.store_id
  AND r.product_id = r1_6.product_id
  AND r1_6.rowno = 6
LEFT JOIN recommendations1 r1_7
  ON r.store_id = r1_7.store_id
  AND r.product_id = r1_7.product_id
  AND r1_7.rowno = 7
LEFT JOIN recommendations1 r1_8
  ON r.store_id = r1_8.store_id
  AND r.product_id = r1_8.product_id
  AND r1_8.rowno = 8
LEFT JOIN recommendations1 r1_9
  ON r.store_id = r1_9.store_id
  AND r.product_id = r1_9.product_id
  AND r1_9.rowno = 9
LEFT JOIN recommendations1 r1_10
  ON r.store_id = r1_10.store_id
  AND r.product_id = r1_10.product_id
  AND r1_10.rowno = 10
LEFT JOIN recommendations1 r1_11
  ON r.store_id = r1_11.store_id
  AND r.product_id = r1_11.product_id
  AND r1_11.rowno = 11
LEFT JOIN recommendations1 r1_12
  ON r.store_id = r1_12.store_id
  AND r.product_id = r1_12.product_id
  AND r1_12.rowno = 12
LEFT JOIN recommendations1 r1_13
  ON r.store_id = r1_13.store_id
  AND r.product_id = r1_13.product_id
  AND r1_13.rowno = 13
LEFT JOIN recommendations2 r2_1
  ON r.store_id = r2_1.store_id
  AND r.product_id = r2_1.product_id
  AND r2_1.rowno = 1
LEFT JOIN recommendations2 r2_2
  ON r.store_id = r2_2.store_id
  AND r.product_id = r2_2.product_id
  AND r2_2.rowno = 2
LEFT JOIN recommendations2 r2_3
  ON r.store_id = r2_3.store_id
  AND r.product_id = r2_3.product_id
  AND r2_3.rowno = 3
LEFT JOIN recommendations2 r2_4
  ON r.store_id = r2_4.store_id
  AND r.product_id = r2_4.product_id
  AND r2_4.rowno = 4
LEFT JOIN recommendations2 r2_5
  ON r.store_id = r2_5.store_id
  AND r.product_id = r2_5.product_id
  AND r2_5.rowno = 5
LEFT JOIN recommendations2 r2_6
  ON r.store_id = r2_6.store_id
  AND r.product_id = r2_6.product_id
  AND r2_6.rowno = 6
LEFT JOIN recommendations2 r2_7
  ON r.store_id = r2_7.store_id
  AND r.product_id = r2_7.product_id
  AND r2_7.rowno = 7
LEFT JOIN recommendations2 r2_8
  ON r.store_id = r2_8.store_id
  AND r.product_id = r2_8.product_id
  AND r2_8.rowno = 8
LEFT JOIN recommendations2 r2_9
  ON r.store_id = r2_9.store_id
  AND r.product_id = r2_9.product_id
  AND r2_9.rowno = 9
LEFT JOIN recommendations2 r2_10
  ON r.store_id = r2_10.store_id
  AND r.product_id = r2_10.product_id
  AND r2_10.rowno = 10
LEFT JOIN recommendations2 r2_11
  ON r.store_id = r2_11.store_id
  AND r.product_id = r2_11.product_id
  AND r2_11.rowno = 11
LEFT JOIN recommendations2 r2_12
  ON r.store_id = r2_12.store_id
  AND r.product_id = r2_12.product_id
  AND r2_12.rowno = 12
LEFT JOIN recommendations2 r2_13
  ON r.store_id = r2_13.store_id
  AND r.product_id = r2_13.product_id
  AND r2_13.rowno = 13
LEFT JOIN recommendations3 r3_1
  ON r.store_id = r3_1.store_id
  AND r.product_id = r3_1.product_id
  AND r3_1.rowno = 1
LEFT JOIN recommendations3 r3_2
  ON r.store_id = r3_2.store_id
  AND r.product_id = r3_2.product_id
  AND r3_2.rowno = 2
LEFT JOIN recommendations3 r3_3
  ON r.store_id = r3_3.store_id
  AND r.product_id = r3_3.product_id
  AND r3_3.rowno = 3
LEFT JOIN recommendations3 r3_4
  ON r.store_id = r3_4.store_id
  AND r.product_id = r3_4.product_id
  AND r3_4.rowno = 4
LEFT JOIN recommendations3 r3_5
  ON r.store_id = r3_5.store_id
  AND r.product_id = r3_5.product_id
  AND r3_5.rowno = 5
LEFT JOIN recommendations3 r3_6
  ON r.store_id = r3_6.store_id
  AND r.product_id = r3_6.product_id
  AND r3_6.rowno = 6
LEFT JOIN recommendations3 r3_7
  ON r.store_id = r3_7.store_id
  AND r.product_id = r3_7.product_id
  AND r3_7.rowno = 7
LEFT JOIN recommendations3 r3_8
  ON r.store_id = r3_8.store_id
  AND r.product_id = r3_8.product_id
  AND r3_8.rowno = 8
LEFT JOIN recommendations3 r3_9
  ON r.store_id = r3_9.store_id
  AND r.product_id = r3_9.product_id
  AND r3_9.rowno = 9
LEFT JOIN recommendations3 r3_10
  ON r.store_id = r3_10.store_id
  AND r.product_id = r3_10.product_id
  AND r3_10.rowno = 10
LEFT JOIN recommendations3 r3_11
  ON r.store_id = r3_11.store_id
  AND r.product_id = r3_11.product_id
  AND r3_11.rowno = 11
LEFT JOIN recommendations3 r3_12
  ON r.store_id = r3_12.store_id
  AND r.product_id = r3_12.product_id
  AND r3_12.rowno = 12
LEFT JOIN recommendations3 r3_13
  ON r.store_id = r3_13.store_id
  AND r.product_id = r3_13.product_id
  AND r3_13.rowno = 13
;

GRANT USAGE ON SCHEMA dm_consideration TO app_consideration;
GRANT USAGE ON SCHEMA dm_consideration TO redash_pricing;
GRANT SELECT ON dm_consideration.recommendations TO app_consideration;
GRANT SELECT ON dm_consideration.recommendations TO redash_pricing;
