DROP TABLE IF EXISTS tmp_live_reporting_rental_plans;
CREATE TEMP TABLE tmp_live_reporting_rental_plans AS 

WITH old_prices AS
	(
	SELECT
		DISTINCT
		rp.id,
		s.store_name,
		s.store_label,
		LAST_VALUE(op.price) OVER (PARTITION BY op.rental_plan_id ORDER BY op.updated_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS higher_price
	FROM stg_api_production.rental_plans rp
	LEFT JOIN stg_api_production.old_prices op
		ON rp.id = op.rental_plan_id
	LEFT JOIN bi_ods.store s
	ON rp.store_id = s.id
	WHERE rp.active
	AND rp.store_id IN (1,4,5,126,128,49,618,621,622,626,627)
	),
	cte_rank_product AS
	(
	SELECT
		product_id,
		store_id,
		rank AS product_store_rank,
		ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY updated_at DESC) AS rank_event
	FROM s3_spectrum_rds_dwh_api_production.product_store_ranks
	),
	all_prices AS
	(
	SELECT
		rp.id,
		rp.active,
		rp.store_id,
		rp.created_at,
		rp.product_id,
		rp.updated_at,
		rp.item_condition_id,
		rp.rental_plan_price::DECIMAL(10,2) AS rental_plan_price,
		rp.minimum_term_months,
		s.store_name,
		s.store_label,
		op.higher_price::DECIMAL(10,2) AS higher_price,
		CASE
			WHEN higher_price IS NULL THEN FALSE
			ELSE TRUE
		END AS is_higher_price_available,
		CASE
			WHEN higher_price IS NOT NULL THEN ((higher_price::DECIMAL(10,2) - rental_plan_price::DECIMAL(10,2))/(higher_price::DECIMAL(10,2)))::DECIMAL(10,2)
			ELSE NULL
		END AS discount_plan,
		CONCAT(CONCAT(rp.rental_plan_price::DECIMAL(10,2), ' , ' )::TEXT, CASE WHEN op.higher_price::DECIMAL(10,2)::TEXT IS NOT NULL THEN op.higher_price::DECIMAL(10,2)::TEXT ELSE ' ' END ) AS rental_plan_price_higher_price,
		product_store_rank
	FROM stg_api_production.rental_plans rp
    LEFT JOIN bi_ods.store s
			ON
		rp.store_id = s.id
	LEFT JOIN old_prices op
			ON
		rp.id = op.id
	LEFT JOIN cte_rank_product rn
			ON
		rp.store_id = rn.store_id
		AND rp.product_id = rn.product_id
		AND rank_event = 1
	WHERE
		rp.active
		AND rp.store_id IN (1, 4, 5, 126, 128, 49, 618, 621, 622, 626, 627)
	)
	SELECT * FROM all_prices
;

BEGIN TRANSACTION;

DELETE FROM live_reporting.rental_plans WHERE 1=1;

INSERT INTO live_reporting.rental_plans
SELECT * FROM tmp_live_reporting_rental_plans;

END TRANSACTION;