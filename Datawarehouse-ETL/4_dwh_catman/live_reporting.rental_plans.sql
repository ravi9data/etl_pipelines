
DROP TABLE IF EXISTS tmp_live_reporting_rental_plans;
CREATE TEMP TABLE tmp_live_reporting_rental_plans AS 

with old_prices as
	(
	select
		distinct
		rp.id,
		s.store_name,
		s.store_label,
		LAST_value(op.price) over (partition by op.rental_plan_id order by op.updated_at asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as higher_price
	from stg_api_production.rental_plans rp
	left join stg_api_production.old_prices op
		on rp.id = op.rental_plan_id
	left join bi_ods.store s
	on rp.store_id = s.id
	where rp.active
	and rp.store_id in (1,4,5,126,128,49,618,621,622,626,627)
	),
	cte_rank_product as
	(
	select
		product_id,
		store_id,
		rank as product_store_rank,
		row_number() over (partition by product_id, store_id order by updated_at desc) as rank_event
	from s3_spectrum_rds_dwh_api_production.product_store_ranks
	),
	all_prices as
	(
	SELECT
		rp.id,
		rp.active,
		rp.store_id,
		rp.created_at,
		rp.product_id,
		rp.updated_at,
		rp.item_condition_id,
		rp.rental_plan_price::decimal(10,2) as rental_plan_price,
		rp.minimum_term_months,
		s.store_name,
		s.store_label,
		op.higher_price::decimal(10,2) as higher_price,
		CASE
			WHEN higher_price IS NULL THEN FALSE
			ELSE TRUE
		END AS is_higher_price_available,
		CASE
			WHEN higher_price IS NOT NULL THEN ((higher_price::decimal(10,2) - rental_plan_price::decimal(10,2))/(higher_price::decimal(10,2)))::decimal(10,2)
			ELSE NULL
		END AS discount_plan,
		concat(concat(rp.rental_plan_price::decimal(10,2), ' , ' )::text, CASE WHEN op.higher_price::decimal(10,2)::text IS NOT NULL THEN op.higher_price::decimal(10,2)::text ELSE ' ' END ) AS rental_plan_price_higher_price,
		product_store_rank
	from stg_api_production.rental_plans rp
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
	select * from all_prices
;

BEGIN TRANSACTION;

DELETE FROM live_reporting.rental_plans WHERE 1=1;

INSERT INTO live_reporting.rental_plans
SELECT * FROM tmp_live_reporting_rental_plans;

END TRANSACTION;
