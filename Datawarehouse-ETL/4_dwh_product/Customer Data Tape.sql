CREATE TEMP TABLE snapshots_date 
(reference_date date);

INSERT INTO snapshots_date 
VALUES 
(dateadd(DAY,-1,dateadd(MONTH,-1,date_trunc('month',current_date)))),
(dateadd(DAY,-1,date_trunc('month',current_date))),
(current_date-1);


CREATE TEMP TABLE r_customer_data_tape_temp AS
WITH date_to_filter AS (
	SELECT reference_date
	FROM snapshots_date
)
,country_ AS (
	SELECT DISTINCT
			customer_id, 
			store_country,
			ROW_NUMBER() OVER (PARTITION BY customer_id
				ORDER BY created_date DESC) row_n
	FROM master.order_historical h
	INNER JOIN date_to_filter d 
	 ON d.reference_date = h."date" 
)
, customer_info AS (
	SELECT 
		d.reference_date,
		c.customer_id,
		c.customer_type,
		COALESCE(cp.gender, 'n/a') AS gender,
		datediff('day',cp.birthdate,d.reference_date)/365 AS age_at_reference,
		CASE
			WHEN age_at_reference < 18 THEN 'under 18'
			WHEN age_at_reference BETWEEN 18 AND 22 
				THEN '[18-22]'
			WHEN age_at_reference BETWEEN 23 AND 27 
				THEN '[23-27]'
			WHEN age_at_reference BETWEEN 28 AND 32 
				THEN '[28-32]'
			WHEN age_at_reference BETWEEN 33 AND 37 
				THEN '[33-37]'
			WHEN age_at_reference BETWEEN 38 AND 42 
				THEN '[38-42]'
			WHEN age_at_reference BETWEEN 43 AND 47
				THEN '[43-47]'
			WHEN age_at_reference BETWEEN 48 AND 52 
				THEN '[48-52]'
			WHEN age_at_reference BETWEEN 53 AND 57 
				THEN '[53-57]'
			WHEN age_at_reference BETWEEN 58 AND 62 
				THEN '[58-62]'
			WHEN age_at_reference >= 63 
				THEN '> 62'
			ELSE 'n/a'
		END age_group_at_reference,
		datediff('day',cp.birthdate,COALESCE(customer_acquisition_cohort,c.created_at))/365 AS age_at_acquisition,
		CASE
			WHEN age_at_acquisition < 18 THEN 'under 18'
			WHEN age_at_acquisition BETWEEN 18 AND 22 
				THEN '[18-22]'
			WHEN age_at_acquisition BETWEEN 23 AND 27 
				THEN '[23-27]'
			WHEN age_at_acquisition BETWEEN 28 AND 32 
				THEN '[28-32]'
			WHEN age_at_acquisition BETWEEN 33 AND 37 
				THEN '[33-37]'
			WHEN age_at_acquisition BETWEEN 38 AND 42 
				THEN '[38-42]'
			WHEN age_at_acquisition BETWEEN 43 AND 47
				THEN '[43-47]'
			WHEN age_at_acquisition BETWEEN 48 AND 52 
				THEN '[48-52]'
			WHEN age_at_acquisition BETWEEN 53 AND 57 
				THEN '[53-57]'
			WHEN age_at_acquisition BETWEEN 58 AND 62 
				THEN '[58-62]'
			WHEN age_at_acquisition >= 63 
				THEN '> 62'
			ELSE 'n/a'
		END age_group_at_acquisition,
		c.is_bad_customer,
		COALESCE(c.shipping_country ,c.signup_country , country_.store_country, 'n/a') AS shipping_country_,
		COALESCE(c.shipping_city, 'n/a') AS shipping_city,
		c.subscription_limit,
		CASE 
			WHEN country_.store_country != 'United States'
		    	THEN COALESCE(c.burgel_risk_category, 'n/a')
			ELSE COALESCE(fico.score_mapped::text, 'n/a')
		END AS risk_category,
		c.signup_language,
		c.email_subscribe,
		c.crm_label,
		CASE
			WHEN country_.store_country != 'United States'
	        	THEN c.created_at::date
			ELSE(to_char(c.created_at::date, 'YYYY-MM')|| '-01')::date
		END
		    AS register_date,
		c.customer_acquisition_cohort,
		c.clv
	FROM date_to_filter d
	INNER JOIN master.customer_historical c
	ON "date" = d.reference_date
	LEFT JOIN country_
	ON c.customer_id = country_.customer_id
	  AND country_.row_n = 1
	LEFT JOIN ods_data_sensitive.customer_pii cp
	ON c.customer_id = cp.customer_id
	LEFT JOIN david.fico_test fico
	ON c.customer_id = fico.customer_id
	  AND fico.rown = 1
)
,orders_base AS (
	SELECT
			d.reference_date,
			customer_id,
			o.order_rank,
			o.store_label,
			o.store_name,
			o.store_country,
			omc.marketing_source,
			device,
			o.created_date,
			o.order_id,
			o.paid_date
	FROM date_to_filter d
	INNER JOIN master.order_historical o
	ON o."date"  = d.reference_date
	LEFT JOIN ods_production.order_marketing_channel omc
	ON o.order_id = omc.order_id
	WHERE o.paid_date IS NOT NULL
)
,orders AS (
	SELECT DISTINCT
		t.reference_date,
		t.created_date,
		t.customer_id,
		t.store_label AS first_order_store_label,
		t.store_name AS first_order_store_name,
		t.store_country,
		t.marketing_source AS first_order_marketing_source,
		t.device AS first_order_device,
		ROW_NUMBER () OVER (PARTITION BY customer_id,reference_date
				ORDER BY paid_date ASC) AS paid_date_rank    
	FROM orders_base t
	WHERE t.order_id != 'R647088467'
)
,orders_final AS (
	SELECT 
		reference_date,
		customer_id,
		first_order_store_label,
		first_order_store_name,
		first_order_marketing_source,
		first_order_device,
		min( CASE
	        	WHEN store_country!= 'United States'
	        		THEN created_date::date
	        	ELSE(to_char(created_date::date, 'YYYY-MM')|| '-01')::date
	        END
		    ) AS first_order_date,
		min( CASE
	        	WHEN store_country!= 'United States'
	        		THEN created_date::date
	      		ELSE(to_char(created_date::date, 'YYYY-MM')|| '-01')::date
	       	END
		    ) AS first_order_date_paid
	FROM orders
	WHERE paid_date_rank = 1
	GROUP BY 1,2,3,4,5,6
)
, subscription_base AS (
	SELECT  
		d.reference_date,
		s.customer_id,
		s.subscription_id,
		s.subscription_value,
		s.created_date,
		s.rental_period,
		s.status,
		s.category_name,
		s.subcategory_name,
		s.product_sku,
		s.product_name,
		s.brand,
		s.start_date,
		s.rank_subscriptions,
		s.country_name
	FROM date_to_filter d
	INNER JOIN master.subscription_historical s
 	ON s."date" = d.reference_date
)
, payment_base AS (
	SELECT 
		d.reference_date,
		p.customer_id,
		c.customer_acquisition_cohort,
		p.due_date,
		p.amount_due		
	FROM date_to_filter d
	INNER JOIN master.payment_all_historical p
	ON p."date" = d.reference_date
	LEFT JOIN master.customer_historical c
	ON p.customer_id = c.customer_id
	 AND c."date" = d.reference_date
	WHERE payment_type != 'DEBT COLLECTION'
		AND c.customer_acquisition_cohort IS NOT NULL
		AND p.status NOT IN ('CANCELLED')
		AND NOT (p.payment_type = 'FIRST' AND p.status IN ('FAILED', 'FAILED FULLY')) 
		AND p.due_date::date < current_date-1
)
, payment AS (
	SELECT
		p.reference_date,
		p.customer_id,
		p.customer_acquisition_cohort,
		max(GREATEST(datediff(MONTH, p.customer_acquisition_cohort::date, p.due_date::date), 0)) AS months_between_acquisition_due,
		max(p.due_date) AS max_due_date,
		sum(p.amount_due) AS amount_due
	FROM payment_base p
	GROUP BY 1,2,3
)
, collection AS (
	SELECT
		DISTINCT 
			reference_date,
			customer_id,
			customer_acquisition_cohort,
			max_due_date,
			date_part('year', customer_acquisition_cohort) AS year_cohort,
			date_part('month', customer_acquisition_cohort) AS month_cohort,
			count (DISTINCT customer_id) AS customer_count,
			max(months_between_acquisition_due) AS max_months_between_acquisition,
			sum(amount_due) AS amount_due
	FROM payment
	GROUP BY 1,2,3,4,5
)	
, final_collection AS (
	SELECT
		reference_date,
		year_cohort,
		month_cohort,
		((sum(amount_due) / sum(customer_count))) AS earned_revenue_cohort,
		max(max_months_between_acquisition) AS max_months_between_acquisition_due,
		sum(customer_count) AS customer_count_cohort,
		sum(amount_due) AS amount_due_cohort
	FROM collection
	GROUP BY 1,2,3
)	
SELECT
	c.reference_date,
    c.customer_id,
	c.is_bad_customer,
	c.customer_type,
    c.age_at_reference,
    age_group_at_reference,
    c.age_at_acquisition,
    age_group_at_acquisition,
	shipping_country_,
	shipping_city,
	c.subscription_limit,
	risk_category,
	signup_language,
	email_subscribe,
	crm_label,
	register_date,
	clv,
--ORDER
	first_order_date,
	o.first_order_date_paid,
	o.first_order_device,
	o.first_order_store_label,
--SUBS
	subscription_id,
	subscription_value,
	created_date,
	rental_period,
	status,
	category_name,
	subcategory_name,
	product_sku,
	product_name,
	brand,
	start_date,
	rank_subscriptions,
	country_name,
--EARNED REVENUE
	COALESCE(c.customer_acquisition_cohort,
			 MIN(start_date) OVER (PARTITION BY c.customer_id),
			 register_date
	)::date AS customer_acquisition_cohort,
	co.max_months_between_acquisition_due,
  	co.customer_count_cohort,
	co.amount_due_cohort,
	co.earned_revenue_cohort,
--EARNED REVENUE BY CUSTOMER ID
		cc.max_months_between_acquisition,
		cc.amount_due
FROM customer_info c
LEFT JOIN orders_final o
 ON c.reference_date = o.reference_date
  AND c.customer_id = o.customer_id
LEFT JOIN subscription_base s
 ON s.reference_date = c.reference_date
  AND s.customer_id = c.customer_id
LEFT JOIN final_collection co
 ON (co.year_cohort = date_part('year', c.customer_acquisition_cohort)
  AND co.month_cohort = date_part('month', c.customer_acquisition_cohort))
  AND co.reference_date = c.reference_date
LEFT JOIN collection cc 
 ON cc.customer_id = c.customer_id
  AND cc.reference_date = c.reference_date
;


DELETE FROM dm_commercial.r_customer_data_tape 
WHERE reference_date IN 
						(
		 				SELECT max(reference_date)
						FROM dm_commercial.r_customer_data_tape
 						);

DELETE FROM dm_commercial.r_customer_data_tape WHERE reference_date IN (SELECT * FROM snapshots_date sd) 
	OR reference_date < dateadd(MONTH,-3,current_date);

INSERT INTO dm_commercial.r_customer_data_tape (SELECT * FROM r_customer_data_tape_temp);

DROP TABLE r_customer_data_tape_temp;

DROP TABLE snapshots_date;