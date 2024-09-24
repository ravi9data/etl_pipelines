WITH user_attributes AS
(SELECT
	DISTINCT s.customer_id,
    c.updated_at,
	c.active_subscriptions,
	subscription_id,
	s.subscription_sf_id,
	s.minimum_term_months,
	s.paid_subscriptions,
	s.product_name,
    s.rental_period,
    s.variant_sku,
	s.start_date,
	s.minimum_cancellation_date::DATE,
	(s.minimum_cancellation_date::DATE - CURRENT_DATE::DATE) AS days_until_cancellation,
	(s.rental_period::TEXT + '_' + s.rank_subscriptions::TEXT) AS rental_rank
FROM master.subscription s
	LEFT JOIN master.customer c
	ON s.customer_id = c.customer_id
WHERE s.status ='ACTIVE'
	ORDER BY s.rank_subscriptions DESC ),

	min_days AS
(SELECT
	customer_id,
	CASE WHEN MIN(days_until_cancellation) < 0 THEN 'yes' ELSE 'no' END AS customers_extended_subs,
	min(days_until_cancellation) AS closest_day_count
FROM user_attributes a
	GROUP BY 1)

SELECT
	a.customer_id AS external_id,
	listagg(product_name, ', ') AS almost_ending_subscription_names,
	listagg(subscription_id,', ') AS almost_ending_subscription_ids,
	listagg(rental_rank,', ') AS almost_ending_subscription_periods,
    listagg(variant_sku,', ') AS almost_ending_subscription_variant_skus,
	count(subscription_id) AS same_day_ending_subs_count,
	min(minimum_cancellation_date) AS closest_minimum_cancellation_date,
	min(active_subscriptions) AS active_subscriptions
FROM user_attributes a
	LEFT JOIN min_days m
	ON a.customer_id = m.customer_id
WHERE customers_extended_subs ='no' --this is the logic that excludes customers that continued renting past their cancellation date
AND CASE WHEN a.rental_period = 1 THEN a.minimum_cancellation_date::DATE BETWEEN CURRENT_DATE  AND CURRENT_DATE + 30
         ELSE a.minimum_cancellation_date::DATE BETWEEN CURRENT_DATE AND CURRENT_DATE + 35 END
AND (a.updated_at) >= CURRENT_TIMESTAMP - INTERVAL '{interval_hour}'
GROUP BY 1
ORDER BY closest_minimum_cancellation_date;
