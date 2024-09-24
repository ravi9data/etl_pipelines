delete from ods_data_sensitive.credit_bureau_spain_shipaddress
where snapshot_date = current_date;

INSERT
	INTO
	ods_data_sensitive.credit_bureau_spain_shipaddress
WITH address_history AS(
	SELECT
		ad.user_id,
		COALESCE(ad.phone, ad.alternative_phone) phone,
		ad.address1,
		ad.address2,
		ad.city,
		ad.zipcode,
		CASE
			WHEN so.state = 'paid' THEN 1
		ELSE 0
	END AS state_new,
		ROW_NUMBER() OVER (PARTITION BY ad.user_id ORDER BY state_new DESC, so.created_at DESC) AS address_desc_index
	FROM stg_api_production.spree_addresses ad
		JOIN stg_api_production.spree_orders so
			ON	so.ship_address_id = ad.id
	WHERE ad.country_id = (SELECT id FROM stg_api_production.spree_countries sc WHERE iso ='ES' LIMIT 1)
	and ad.user_id not in (1516539, 1305602, 1374620)  -- excluded customers with stolen identity
)
,_nornalized_address AS (
	SELECT
		*,ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY updated_date DESC) AS rn
	FROM
SELECT
	DISTINCT 
	s.customer_id,
	s.order_id,
	s.subscription_id,
	--COALESCE(s.subscription_bo_id,s.subscription_id) AS subscription_id,
	cf.dpd,
	CASE
		WHEN cf.dpd > 180 THEN '6'
		ELSE CEIL(cf.dpd / 30::NUMERIC - 1)::varchar
	END AS payment_situation,
	s.start_date::date,
	GREATEST(s.minimum_cancellation_date, last_billing_period_start)::date AS end_date,
	cf.default_date::date AS first_unpaid_due_date,
	last_billing_period_start::date AS last_unpaid_due_date,
	cf.failed_subscriptions AS unpaid_instalments_count,
	cf.outstanding_subscription_revenue AS unpaid_instalments_amount,
	cp.first_name AS customer_firstname,
	cp.last_name AS customer_lastname,
	cp.birthdate::date AS customer_birthdate,
	pi.identification_number AS customer_document_number,
	CASE
		WHEN pi.identification_sub_type = 'dni' THEN '01'
		WHEN pi.identification_sub_type = 'nie' THEN '02'
		ELSE NULL
	END AS customer_document_type,
	'724' AS customer_document_country,
	--ISO-3166-1 code for Spain,
	COALESCE(ad.phone, cp.phone_number) AS customer_phone,
	ad.address1 AS shipping_address_street ,
	ad.address2 AS shipping_house_number,
	ad.city AS shipping_city,
	'Spain' AS shipping_country,
	ad.zipcode AS shipping_zipcode,
	na.tipo_via_norm,
	na.particulas_norm,
	na.via_norm,
	na.numero,
	na.numero_norm,
	na.nexo_norm,
	na.localidad_norm,
	na.municipio_norm,
	na.provincia_norm,
	na.codpostal_norm,
	na.id1 IS NOT NULL AS lastest_address_normalized,
	COALESCE(na.id1, na_any.id1) IS NOT NULL AS any_normalized_address,
	current_date AS snapshot_date
FROM
	ods_production.subscription s
INNER JOIN ods_production.subscription_cashflow cf 
	ON
	s.subscription_id = cf.subscription_id
INNER JOIN address_history ad
	ON
	ad.user_id = s.customer_id
	AND address_desc_index = 1
LEFT JOIN ods_production.subscription_assets sa 
      ON
	s.subscription_id = sa.subscription_id
INNER JOIN ods_data_sensitive.customer_pii cp 
	ON
	s.customer_id = cp.customer_id
	AND cp.customer_type = 'normal_customer'
LEFT JOIN stg_api_production.personal_identifications pi 
    ON
	cp.customer_id = pi.user_id
LEFT JOIN _nornalized_address na
	ON
	na.id1 = s.customer_id
	AND trim(ad.address1) = trim(na.direccion)
	AND trim(ad.address2) = trim(na.numero)
	AND rn =1
LEFT JOIN (SELECT DISTINCT id1 FROM _nornalized_address) na_any 
	ON
	na_any.id1 = s.customer_id
WHERE
	s.country_name = 'Spain'
	AND COALESCE(sa.first_asset_delivered, s.first_asset_delivery_date) IS NOT NULL --To Eliminate the non-delivered subscriptions
	AND cf.dpd > 30  /*to consider only subs with 30+ dpd */
	AND cf.failed_subscriptions > 0	--To eliminate the non-failed subscriptions
ORDER BY
	last_unpaid_due_date DESC;
