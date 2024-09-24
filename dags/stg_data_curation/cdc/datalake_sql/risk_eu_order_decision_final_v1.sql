SELECT
	updated_at,
	outcome_namespace,
	outcome_message,
	published_at,
	created_at,
	store_country_iso,
	order_id,
	customer_id,
	outcome_timeout_days,
	json_extract_scalar(collected_data,	'$.rule_tournament.deyde_identity.degree_of_resemblance')
	    AS deyde_identity_degree_of_resemblance,
	json_extract_scalar(collected_data,	'$.rule_tournament.deyde_identity.registered')
	    AS deyde_identity_registered,
    year||month||day||hour AS partition_col
FROM
	"data_production_kafka_topics_raw"."risk_eu_order_decision_final_v1"
