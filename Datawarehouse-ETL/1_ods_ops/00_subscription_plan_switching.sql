drop table if exists ods_production.subscription_plan_switching;
create table ods_production.subscription_plan_switching as

with old_infra as (
	select distinct 
		"action" as upgrade_action,
		 subscription_id, 
		 "date",		 
		 cast(json_extract_path_text(previous_state,'minimum_term_months_was')||'->'||coalesce(case when json_extract_path_text(previous_state,'minimum_term_months_new')::text='' then null end 
		          ,minimum_term_months::text) as varchar(255)) as upgrade_path,
	     --before
		 case when json_extract_path_text(previous_state,'minimum_term_months_was')='' then null 
			 	else cast(json_extract_path_text(previous_state,'minimum_term_months_was') as decimal(5,2)) 
		 	end as duration_before,	 	
		 case when json_extract_path_text(previous_state,'amount_was') not like '%.%' then null
 				else cast(json_extract_path_text(previous_state,'amount_was') as decimal(5,2)) end as sub_value_before,		 				
		 (case when json_extract_path_text(previous_state,'minimum_term_months_was')='' then null 
			 	else cast(json_extract_path_text(previous_state,'minimum_term_months_was') as decimal(5,2)) end) 
			 * case when json_extract_path_text(previous_state,'amount_was') not like '%.%' then null
 				else cast(json_extract_path_text(previous_state,'amount_was') as decimal(5,2)) end as committed_sub_value_before,
		  --after
		 coalesce(case when json_extract_path_text(previous_state,'minimum_term_months_new')::text='' then null end 
		          ,minimum_term_months::text)::decimal(5,2) as duration_after,
		 coalesce(cast(json_extract_path_text(previous_state,'amount__c') as decimal(5,2)),price) as sub_value_after,
		 cast(coalesce(case when json_extract_path_text(previous_state,'minimum_term_months_new')::text='' then null end 
		          ,minimum_term_months::text) as decimal(5,2)) * cast(json_extract_path_text(previous_state,'amount__c') as decimal(5,2)) as committed_sub_value_after,
		 --delta
		 cast(json_extract_path_text(previous_state,'amount__c') as decimal(5,2)) -
		 	case when json_extract_path_text(previous_state,'amount_was') not like '%.%' then null
 				else cast(json_extract_path_text(previous_state,'amount_was') as decimal(5,2)) end as delta_in_sub_value,
		 coalesce(case when json_extract_path_text(previous_state,'minimum_term_months_new')::text='' then null end 
		          ,minimum_term_months::text)::decimal(5,2) * cast(json_extract_path_text(previous_state,'amount__c') as decimal(5,2)) -
		 		cast(json_extract_path_text(previous_state,'minimum_term_months_was') as decimal(5,2)) * 
		 		case when json_extract_path_text(previous_state,'amount_was') not like '%.%' then null
 					else cast(json_extract_path_text(previous_state,'amount_was') as decimal(5,2)) end  as delta_in_committed_sub_value,
		 --rank
		 row_number() over (partition by subscription_id order by date) as rank_events
	from stg_salesforce_events.subscription s
	where "action" in ('upgrade','amount-change')	
)	 	
 ,new_infra_parsing AS (
	SELECT DISTINCT
		event_timestamp,
		event_name,
		nullif(JSON_EXTRACT_PATH_text(payload,'id'),'') as id,
		json_extract_path_text(payload,'billing_terms', 'new', 'price') AS new_price,
		json_extract_path_text(payload,'billing_terms', 'old', 'price') AS old_price,
		json_extract_path_text(payload,'duration_terms', 'new', 'committed_length')::int AS new_committed_length,
		json_extract_path_text(payload,'duration_terms', 'old', 'committed_length')::int AS old_committed_length,
		json_extract_path_text(payload,'billing_terms', 'old', 'current_period', 'number')::int AS month_changed_committed_length
	FROM stg_kafka_events_full.stream_customers_contracts_v2
	WHERE event_name = 'extended'
)
, new_infra_rank AS (
	SELECT DISTINCT 
		event_name as upgrade_action, 
		coalesce(ssm.subscription_id, np.id) as subscription_id, 
		event_timestamp::timestamp as "date", 
		--before
		old_committed_length as duration_before,
		old_price::decimal(30,2) as sub_value_before,
		(duration_before * sub_value_before) as committed_sub_value_before, 
		--after
		new_committed_length - month_changed_committed_length as duration_after,
		new_price::decimal(30,2) as sub_value_after, 
		round((duration_after * sub_value_after),2) as committed_sub_value_after,
		--delta
		(sub_value_after - sub_value_before) as delta_in_sub_value,
		(committed_sub_value_after - committed_sub_value_before) as delta_in_committed_sub_value,
		--rank
		row_number () over (partition by np.id order by np.event_timestamp) as rank_events,
		--original value 
		s2.rental_period AS first_rental_period
	FROM new_infra_parsing np
	LEFT JOIN (SELECT subscription_id, subscription_bo_id, migration_date
	 			FROM ods_production.subscription s 
				WHERE s.migration_date IS NOT NULL) ssm
		ON np.id = ssm.subscription_bo_id
	LEFT JOIN ods_production.subscription s2
		ON coalesce(ssm.subscription_id, np.id) = s2.subscription_id
)
, new_infra_previous_duration AS (
	SELECT 
		upgrade_action,
		subscription_id,
		"date",
		--before
		duration_before,
		sub_value_before,
		committed_sub_value_before, 
		--after
		duration_after,
		sub_value_after, 
		committed_sub_value_after,
		--delta
		delta_in_sub_value,
		delta_in_committed_sub_value,
		--rank
		rank_events,
		--original value 
		first_rental_period,	
		lag(duration_after) OVER (PARTITION BY subscription_id ORDER BY rank_events) AS previous_duration_after
	FROM new_infra_rank 
)
SELECT 
	upgrade_action,
	subscription_id,
	"date",
	(CASE WHEN duration_before IN (1,3,6,12,18,24) THEN duration_before
		  WHEN rank_events = 1 AND first_rental_period IN (1,3,6,12,18,24) THEN first_rental_period
		  WHEN previous_duration_after IN (1,3,6,12,18,24) THEN previous_duration_after
		  ELSE duration_before END 
		 ||'->'|| duration_after) as upgrade_path,
	--before
	CASE WHEN duration_before IN (1,3,6,12,18,24) THEN duration_before
		 WHEN rank_events = 1 AND first_rental_period IN (1,3,6,12,18,24) THEN first_rental_period
		 WHEN previous_duration_after IN (1,3,6,12,18,24) THEN previous_duration_after
		 ELSE duration_before END AS duration_before,
	sub_value_before,
	committed_sub_value_before, 
	--after
	duration_after,
	sub_value_after, 
	committed_sub_value_after,
	--delta
	delta_in_sub_value,
	delta_in_committed_sub_value,
	--rank
	rank_events
FROM new_infra_previous_duration
--
UNION ALL 
--
SELECT DISTINCT 
 	upgrade_action, 
 	subscription_id, 
 	"date", 
 	upgrade_path,
 	--before
 	duration_before,
 	sub_value_before,
 	committed_sub_value_before,
 	--after,
	duration_after,
 	sub_value_after,
	committed_sub_value_after,
	--delta
	delta_in_sub_value,
	delta_in_committed_sub_value,
	--RANK 
	rank_events
FROM old_infra
	 ;

GRANT SELECT ON ods_production.subscription_plan_switching TO tableau;
