-- Ordering new_infra decision_result
DROP TABLE IF EXISTS decision_result;
CREATE TEMP TABLE decision_result
SORTKEY(order_id)
DISTKEY(order_id)
as
with cte as (
SELECT distinct
    created_at,
    updated_at,
    order_id,
    amount,
    code,
    message,
    ROW_NUMBER () OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS row_num
FROM
    stg_order_approval.decision_result
WHERE
    LEFT(order_id, 1) != 'R')
SELECT distinct
    created_at,
    updated_at,
    order_id,
    amount,
    code,
    message
FROM
    cte
WHERE
    row_num=1;

-- Merging olda and new infra decisions
DROP TABLE IF EXISTS order_approval_results;
CREATE TEMP TABLE order_approval_results
SORTKEY(order_id)
DISTKEY(order_id)
AS
SELECT distinct
    created_at AS creation_timestamp,
    updated_at,
    order_id,
    amount AS credit_limit,
    '' AS decision_action, --deprecated field
    code AS decision_code,
    message AS comments
FROM
    decision_result
union
select creation_timestamp,updated_at,order_id,credit_limit,decision_action,decision_code,comments
from(
SELECT distinct
    creation_timestamp::timestamp creation_timestamp,
    updated_at::timestamp as updated_at,
    order_id,
    round(credit_limit,0)::int as credit_limit,
    decision_action,
    round(decision_code,0)::int as decision_code,
    comments,
    row_number() over (partition by order_id order by cast(updated_at as timestamp) desc) as idx
FROM stg_curated.order_approval_results)
where idx =1
-- US risk Data --
union
select distinct creation_timestamp,updated_at,order_id,credit_limit,decision_action,decision_code,comments
from (
select  distinct
    cast(created_at as timestamp) AS creation_timestamp,
    cast(updated_at as timestamp) updated_at,
    order_id,
    cast(amount as Int) AS credit_limit,
    '' AS decision_action, --deprecated field
    cast(code as Int) AS decision_code,
    message AS comments,
    row_number() over (partition by order_id order by cast(updated_at as timestamp) desc) as idx
FROM
    stg_curated.risk_internal_us_risk_decision_result_v1)
   where idx =1
;


-- Order_sub Temp TABLE Creation

DROP TABLE IF EXISTS order_sub;
CREATE TEMP TABLE order_sub
SORTKEY(order_id)
DISTKEY(order_id)
AS
WITH customer_pre2 AS (
        SELECT distinct
            o.order_id,
            o.submitted_date,
            dt.main_reason,
            dt.id,
            dt.customer_id,
            ABS(DATEDIFF('minute', o.submitted_date::timestamp, dt.creation_timestamp::timestamp)) AS time_diff,
            MIN(time_diff) OVER (PARTITION BY o.order_id) AS closest
        FROM ods_production.order AS o
        LEFT JOIN stg_curated.decision_tree_algorithm_results AS dt ON o.customer_id = dt.customer_id
        WHERE time_diff IS NOT NULL
        ),
     customer_pre1 AS (
        SELECT distinct
            order_id,
            main_reason,
            id AS score_id,
            customer_id,
            RANK() OVER (PARTITION BY order_id ORDER BY score_id) AS final_rank
        FROM customer_pre2
        WHERE time_diff = closest
        ),
     customer AS (
        SELECT
            *
        FROM customer_pre1
        WHERE final_rank = 1
        )
        SELECT distinct
            o.customer_id AS user_id,
            o.order_id,
            o.created_date AS order_created_at,
            UPPER(o.status) AS order_Status,
            CASE
                WHEN r.decision_action = 'decline' AND o.status = 'PAID' THEN True
                ELSE False
            END AS approved_manually,
            r.creation_timestamp AS order_scored_At,
            MAX(r.creation_timestamp) OVER (PARTITION BY o.order_id) AS last_scoring_date,
            r.decision_action AS order_scoring_decision,
            r.credit_limit, --sub_limit from this order scoring result
            t.main_reason AS scoring_reason,
            r.comments AS order_scoring_comments,
            r.decision_code,
            CASE
                WHEN r.decision_code = '1000' THEN 'Model Approved'
                WHEN r.decision_code = '2000' THEN 'Model Declined'
                WHEN r.decision_code = '3010' THEN 'MR Approved'
                WHEN r.decision_code = '3020' THEN 'MR Declined'
                ELSE 'Others'
            END AS decision_code_label,
            cd.subscription_limit, --sub_limit before this order
            round(cd.payments,0)::int as payments,
            round(cd.recurrent_failed_payments,0)::int as recurrent_failed_payments,
            round(cd.first_failed_payments,0)::int as first_failed_payments,
            cd.pending_value,
            o.voucher_code,
            ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY r.creation_timestamp DESC ) row_num
        FROM ods_production.order AS o
        LEFT JOIN order_approval_results AS r ON o.order_id = r.order_id
        LEFT JOIN stg_curated.customer_data AS cd ON o.order_id = cd.order_id
        LEFT JOIN customer AS t ON o.order_id = t.order_id
        WHERE (r.creation_timestamp IS NOT NULL OR cd.creation_timestamp IS NOT NULL);

-- Main Table Creation
DROP TABLE IF EXISTS ods_production.order_scoring;

CREATE TABLE ods_production.order_scoring
SORTKEY(order_id)
DISTKEY(order_id)
AS
        --- NOTE BELOW : ORDER ID - SCORING MODEL ATTRIBUTION LOGIC (See ticket BI-8363 for explanatory Analysis)
        --- Choice 1. Where we have a Ground Truth Model ID (from Order Final Decision tables) we take that. 
       		-- Ground Truth updates May 2024 (BI-8457)
       			-- Legacy and new infra (ml fraud) sections of order final decision collected_data payloads made available by DENG in new columns. Extracting from those with preference for ml_fraud.
       			-- Grover pulled out of US, where there was only one model developed (22), so no need for elaborate model picking logic (removing US ground truth section).
        --- Choice 2. (Fallback 1) If the Order is a German Order submitted from October 2024 onwards, we take the MIN Model ID (i.e. Model 38 instead of 41 ),
        --- Choice 3. (Fallback 2) For all remaining orders, we take the latest Model scored.
        WITH legacy_tags AS 
		(
		SELECT
			order_id,
			SPLIT_PART ( 
				REPLACE(REPLACE(REPLACE(
					JSON_EXTRACT_PATH_TEXT( legacy, 'risk_tags') 
				, '[', ''),']', ''),'"', ''), -- remove square brackets AND quotation marks
			',', 1 ) model_id, -- take FIRST POSITION value
			ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at DESC ) row_num
		FROM stg_curated.risk_eu_order_decision_final_v1_unnested
		WHERE outcome_is_final = 'True'
			AND model_id ~ '^[0-9]' -- NUMERIC VALUES only
		)
		, ground_truth_models AS -- Choice 1
		(
		SELECT
			f.order_id,
			COALESCE( JSON_EXTRACT_PATH_TEXT( ml_fraud, 'model_id') , l.model_id) model_id, -- We take NEW infra MODEL id WHERE possible, AND IF NOT, we take FROM legacy payload. 
			ROW_NUMBER() OVER (PARTITION BY f.order_id ORDER BY f.created_at, f.updated_at DESC ) row_num -- duplicates exist IN base TABLE .. see example ORDER id 'R662767704' 
		FROM stg_curated.risk_eu_order_decision_final_v1_unnested f -- We take EU ONLY, see explanatory notes above.
		LEFT JOIN legacy_tags l
			ON l.order_id = f.order_id
				AND l.row_num = 1
		WHERE f.outcome_is_final = 'True'
		)
		, prediction_models_cleaned AS
        (
         SELECT DISTINCT ID, file_path
         FROM stg_curated.prediction_models 
        )
        , scoring_predictions_raw AS
        (
        SELECT DISTINCT
            op.order_id,
            pm.id::int as scoring_model_id,
            gt.model_id AS scoring_model_id_verification, -- Verification field. IF NOT NULL THEN use this TO FILTER.
            op.score::numeric(18,8),
            op.approve_cutoff::numeric(14,4),
            op.decline_cutoff::numeric(14,4),
            op.creation_timestamp::timestamp,
            pm.file_path,
            CONCAT('Model ', pm.id::text) AS model_name,
            CASE WHEN COUNT(gt.order_id) OVER (PARTITION BY op.order_id) >= 1 THEN 1 ELSE 0 END AS match_exists_for_order,
          	ROW_NUMBER() OVER (PARTITION BY op.order_id, op.model_id, op.creation_timestamp, op.score) row_num -- remove duplicate predictions
        FROM oltp_risk_ml.order_predictions AS op
        INNER JOIN prediction_models_cleaned AS pm -- This INNER JOIN removes MODEL 0 which appears TO be a fallback OUTPUT. 
            ON op.model_id = pm.id
        LEFT JOIN ground_truth_models gt
        	ON gt.order_id = op.order_id
        		AND gt.model_id = op.model_id
        			AND gt.row_num = 1
        )
        , validated_orders_raw AS
        (
        SELECT order_id, scoring_model_id, score, approve_cutoff, decline_cutoff, creation_timestamp, file_path, model_name, 
        	-- An order can be scored by the same Model multiple times, so we will take the latest one (scores should be near-identical anyway)
        	ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY creation_timestamp DESC) row_num
        FROM scoring_predictions_raw
        WHERE match_exists_for_order = 1
            AND scoring_model_id::VARCHAR(55) = scoring_model_id_verification::VARCHAR(55) -- WHERE possiblity FOR ground truth FOR ORDER EXISTS, take the correct model_id.
        )
        , fall_back_allocations AS
        (
        SELECT sp.order_id, sp.scoring_model_id, sp.score, sp.approve_cutoff, sp.decline_cutoff, sp.creation_timestamp, sp.file_path, sp.model_name,
        	CASE 
	        	WHEN o.store_country = 'Germany'
    				AND o.submitted_date::date >= '2023-10-01'
    					THEN 'Fallback 1' -- See description above.
    			ELSE 'Fallback 2'
        	END AS fallback_option,
        	ROW_NUMBER() OVER (PARTITION BY sp.order_id ORDER BY sp.scoring_model_id::INT ASC ) fallback_1_row_num,
        	ROW_NUMBER() OVER (PARTITION BY sp.order_id ORDER BY sp.creation_timestamp DESC ) AS fallback_2_row_num
        FROM scoring_predictions_raw sp
        INNER JOIN ods_production."order" o
        	ON o.order_id = sp.order_id
        WHERE match_exists_for_order = 0
        )
        , final_model_scores AS
        (
        -- Ground truth scores
        SELECT order_id, scoring_model_id, score, approve_cutoff, decline_cutoff, creation_timestamp, file_path, model_name
        FROM validated_orders_raw
        WHERE row_num = 1
        UNION
        -- Fallback scores
        SELECT order_id, scoring_model_id, score, approve_cutoff, decline_cutoff, creation_timestamp, file_path, model_name
        FROM fall_back_allocations
        WHERE 
        	( fallback_option = 'Fallback 1' AND fallback_1_row_num = 1 )
        		OR 
        		( fallback_option = 'Fallback 2' AND fallback_2_row_num = 1 )
        )
     , sub_limit AS (
        SELECT distinct
            user_id,
            DATE_TRUNC('day', created_at::timestamp) AS created_date,
            LAST_VALUE(old_subscription_limit) OVER (PARTITION BY user_id, DATE_TRUNC('day', created_at::timestamp) ORDER BY DATE_TRUNC('day', created_at::timestamp) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_subscription_limit,
            LAST_VALUE(new_subscription_limit) OVER (PARTITION BY user_id, DATE_TRUNC('day', created_at::timestamp) ORDER BY DATE_TRUNC('day', created_at::timestamp) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_subscription_limit
        FROM stg_curated.spree_users_subscription_limit_log
        ),
     limit_agg AS (
        SELECT
            *
        FROM sub_limit
        GROUP BY 1,2,3,4
        ),
     anomaly_score AS (
        SELECT distinct
            o.order_id,
            so.anomaly_score::numeric(16,8)
        FROM ods_production."order" AS o
        LEFT JOIN stg_curated.anomaly_score_order AS so ON o.order_id = so.order_id
        ),
     payment_risk AS (
        SELECT distinct
            order_id,
            payment_category_fraud_rate,
            payment_category_info
        FROM stg_curated.payment_risk_categories
        ),
     assets_risk AS (
        SELECT
            DISTINCT order_id,
            MAX(numeric_fraud_rate::numeric(4,3)) AS cart_numeric_fraud_rate,
            CASE
                WHEN cart_numeric_fraud_rate < 0.05 THEN '<5%'
                WHEN cart_numeric_fraud_rate < 0.1 THEN '>5%'
                WHEN cart_numeric_fraud_rate < 0.2 THEN '>10%'
                WHEN cart_numeric_fraud_rate < 0.5 THEN '>20%'
                ELSE '>50%'
            END AS cart_risk_fraud_rate,
            LISTAGG(asset_risk_info, ' /n ') AS cart_risk_info
        FROM stg_curated.asset_risk_categories_order
        GROUP BY order_id
        ),
     similar_orders AS (
        select
            order_id,
            customers_count::int,
            top_scores,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY extracted_at DESC ) AS row_num -- take LAST extracted
        FROM stg_curated.order_similarity_result
        ),
     fmfv AS (
        SELECT distinct
            code,
            True AS first_month_free_voucher
        FROM stg_external_apis.first_month_free_voucher
        ),
     onfido AS (
        SELECT distinct
            order_id,
            verification_state,
            "trigger",
            is_processed,
            "data" as data_json,
            CASE
                WHEN "trigger" = 1
                    THEN 'Prepaid Card'
                WHEN "trigger" = 2
                    THEN 'Unverified PayPal'
                WHEN "trigger" = 3
                    THEN 'FFP'
                WHEN "trigger" = 6 --Deprecated
                    THEN 'Above 400'
                WHEN "trigger" IN (4, 5, 7)
                    THEN 'High Risk Assets'
                WHEN "trigger" = 8
                    THEN 'Onfido Bonus'
                WHEN "trigger" = 9
                    THEN 'Manual Request'
                WHEN "trigger" = 10
                    THEN 'Multiple phones'
                WHEN "trigger" = 11
                    THEN 'Multiple sessions'
                WHEN "trigger" = 12
                    THEN 'Spain'
                WHEN "trigger" = 13
                    THEN 'Test'
                WHEN "trigger" = 14
                    THEN 'DE High Risk'
                WHEN "trigger" = 20
                    THEN 'Indirect'
                WHEN "trigger" = 100
                    THEN 'No trigger data available'
                ELSE NULL
            END AS onfido_trigger,
            row_number() over (partition by order_id order by updated_at desc) as row_n
        FROM stg_curated.id_verification_order
        )
SELECT distinct
   o.user_id,
   o.order_id,
   o.order_created_at,
   o.order_status,
   o.approved_manually,
   o.order_scored_at,
   o.last_scoring_date,
   o.order_scoring_decision,
   o.credit_limit,
   o.scoring_reason,
   o.order_scoring_comments,
   o.decision_code,
   o.decision_code_label,
   o.subscription_limit,
   o.payments,
   o.recurrent_failed_payments,
   o.first_failed_payments,
   o.pending_value,
   c.scoring_model_id,
   c.score,
   cs.schufa_class,
   cs.schufa_score,
   la.old_subscription_limit,
   la.new_subscription_limit,
   ans.anomaly_score AS anomaly_score,
   pr.payment_category_fraud_rate,
   pr.payment_category_info,
   ar.cart_risk_fraud_rate,
   ar.cart_risk_info,
   c.approve_cutoff,
   c.decline_cutoff,
   c.creation_timestamp,
   c.file_path,
   c.model_name,
   so.customers_count AS similar_customers_count,
   so.top_scores AS top_similar_scores,
   fmfv.first_month_free_voucher,
   onf.onfido_trigger,
   onf.verification_state,
   onf.is_processed
FROM order_sub AS o
LEFT JOIN final_model_scores AS c ON o.order_id = c.order_id
LEFT JOIN ods_production.customer_scoring AS cs ON o.user_id = cs.customer_id
LEFT JOIN limit_agg AS la ON o.user_id = la.user_id
                         AND DATE_TRUNC('day', o.order_scored_At) = la.created_date
LEFT JOIN anomaly_score AS ans ON o.order_id = ans.order_id
LEFT JOIN payment_risk AS pr ON o.order_id = pr.order_id
LEFT JOIN assets_risk AS ar ON o.order_id = ar.order_id
LEFT JOIN similar_orders AS so 
	ON o.order_id = so.order_id 
		AND so.row_num = 1
LEFT JOIN fmfv ON o.voucher_code = fmfv.code
LEFT JOIN onfido AS onf ON o.order_id = onf.order_id
    and row_n = 1
WHERE o.row_num = 1
ORDER BY o.order_created_at DESC
; 


grant select on ods_production.order_scoring TO group risk_users,risk_users_redash;

GRANT SELECT ON ods_production.order_scoring TO tableau;
