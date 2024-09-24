WITH feature_list AS
    (SELECT model_id,
            (final_data->'data'->0)::json AS features
     FROM fraud_and_credit_risk.order_predictions op
     WHERE model_id = {model_id}
         AND final_data::TEXT != 'null'
     ORDER BY creation_timestamp DESC
     LIMIT 1)
SELECT jsonb_agg(d.key) AS feats
FROM feature_list
JOIN json_each_text(feature_list.features) d ON TRUE;
