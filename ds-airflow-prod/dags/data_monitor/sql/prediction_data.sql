WITH user_ids AS
    (SELECT op.user_id,
            op.model_id,
            op.score,
            op.approve_cutoff,
            op.decline_cutoff,
            op.creation_timestamp,
            ROW_NUMBER() OVER(PARTITION BY user_id
                              ORDER BY creation_timestamp ASC) AS ROW_NUMBER
     FROM fraud_and_credit_risk.order_predictions op
     WHERE model_id = {model_id}
         AND op.creation_timestamp < now() - '3 month'::INTERVAL )
SELECT user_ids.model_id,
       user_ids.score,
       user_ids.creation_timestamp
FROM user_ids
WHERE user_ids.ROW_NUMBER = 1;
