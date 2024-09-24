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
         AND op.creation_timestamp < now() - '3 month'::INTERVAL ),
     preds AS
    (SELECT cl."label" AS labels3,
            CASE
                WHEN u_id.score <= u_id.approve_cutoff THEN 'good'
                WHEN u_id.score >= u_id.decline_cutoff THEN 'fraud'
            END AS score_label,
            u_id.creation_timestamp
     FROM user_ids AS u_id
     LEFT JOIN order_approval.customer_labels3 cl ON u_id.user_id = cl.customer_id
     WHERE u_id.creation_timestamp < now() - '3 month'::INTERVAL
         AND cl."label" IN ('good',
                            'fraud')
         AND u_id.ROW_NUMBER = 1
     ORDER BY u_id.creation_timestamp DESC)
SELECT *
FROM preds
WHERE score_label IS NOT NULL;
