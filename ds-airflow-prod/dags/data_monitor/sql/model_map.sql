SELECT id,
       file_path,
       country_code
FROM fraud_and_credit_risk.prediction_models pm
WHERE customer_type = 'normal'
    AND id NOT IN (18, -- these are dummy models for default model
                   19);
