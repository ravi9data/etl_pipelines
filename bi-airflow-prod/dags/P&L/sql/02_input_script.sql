DROP TABLE IF EXISTS finance.profit_and_loss_input;

CREATE TABLE finance.profit_and_loss_input AS 
SELECT 
dimension
,date::date AS date_
,CASE WHEN cost_entity = 'Germany' AND customer_type = 'normal_customer' THEN 'B2C-Germany'
      WHEN cost_entity = 'Germany' AND customer_type = 'business_customer' THEN 'B2B-Germany'
      --WHEN cost_entity = 'US' THEN 'United States'
      ELSE cost_entity 
    END AS cost_entity
,REPLACE(amount,',','')::DECIMAL(22,4) AS amount
FROM staging_airbyte_bi.p_l_import_sheet plis 
;


GRANT SELECT ON finance.profit_and_loss_input TO tableau;