--Grant Select on tables
--1. Select Group
GRANT SELECT ON ALL TABLES IN SCHEMA dwh TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA master TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_production TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_external TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_spv_historical TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA stg_salesforce_events TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA stg_events TO GROUP select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA  stg_fraud_and_credit_risk to group select_group;
GRANT SELECT ON ALL TABLES IN SCHEMA  web to group select_group;

--2. Privacy Group
GRANT SELECT ON ALL TABLES IN SCHEMA stg_realtimebrandenburg TO  group privacy_group;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_data_sensitive TO  group privacy_group;
GRANT SELECT ON ALL TABLES IN SCHEMA stg_salesforce TO  group privacy_group;


--3. Dev Group
GRANT SELECT ON ALL TABLES IN SCHEMA master TO GROUP dev;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_production TO GROUP dev;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_external TO GROUP dev;
GRANT SELECT ON ALL TABLES IN SCHEMA dwh TO GROUP dev;
GRANT SELECT ON ALL TABLES IN SCHEMA ods_spv_historical TO GROUP dev;
GRANT SELECT ON ALL TABLES IN SCHEMA stg_events TO GROUP dev;
GRANT SELECT ON ALL TABLES IN SCHEMA stg_salesforce_events TO GROUP dev;

--4. Special Permissions
GRANT SELECT ON ALL TABLES IN SCHEMA "atomic" TO jason;


