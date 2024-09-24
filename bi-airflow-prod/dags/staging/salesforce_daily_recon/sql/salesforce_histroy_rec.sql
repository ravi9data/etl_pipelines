INSERT INTO stg_salesforce.asset_history
SELECT a.id, a.isdeleted , a.assetid, a.createdbyid, a.createddate, a.field, a.oldvalue , a.newvalue  
FROM staging.assethistory  a
WHERE id NOT IN (SELECT id FROM stg_salesforce.asset_history );

-- ---------------------------------------------------------------

INSERT INTO stg_salesforce.customer_asset_allocation__history
SELECT id,isdeleted,parentid,createdbyid,createddate,field,oldvalue,newvalue FROM staging.customer_asset_allocation__history
WHERE id NOT  IN (
SELECT id FROM stg_salesforce.customer_asset_allocation__history);
