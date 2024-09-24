SELECT
	t.serialnumber AS "Serial Number",
	t.status AS "Status",
	t."name" AS "Asset Name",
	t.f_product_sku_variant__c AS "Product SKU Variant",
	p.ean__c AS "EAN",
	t.weight__c AS "Weight (kg)",
	t.height__c AS "Height (cm)",
	t.length__c AS "Length (cm)",
	t.width__c AS "Width (cm)",
	t.cost_price__c AS "Purchase Price"
FROM stg_salesforce.asset t
LEFT JOIN stg_salesforce.product2 p
ON t.f_product_sku_variant__c = p.sku_variant__c
WHERE t.warehouse__c ='ups_softeon_eu_nlrng'
AND p.isactive
AND t.status NOT IN ('RESERVED', 'CROSS SALE', 'ON LOAN', 'IN DEBT COLLECTION',
'SOLD','LOST', 'OFFICE', 'RETURNED TO SUPPLIER', 'DELETED', 'LOCKED DEVICE',
'WAITING FOR REFURBISHMENT', 'SENT FOR REFURBISHMENT', 'WARRANTY', 'IRREPARABLE',
'RECOVERED','SELLING', 'WRITTEN OFF DC', 'WRITTEN OFF OPS', 'INVESTIGATION CARRIER', 'INSOLVENCY', 'DONATED'
)
