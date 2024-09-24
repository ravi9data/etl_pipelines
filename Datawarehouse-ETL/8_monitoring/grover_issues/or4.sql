--- Monitoring Not returend Original Asset after replacement is delivered 
drop table if exists monitoring.or4;
CREATE TABLE  monitoring.or4 as 
with wemalo_checks AS (
	SELECT 
		reporting_date,
		asset_id,
		seriennummer 
	FROM 
		dwh.wemalo_sf_reconciliation wsr )
,allocation_pre as (
    select o.allocation_id,
        o.asset_id,
        case when o.allocation_sf_id is null then 'New Infra'
		 else 'Legacy' end as infra,
        a.residual_value_market_price,
        a.amount_rrp,
        o.serial_number,
        --to identify if the customer still keeps the old asset
        --then we need to filter only delivered shipments out.
        case
            when o.delivered_at is not null then 'Delivered'
            else 'Not Delivered'
        end as is_delivered,
        o.customer_id,
        o.order_id,
        o.subscription_id,
        s.subscription_value,
        s.status,
        o.allocation_status_original,
        a.asset_status_original,
        o.asset_status,
        o.is_recirculated,
        o.allocated_at,
        o.shipment_at,
        o.delivered_at,
        wc.reporting_date AS  warehouse_return_date,
        ROW_NUMBER () OVER (PARTITION BY seriennummer ORDER BY  reporting_date ) idx,
        a.initial_price,
        o.replacement_date,
        n.shipment_at replaced_shipment_at,
        n.delivered_at replaced_delivered_at,
        o.replacement_reason,
        o.replaced_by AS replaced_allocation,
        n.asset_id AS replaced_asset,
        o.return_shipment_id,
        o.return_shipment_tracking_number,
        o.return_shipment_label_created_at,
        o.return_shipment_at,
        o.return_delivery_date,
        o.return_delivery_date_old
    from ods_production.allocation o
        left join master.asset a on o.asset_id = a.asset_id
        Left join master.subscription s ON s.subscription_id=o.subscription_id  
        left join ods_production.allocation n on o.replaced_by = n.allocation_id
        LEFT JOIN wemalo_checks wc ON o.serial_number=wc.seriennummer AND o.delivered_at<wc.reporting_date 
        where o.replacement_date is not null 
        AND o.return_delivery_date IS NULL 
         )
,allocation AS (
SELECT *
FROM allocation_pre WHERE
warehouse_return_date IS NULL OR 
idx=1        
)
--check when asset back in the stock
--since for some allocation return timestamps are not available 
--even if they are back in the wh.
--classify each day per shipment date, so we'll be able to detect if asset has returned 
,wemalo as (
    select w.seriennummer,
        w.reporting_date,
        case
            when r.shipment_at < w.reporting_date then row_number() over (
                partition by w.seriennummer,
                case
                    when r.shipment_at > w.reporting_date then 1
                    else null
                end
                order by w.reporting_date asc
            )
        end rn
    from stg_external_apis.wemalo_inventory w
        left join allocation r on r.serial_number = w.seriennummer
),
wemalo_f as (
    select seriennummer,
        reporting_date,
        rn
    from wemalo
    where rn = 1
)
select r.*,
		coalesce (
        coalesce(
            r.return_delivery_date,
            r.return_delivery_date_old,warehouse_return_date
        ),
        case
            when f.reporting_date > r.replacement_date then f.reporting_date
            else null
        end
    ) return_delivery,
    CASE WHEN return_delivery IS NOT NULL OR asset_status='RETURNED' THEN 'RETURNED'
    	WHEN asset_status IN ('IN DEBT COLLECTION','LOST') THEN asset_status
    	WHEN return_shipment_at IS NOT NULL THEN 'SHIPPED'
    	WHEN asset_status LIKE 'INVESTIGATION' OR Asset_status <>'ON LOAN' THEN 'INVESTIGATION/OTHER'
    	ELSE 'NOT RETURNED' END return_status,	
     CASE
    	WHEN return_shipment_label_created_at IS NOT NULL THEN TRUE ELSE FALSE 
    END  AS is_org_label_created,
    CASE 
    	WHEN return_shipment_at IS NOT NULL THEN TRUE ELSE FALSE 
    END AS is_org_with_carrier,
    CASE 
    	WHEN return_delivery IS NOT NULL THEN TRUE ELSE FALSE 
    END is_org_asset_returned,
    CASE 
    	WHEN replaced_delivered_at IS NOT NULL THEN TRUE ELSE FALSE 
    END AS is_replacement_delivered,
     CASE 
	    WHEN is_org_label_created =FALSE AND is_org_with_carrier=FALSE AND is_org_asset_returned=FALSE AND is_replacement_delivered=TRUE THEN 'UC1'--DATA missing or OA shipment label not created / RA delivered
	    WHEN is_org_label_created =TRUE AND is_org_with_carrier=FALSE AND is_org_asset_returned=FALSE AND is_replacement_delivered=TRUE THEN 'UC2'--OA shipment label created / RA Delivered
	    WHEN /*is_org_label_created =TRUE AND*/ is_org_with_carrier=TRUE AND is_org_asset_returned=FALSE AND is_replacement_delivered=TRUE THEN 'UC3'--OA picked by carrier / RA delivered 
	    WHEN is_org_label_created =FALSE AND is_org_with_carrier=FALSE AND is_org_asset_returned=FALSE AND is_replacement_delivered=FALSE THEN 'UC4'--DATA missing or OA shipment label not created/RA not delivered
	    WHEN is_org_label_created =TRUE AND is_org_with_carrier=FALSE AND is_org_asset_returned=FALSE AND is_replacement_delivered=FALSE THEN 'UC5' -- OA label created / RA NOT delivered
	    WHEN /*is_org_label_created =TRUE AND*/ is_org_with_carrier=TRUE AND is_org_asset_returned=FALSE AND is_replacement_delivered=FALSE THEN 'UC6'--OA picked by carrier / RA not delivered   
	    ELSE 'WTF'
	    END use_Cases
from allocation r
    left join wemalo_f f on r.serial_number = f.seriennummer
   WHERE 1=1 
   	AND replacement_date<=current_date-14
   	AND is_org_asset_returned=FALSE
   	AND is_delivered='Delivered' ;

GRANT SELECT ON monitoring.or4 TO tableau;
