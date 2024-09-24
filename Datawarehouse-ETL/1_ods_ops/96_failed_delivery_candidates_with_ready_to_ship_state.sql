DROP TABLE IF EXISTS ods_operations.failed_delivery_candidates_with_ready_to_ship_state;
CREATE TABLE ods_operations.failed_delivery_candidates_with_ready_to_ship_state as

WITH allocation AS(
SELECT 
  as2.tracking_number
 ,ob_shipment_unique
 ,as2.allocation_id
 ,as2.allocated_at
 ,as2.ready_to_ship_at
 ,as2.shipment_label_created_at
 ,as2.carrier
 ,as2.allocation_status_original
 ,as2.subscription_id
 ,as2.serial_number
 ,as2.customer_type
 ,as2.warehouse
 ,as2.region
 ,a.replacement_date 
FROM ods_operations.allocation_shipment as2
LEFT JOIN ods_production.allocation a ON as2.allocation_id = a.allocation_id 
WHERE as2.ready_to_ship_at IS NOT NULL
AND as2.shipment_at IS NULL
AND as2.delivered_at IS NULL
AND as2.failed_delivery_at IS NULL
AND as2.return_delivery_date IS NULL
AND datediff('day', as2.shipment_label_created_at::date, current_date)>=7
AND as2.shipment_label_created_at::date>='2023-01-01'
)

,last_tracking_event AS(
SELECT 
 tracking_number
,carrier
,shipment_type
,status
,details
,event_timestamp
,failed_reason
,event_bucket
FROM ods_operations.tracking_events
WHERE is_last_event 
)

,failed_delivery_candidates_with_ready_to_ship_state AS(
SELECT 
 a.*
,t.details AS last_detail_of_shipment
,t.failed_reason
,t.event_bucket
,t.status
FROM allocation a
LEFT JOIN last_tracking_event t ON a.tracking_number = t.tracking_number
WHERE a.tracking_number IS NOT NULL
AND t.details IN
('Die Auftragsdaten zu dieser Sendung wurden vom Absender elektronisch an DHL übermittelt.', 
'Es erfolgte keine Einlieferung zu der per EDI Daten beauftragten Sendung. Die Sendungsdaten wurden daher storniert.',
'A claim has been issued to the sender for your package. Please contact the sender for more information.',
'We''ve begun an investigation to locate the package.',
'Der Inhalt der Sendung wurde beschädigt. Der DHL Kundenservice wird den Absender kontaktieren.',
'The package is not claim eligible because the liability period has passed.',
'Es liegt ein Wareneingangsscan des Kunden vor. (Information)',
'Shipper created a label, UPS has not received the package yet.',
'Sie wurden zum angekündigten Termin Datum nicht angetroffen. Bitte setzen Sie sich mit unserem zuständigen Depot in Verbindung.'
'Shipment created'))

--CTE
SELECT *
FROM failed_delivery_candidates_with_ready_to_ship_state;

GRANT SELECT ON ods_operations.failed_delivery_candidates_with_ready_to_ship_state TO tableau;
