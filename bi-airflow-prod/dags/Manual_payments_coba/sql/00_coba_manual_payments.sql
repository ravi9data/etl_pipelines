DELETE from debt_collection.manual_payments_coba
where report_date=current_date;

insert into debt_collection.manual_payments_coba  
SELECT 
current_date AS report_date
,extractnumber::int
,booking_date  
,arrival_date  
,amount  
,currency   
,s_h  
,store_no   
,positions::int  
,gvc::int  
,booking_text  
,reference  
,reason_for_return   
,reference_for_return   
,primanota_nr::int  
,original_amount  
,fees 
,"bank_reference " AS bank_reference
,customer_reference  
,check_number  
,end_to_end_reference   
,mandate_reference   
,customer_name 
,customer_iban 
,customer_big
FROM stg_external_apis_dl.manual_payments_coba;



/* One time run script
DROP TABLE IF EXISTS debt_collection.manual_payments_coba;
CREATE TABLE debt_collection.manual_payments_coba AS 
SELECT 
report_date,
extractnumber,
bookingdate AS booking_date,
arrivaldate AS arrival_date,
amount,
currency,
sh AS s_h,
storeno AS store_no,
positions,
gvc,
bookingtext AS booking_text,
reference,
reasonforreturn AS reason_for_return ,
referenceforreturn AS reference_for_return,
primanotanr AS primanota_nr,
originalamount AS original_amount,
fees,
bankreference AS bank_reference,
customerreference AS customer_reference,
checknumber AS check_number,
endtoendreference AS end_to_end_reference,
mandatereference AS mandate_reference,
customername AS customer_name,
customeriban AS customer_iban,
customerbig AS customer_big 
FROM stg_external_apis.manual_payments_coba;*/
