create view dm_risk.v_credit_bureau_reporting
as (
with credit_reporting_data AS
(
SELECT *
FROM ods_data_sensitive.credit_bureau_spain_shipaddress cbss 
WHERE snapshot_date = current_date 
	AND lastest_address_normalized 
	AND codigo_postal IS NOT NULL
	AND municipio_norm IS NOT null
	AND regexp_count(numero, '[0-9]')>0
	AND regexp_count(lower(numero), '[a-z]') <= 3
	AND dpd >30
)
,header_rec as (  --- header record
select rpad('EX200007106'|| to_char(current_date,'YYYYMMDD') || to_char(current_date,'YYYYMMDD'),600) as rec 
)
,transaction_rec AS (-- Transaction records
select 'EX2010'||  									-- code for transaction record (M)
	   '07106' ||  									-- subscriber code  (M 6)
	   rpad(subscription_id ,29,'0') ||				-- Account Number  (M 29)
	   rpad(' ',1,' ') || 							-- Filler (M 1)
	   '51' ||										-- Type of Financial Product (M 2) - Renting
	   payment_situation ||							-- Payment Status (M 1)		
	   to_char(start_date,'YYYYMMDD') ||			-- start_date  (M 8)
	   to_char(end_date,'YYYYMMDD')  ||				-- end_date (C 8)
	   to_char(first_unpaid_due_date,'YYYYMMDD') ||	-- Date of the First Outstanding Deadline (M 8)
	   to_char(last_unpaid_due_date,'YYYYMMDD') ||	-- Date of the Last Outstanding Deadline (M 8)
	   rpad(' ',4,' ')	||							-- Number of Instalments (O 4)
	   '01'  || 									-- payment frequency (M 2)  - Monthly
	   lpad(unpaid_instalments_count,4,'0')	||		-- Number of Outstanding Instalments (C 4)
	   rpad(' ',15,' ')	||							-- Financed Amount (O 15)
	   rpad(' ',15,' ')	||							-- Amount of each Instalment (O 15)
	   rpad(' ',15,' ')	||							-- Total Pending Amount (O 15)
	   lpad(replace(replace(round(unpaid_instalments_amount,2)::decimal(38,2),',',''),'.',''),15,0) ||	-- Amount Currently Outstanding (M 15)
	   rpad(' ',3,' ')	||							-- Reserved Filler (M 3)
	   rpad(' ',451,' ')	as rec					-- Filler (M 451)
FROM credit_reporting_data)
,name_rec AS ( -- Name records
select 'EX2020'||  									-- code for names record (M)
	   '07106' ||  									-- subscriber code  (M 6)
	   rpad(subscription_id ,29,'0') || 				-- Account Number  (M 29)
	   ' ' || 										-- Filler (M 1)
	   rpad(customer_document_number,10) ||			-- Document Number 1 (M 10) 
	   rpad(' ',10,' ')	||							-- Filler (M 10)
	   '1' ||										-- Party Type (M 1)		
	   '2' ||										-- Name Format Indicator  (M 1) Surnames (60) Name (50)
	   rpad(customer_lastname,60)||rpad(customer_firstname,50) || -- Name/Company Name (M 110)
	   rpad(' ',8,' ')	||							-- Incorporation Date (O 8)
	   rpad(' ',3,' ')	||							-- Nationality Code (O 3)
	   rpad(' ',5,' ')	||							-- CNO Code (O 5)
	   rpad(' ',5,' ')	||							-- CNAE Code (O 5)
	   '10000'	||									-- Chargeable percentage (M 5)
	   customer_document_type ||					-- Document 1 type (C 2)
	   '724'||										-- Document 1 Country of issue (C 3)  - Spain
	   rpad(' ',2,' ') ||							-- Document 2 type (C 2)
	   rpad(' ',3,' ') ||							-- Document 2 Country of issue (C 3)  
	   rpad(' ',25,' ')	||							-- Number of Document 2 (C 25)
	   rpad(' ',366,' ')	as rec					-- Filler (M 366)
FROM credit_reporting_data
)
,address_rec AS (-- Address records
SELECT 'EX2030'||  									-- code for names record (M)
	   '07106' ||  									-- subscriber code  (M 6)
	   rpad(subscription_id ,29,'0') || 			-- Account Number  (M 29)
	   ' ' || 										-- Filler (M 1)
	   rpad(customer_document_number,10) ||			-- Document Number 1 (M 10) 
	   rpad(' ',10,' ')	||							-- Filler (M 10)
	   '2' ||										-- Address Format Indicator (M 1) 	 	
	   rpad(COALESCE(tipo_via_norm,' '),10) 
	   	|| rpad(COALESCE(via_norm,' '),50) 
	   	|| rpad(COALESCE(numero_norm,numero),10)
	   	|| rpad(' ',40,' ') ||						-- Address  (M 110) Road Type (10) Road Name (50) Road No. (10) Other (40)
	   rpad(' ',6,' ')	||							-- District Code (C 6)	
	   rpad(municipio_norm ,50,' ')	||				-- Town (C 50)	
	   LEFT(lpad(codigo_postal::text,5,0),2)	||	-- Province Code(C 2)
	   rpad(' ',3,' ')	||							-- Country Code(C 3)	
	   rpad(codigo_postal,5)||						-- postal code (M 5)
	   rpad(coalesce(customer_phone,' '),20)|| 		-- Telephone (O 20)
	   rpad(' ',1,' ')	||							-- Confirmed address indicator (C 1)
	   customer_document_type ||					-- Document 1 type (C 2)
	   '724' ||										-- Document 1 Country of issue (C 3)  - Spain
	   rpad(' ',2,' ') ||							-- Document 2 type (C 2)
	   rpad(' ',3,' ') ||							-- Document 2 Country of issue (C 3)  
	   rpad(' ',25,' ') ||							-- Number of Document 2 (C 25)
	   rpad(' ',1,' ') ||							-- Confirmed address indicator required before payment (C 1)
	   rpad(' ',305,' ')		as rec				-- Filler (M 305)
FROM credit_reporting_data)
,cntrl_rec AS ( -- CONTROL record
SELECT rpad('EX299907106'|| to_char(current_date,'YYYYMMDD')   		-- Type of Record
	|| lpad((SELECT count(1) FROM transaction_rec)::text ,9,'0') 	-- count OF TRANSACTION records
	|| lpad((SELECT count(1) FROM name_rec)::text,9,'0') 			-- count OF name records
	|| lpad((SELECT count(1) FROM address_rec)::text,9,'0') 		-- count OF address records
	|| lpad((SELECT count(1)*3+2 FROM credit_reporting_data),9,'0') ,600) as rec	-- count OF total records
	)
,final_data AS (
SELECT rec
FROM header_rec
UNION all
SELECT rec
FROM transaction_rec
UNION all
SELECT rec
FROM name_rec
UNION all
SELECT rec
FROM address_rec
UNION ALL
SELECT rec
FROM cntrl_rec)
SELECT rec 
FROM final_data
order by left(rec,4),substring(rec,12,29),left(rec,6)
)  ;
