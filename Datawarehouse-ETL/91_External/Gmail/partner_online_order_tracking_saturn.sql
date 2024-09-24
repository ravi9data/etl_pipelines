drop table skyvia.partner_online_order_tracking_saturn;
create table skyvia.partner_online_order_tracking_saturn as  
with a as (
	select 
     "date" as email_date,
     left(SUBSTRING(messagebody,LEN(LEFT(messagebody, CHARINDEX ('ST-PR', messagebody)))),10) as reference_nr,
      SPLIT_PART(left(attachments,20),'_',2) as rechnungs_number,
      SPLIT_PART (SPLIT_PART(messagebody,'he von <b>',2),'</b>',1) as amount,
      SPLIT_PART(regexp_substr(messagebody,'href="https://www.saturn.de/de/shop/sendungsverfolgung.html[^"]+',1,1),'"',2) as sendung_verfolgen1,
 	SPLIT_PART(regexp_substr(messagebody,'href="https://www.saturn.de/de/shop/sendungsverfolgung.htm[^"]+',1,2),'"',2) as sendung_verfolgen2,
 	SPLIT_PART(regexp_substr(messagebody,'href="https://www.saturn.de/de/shop/sendungsverfolgung.htm[^"]+',1,3),'"',2) as sendung_verfolgen3,
	SPLIT_PART(regexp_substr(messagebody,'href="https://www.saturn.de/de/shop/sendungsverfolgung.htm[^"]+',1,4),'"',2) as sendung_verfolgen4,
 	SPLIT_PART(regexp_substr(messagebody,'href="https://www.saturn.de/de/shop/sendungsverfolgung.htm[^"]+',1,5),'"',2) as sendung_verfolgen5
 from stg_external_apis.procurement_email_saturn pespe 
 where subject like 'Versandbestätigung und Rechnung - Bestellung%'
 ),  b as ( 
 	SELECT 
      "date" ,
      right(subject,8 ) as order_number
  ,    left(SUBSTRING(messagebody,LEN(LEFT(messagebody, CHARINDEX ('ST-PR', messagebody)))),10) as reference_number,
      SPLIT_PART(SPLIT_PART(SPLIT_PART(messagebody ,'Rechnungssumme',2 ),'</span>',1),'normal;">',2) as amount
 from stg_external_apis.procurement_email_saturn pespe 
 where subject like 'Bestätigung - Bestellung%')
 select 
   	a.reference_nr,
   	trunc(b."date") as order_date,
   	b.order_number,
   	b.amount as order_amount,
   	sendung_verfolgen1 as tracking_link1,
   	sendung_verfolgen2 as tracking_link2,
   	sendung_verfolgen3 as tracking_link3,
   	sendung_verfolgen4 as tracking_link4,
   	sendung_verfolgen5 as tracking_link5,
   	trim(left(SPLIT_PART(sendung_verfolgen1,'trackingNo=',2),12) || '/' || left(SPLIT_PART(sendung_verfolgen2,'trackingNo=',2),12) || '/' ||left(SPLIT_PART(sendung_verfolgen3,'trackingNo=',2),12) || '/' ||left(SPLIT_PART(sendung_verfolgen4,'trackingNo=',2),12) || '/' ||left(SPLIT_PART(sendung_verfolgen5,'trackingNo=',2),12),'//' ) as invoice_number ,
    trunc(a.email_date) as invoice_date,
    rechnungs_number,
	a.amount as invoice_amount
   from a
   left join b on a.reference_nr=b.reference_number
order by a.email_date  desc ;
 

GRANT all ON SCHEMA skyvia  TO skyvia;
GRANT all ON ALL TABLES IN SCHEMA skyvia  TO  skyvia;
