drop table if exists skyvia.partner_online_order_tracking;
create table skyvia.partner_online_order_tracking as 
 with a as (
    SELECT 
     "date" as email_date,
     left(SUBSTRING(messagebody,LEN(LEFT(messagebody, CHARINDEX ('MM-PR', messagebody)))),10) as reference_nr,
     SPLIT_PART(left(attachments,20),'_',2) as rechnungs_number, 
      SPLIT_PART(SPLIT_PART(messagebody,'Betrag von <b>',2),' Eur',1) as amount,
    SPLIT_PART(regexp_substr(messagebody,'<a href="http://www.mediamarkt.de/de/shop/sendungsverfolgung[^"]+',1,1),'"',2) as sendung_verfolgen1,
 	SPLIT_PART(regexp_substr(messagebody,'<a href="http://www.mediamarkt.de/de/shop/sendungsverfolgung[^"]+',1,2),'"',2) as sendung_verfolgen2,
 	SPLIT_PART(regexp_substr(messagebody,'<a href="http://www.mediamarkt.de/de/shop/sendungsverfolgung[^"]+',1,3),'"',2) as sendung_verfolgen3,
	SPLIT_PART(regexp_substr(messagebody,'<a href="http://www.mediamarkt.de/de/shop/sendungsverfolgung[^"]+',1,4),'"',2) as sendung_verfolgen4,
 	SPLIT_PART(regexp_substr(messagebody,'<a href="http://www.mediamarkt.de/de/shop/sendungsverfolgung[^"]+',1,5),'"',2) as sendung_verfolgen5,
      'invoice and shipping' as email_type
from stg_external_apis.procurement_email
where subject ='MediaMarkt, Ihre Rechnung & Versandbestätigung'
    order by "date" DESC 
), b as (
   SELECT 
 	 "date" ,
    SPLIT_PART(regexp_substr(messagebody,'Bestellung <b>[^ <]+',1,1),'>',2)  as order_number,
 	 left(SUBSTRING(messagebody,LEN(LEFT(messagebody, CHARINDEX ('MM-PR', messagebody)))),10) as reference_number,
    SUBSTRING(messagebody , LEN(LEFT(messagebody, CHARINDEX ('von <b>', messagebody))) + 7, LEN(messagebody) - LEN(LEFT(messagebody, 
    CHARINDEX ('von <b>', messagebody)+7)) - LEN(RIGHT(messagebody, LEN(messagebody) - CHARINDEX ('Euro</b>', messagebody))) ) as amount,
    'order_confirmation' as email_type
  from stg_external_apis.procurement_email pe 
    where subject ='MediaMarkt, Ihre Bestellung ist eingegangen'
    order by "date" DESC )
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
   	trim(left(SPLIT_PART(sendung_verfolgen1,';trackingNo=',2),12) || '/' || left(SPLIT_PART(sendung_verfolgen2,';trackingNo=',2),12) || '/' ||left(SPLIT_PART(sendung_verfolgen3,';trackingNo=',2),12) || '/' ||left(SPLIT_PART(sendung_verfolgen4,';trackingNo=',2),12) || '/' ||left(SPLIT_PART(sendung_verfolgen5,';trackingNo=',2),12),'//' ) as invoice_number ,
    trunc(a.email_date) as invoice_date,
    rechnungs_number,
	a.amount as invoice_amount
   from a
   left join b on a.reference_nr=b.reference_number
order by a.email_date  desc  ;



GRANT all ON SCHEMA skyvia  TO skyvia;
GRANT all ON ALL TABLES IN SCHEMA skyvia  TO  skyvia;
