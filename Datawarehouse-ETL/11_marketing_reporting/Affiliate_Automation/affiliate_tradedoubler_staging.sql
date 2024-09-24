drop table if exists staging.tradedoubler;

create table staging.tradedoubler 
(
programm_country varchar(50),
visit_time varchar(50),
transaction_time varchar(50),
order_id varchar(50),
event_id varchar(200),
website_name varchar(200),
order_amount varchar(50),
commision_amount varchar(50)
);

copy staging.tradedoubler
iam_role 'arn:aws:iam::031440329442:role/data-production-redshift-datawarehouse-s3-role'
delimiter ';'  IGNOREHEADER as 1 --ESCAPE maxerror 1
;