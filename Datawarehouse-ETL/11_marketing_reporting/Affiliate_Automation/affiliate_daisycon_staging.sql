drop table if exists staging.daisycon;

create table staging.daisycon
(
    transaction_id varchar(50),
    affiliatemarketing_id varchar(50),
    date_transaction varchar(50),
    program_name varchar(200),
    ad_id varchar(50),
    ip varchar(50),
    ip_hash varchar(50),
    ip_country_code varchar(50),
    ip_country_name varchar(50),
    device_type varchar(50),
    device_model varchar(50),
    device_platform varchar(50),
    device_browser varchar(50),
    part_nr varchar(50),
    part_id varchar(50),
    unknown_1 varchar(50),
    unknown_2 varchar(50),
    date_click varchar(50),
    click_interval varchar(50),
    referer_click varchar(20000),
    media_id varchar(50),
    media_name varchar(50),
    publisher_name varchar(300),
    program_description varchar(500),
    program_commission varchar(50),
    publisher_description varchar(500),
    publisher_commission varchar(50),
    status varchar(50),
    disapproved_reason varchar(50),
    modified_date varchar(50),
    used_compensationcode varchar(50),
    revenue varchar(50),
    extra_1 varchar(50),
    extra_2 varchar(50),
    extra_3 varchar(50),
    extra_4 varchar(50),
    extra_5 varchar(50),
    reference varchar(50),
    invoice_status varchar(50),
    promotioncode varchar(50),
    currency_code varchar(50),
    extra_6 varchar(50)
);

copy staging.daisycon
    iam_role 'arn:aws:iam::031440329442:role/data-production-redshift-datawarehouse-s3-role'
    delimiter ';'  IGNOREHEADER as 1 --ESCAPE maxerror 1
;