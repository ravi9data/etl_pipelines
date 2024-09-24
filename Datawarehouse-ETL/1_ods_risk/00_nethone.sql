drop table if exists ods_data_sensitive.nethone;
create table ods_data_sensitive.nethone as 
select
	customer_id,
	order_id,
	score,
	advice,
	creation_timestamp
FROM s3_spectrum_rds_dwh_order_approval.nethone_data;
