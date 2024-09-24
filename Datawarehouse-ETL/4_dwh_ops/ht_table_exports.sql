drop table if exists hightouch_sources.wemalo_position_labels;
create table hightouch_sources.wemalo_position_labels as
select * from stg_external_apis.wemalo_position_labels;


drop table if exists hightouch_sources.monitoring_b2b_or1;
create table hightouch_sources.monitoring_b2b_or1 as
select * from monitoring.b2b_or1;

drop table if exists hightouch_sources.monitoring_or5;
create table hightouch_sources.monitoring_or5 as
select * from monitoring.or5;


drop table if exists hightouch_sources.assets_booked;
create table hightouch_sources.assets_booked as
select * from dm_operations.assets_booked;