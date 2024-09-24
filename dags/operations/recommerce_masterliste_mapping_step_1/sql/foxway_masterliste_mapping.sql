drop table if exists hightouch_sources.foxway_masterliste_mapping;
create table hightouch_sources.foxway_masterliste_mapping as

with enriching_with_SF as(
select fm.*, a.asset_id from dm_recommerce.distinct_foxway_masterliste fm
left join master.asset a on fm.grover_id=a.serial_number),

foxway_masterliste_new_records as(
select * from enriching_with_SF
where inventory_id not in(select inventory_id from dm_recommerce.foxway_masterliste_mapping)
and asset_id is not null)

select * from(
select * from foxway_masterliste_new_records
union
select * from dm_recommerce.foxway_masterliste_mapping);

 grant select on table hightouch_sources.foxway_masterliste_serial_mismatch to tableau;
 grant select on table hightouch_sources.foxway_masterliste_serial_mismatch to GROUP bi;
 
 grant select on table hightouch_sources.foxway_masterliste_mapping to tableau;
 grant select on table hightouch_sources.foxway_masterliste_mapping to GROUP bi;
 grant select on table hightouch_sources.foxway_masterliste_mapping to hightouch;
