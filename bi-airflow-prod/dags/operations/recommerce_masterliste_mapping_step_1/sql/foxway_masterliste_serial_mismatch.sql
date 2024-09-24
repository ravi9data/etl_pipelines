drop table if exists dm_recommerce.foxway_masterliste_serial_mismatch;
create table dm_recommerce.foxway_masterliste_serial_mismatch as
select fm.*, a.asset_id from dm_recommerce.distinct_foxway_masterliste fm
left join master.asset a on fm.grover_id=a.serial_number
where asset_id is null;

GRANT SELECT ON dm_recommerce.foxway_masterliste_serial_mismatch TO tableau;
