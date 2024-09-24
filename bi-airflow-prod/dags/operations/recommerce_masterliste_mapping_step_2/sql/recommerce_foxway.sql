truncate table dm_recommerce.foxway_masterliste_mapping;

insert into dm_recommerce.foxway_masterliste_mapping 
select external_id, grover_id, inventory_id, asset_id from staging.matching;

 grant select on table dm_recommerce.foxway_masterliste_mapping to tableau;
 grant select on table dm_recommerce.foxway_masterliste_mapping to GROUP bi;
 