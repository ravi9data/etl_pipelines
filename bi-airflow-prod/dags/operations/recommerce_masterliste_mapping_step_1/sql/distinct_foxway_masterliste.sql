drop table if exists dm_recommerce.distinct_foxway_masterliste;
create table dm_recommerce.distinct_foxway_masterliste as
select distinct trim(external_id) as external_id,
upper(trim(grover_id)) as grover_id,
trim(inventory_id) as inventory_id 
from recommerce.foxway_masterliste
where inventory_id<>'';
