truncate dm_recommerce.SPL_KD_Cleanup_Channeling_Mapping;

insert into dm_recommerce.SPL_KD_Cleanup_Channeling_Mapping
select channeling, mapping from staging.mapping;

drop table if exists dm_recommerce.SPL_KD_Final;
create table dm_recommerce.SPL_KD_Final as
select skf.*, 
coalesce(m.mapping, 'INFORMATION MISSING') as mapping 
from staging.SPL_KD_Final skf 
left join dm_recommerce.spl_kd_cleanup_channeling_mapping  m on skf.channeling = m.channeling;
