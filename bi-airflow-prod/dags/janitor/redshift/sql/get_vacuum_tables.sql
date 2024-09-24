select i.schema        as schema_name,
       i.table         as table_name,
       i.unsorted      as percent_unsorted,
       i.stats_off     as stats_needed,
       i.vacuum_sort_benefit as vacuum_needed
from   svv_table_info i
where i.schema in ('master')
and (percent_unsorted > 50
or stats_needed > 50
or vacuum_needed > 50);
