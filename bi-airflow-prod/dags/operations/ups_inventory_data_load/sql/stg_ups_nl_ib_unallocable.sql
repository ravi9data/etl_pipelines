/*grant all on staging.ups_nl_ib_unallocable to group bi;

drop table if exists stg_external_apis.ups_nl_ib_unallocable;
create table stg_external_apis.ups_nl_ib_unallocable as
select distinct
sheetname,
sku,
description,
weekly_subs_avg,
disp_code_status_qty,
unallocable_kitted_qty,
unallocable_not_kitted_qty,
current_date::date as report_date
from staging.ups_nl_ib_unallocable
where sku is not null;*/

delete from stg_external_apis.ups_nl_ib_unallocable
where report_date = current_date::date;

insert into stg_external_apis.ups_nl_ib_unallocable
select distinct
sheetname,
sku,
description,
weekly_subs_avg,
disp_code_status_qty,
unallocable_kitted_qty,
unallocable_not_kitted_qty,
current_date::date as report_date
from staging.ups_nl_ib_unallocable
where sku is not null;

grant all on stg_external_apis.ups_nl_ib_unallocable to group bi;
