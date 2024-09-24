--Delete in case of any error
with cte_remove_dups as
(
select id
from s3_spectrum_intercom.conversation_id 
group by 1
)
DELETE FROM staging.intercom_deleted_conversations 
USING  cte_remove_dups src
WHERE  src.id  = staging.intercom_deleted_conversations.id;
/*
--Insert the deleted conversations
insert into staging.intercom_deleted_conversations
with cte_year_month_day as
(
--grouping rows in day, month, year
select
year,
month,
day
from s3_spectrum_intercom.conversation_id 
group by 1,2,3
)
,cte_ranked as 
(
--ranking days
select 
*, 
row_number() over(order by year desc, month desc, day desc ) rn
from cte_year_month_day
)
,last_3_days as 
(
--fetch last 3 days in case of any error just as a backup
select * 
from cte_ranked
where rn <= 3
)
,cte_remove_dups_last_3_days as 
(
--fetch distinct conversations in the last 3 days
select
src.id
from s3_spectrum_intercom.conversation_id src
inner join last_3_days l3d
on src.year = l3d.year
and src.month = l3d.month
and src.day = l3d.day
group by 1
)
,cte_not_found_anymore as
(
--fetch conversation not existing anymore in s3_spectrum_intercom.conversation_id  "full daily update" vs stg_external_apis.intercom_conversations 
select 
trg.id
from stg_external_apis.intercom_conversations  trg
left join cte_remove_dups_last_3_days src
on trg.id  = src.id
where src.id is null
)
select 
id,
CURRENT_TIMESTAMP as loaded_at 
from cte_not_found_anymore 
where id not in(select id from staging.intercom_deleted_conversations);*/