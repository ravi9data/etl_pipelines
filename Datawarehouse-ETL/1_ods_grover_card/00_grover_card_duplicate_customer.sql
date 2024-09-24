as
with cte_src as(
select 
src.customer_id,
src.solaris_id,
src.first_card_created_date,
cs.first_name, 
cs.last_name,
cs.phone_number
inner join ods_data_sensitive.customer_pii cs
on src.customer_id = cs.customer_id
where src.first_card_created_date is not null
and solaris_id is not null
)
,cte_fraud_name as( 
select 
*,
count(*) over(partition by first_name, last_name) cnt_first_last_name
from cte_src
)
,
cte_fraud_number as( 
select 
*, 
count(*) over(partition by phone_number) cnt_phone_number
from cte_src
where phone_number is not null 
)
select 
customer_id,
solaris_id,
first_card_created_date,
'duplicate_name' reason
from cte_fraud_name where cnt_first_last_name > 1
union
select 
customer_id,
solaris_id,
first_card_created_date,
'duplicate_number' reason
from cte_fraud_number where cnt_phone_number > 1;

