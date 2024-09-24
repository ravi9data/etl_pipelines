
--deduplicate src table
create temp table ranked_affiliate_daisycon_submitted_orders as
with cte_base as
(
select 
transaction_id,							
to_timestamp(replace(date_transaction,'"',''), 'YYYY-MM-DD HH:MI:SS')::timestamp date_transaction,
ip_country_code,
ip_country_name,
to_timestamp(replace(date_click,'"',''), 'YYYY-MM-DD HH:MI:SS')::timestamp date_click,
replace(media_name,'"','') media_name,
publisher_name,
row_number() over(partition by transaction_id order by date_transaction desc) rn
from staging.daisycon
)
select 
transaction_id,
date_transaction,
date_click,
ip_country_code,
ip_country_name,
media_name,
publisher_name
from cte_base
where rn = 1;

--delete existing record so we dont have dups
delete from marketing.affiliate_daisycon_submitted_orders
using ranked_affiliate_daisycon_submitted_orders src 
where marketing.affiliate_daisycon_submitted_orders.transaction_id = src.transaction_id;

--insert records
insert into marketing.affiliate_daisycon_submitted_orders
select *
from ranked_affiliate_daisycon_submitted_orders;
