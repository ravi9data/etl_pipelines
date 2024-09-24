 ----------- USER MAPPING
drop table if exists scratch.customer_mapping;
create table scratch.customer_mapping as 
with prep as (
select distinct 
    (a.page_view_id) AS page_view_id,
  a.domain_userid,
  b.min_tstamp,
  TRIM(f.customer_id)  as customer_id,
    f.created_at as user_registration_date,
    lead(f.customer_id) 
     ignore nulls over (partition by a.domain_userid order by b.min_tstamp)::varchar as next_customer,
  lag(f.customer_id) 
     ignore nulls over (partition by a.domain_userid order by b.min_tstamp)::varchar as previous_customer
FROM scratch.web_events AS a 
left  JOIN scratch.user_encoded_ids AS f
    ON a.user_id = f.encoded_id
left  JOIN scratch.web_events_time AS b
    ON a.page_view_id = b.page_view_id
)
select distinct 
domain_userid,
page_view_id,
min_tstamp,
coalesce(c.created_at,prep.user_registration_date) as user_registration_date,
c.start_date_of_first_subscription as acquisition_date,
prep.customer_id,
COALESCE(prep.customer_id,prep.next_customer,prep.previous_customer) as  customer_id_mapped
from prep
left join master.customer c 
 on COALESCE(prep.customer_id,prep.next_customer,prep.previous_customer)=c.customer_id
order by 2;
