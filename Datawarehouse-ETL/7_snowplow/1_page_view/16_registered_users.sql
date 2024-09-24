drop table if exists scratch.non_registered_users;
create table scratch.non_registered_users as 
select domain_userid, count(distinct user_id) as user_ids
from atomic.events
where useragent not ilike '%datadog%'
group by 1
having coalesce(count(distinct user_id),0)=0;

drop table if exists scratch.registered_users;
create table scratch.registered_users as 
select domain_userid, count(distinct user_id) as user_ids
from atomic.events
where useragent not like '%Datadog%'
group by 1
having coalesce(count(distinct user_id),0)>=1;

