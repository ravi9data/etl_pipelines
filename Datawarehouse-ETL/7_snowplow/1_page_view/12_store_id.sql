drop table if exists scratch.url_store_mapping;
create table scratch.url_store_mapping as 
with a as (
    select 
    page_url,
          case 
            when store_name in ('at','_at','at_en') then 'Austria'
            when store_name in ('uk_en') then 'UK'
            when store_name in ('business','business_en') then 'Grover_B2B_Germany'
            when store_name =('gb') then 'UK'
            when store_name in ('mediamarkt','mediamarkt_en','mediamarkt_s10') then 'Media_Markt'
            when store_name = ('saturn_en') then 'saturn'
            when store_name=('conrad_en') then 'conrad'
            when store_name in ('otto','otto austria','otto_austria_en') then 'Otto_Austria'
            when store_name in ('C:') then ''
            when store_name in ('quelle_online','quelle','quelle_austria','quellea') then 'Quelle_Austria'
            when store_name in ('universal_online','universal_austria','universal') then 'Universal_Austria'
            when store_name in ('nl','nl_nl','nl_en') then 'Netherlands'
            when store_name in ('es','es_es','es_en') then 'Spain'
            when store_name in ('us','us_en') then 'United_States'
            when store_name ilike ('%irobot%') then 'iRobot'
            when store_name ilike ('%shifter%') then 'Shifter'
            when store_name ilike ('%Baur%') then 'Baur'
            when store_name ilike ('%boltshop%') then 'Boltshopnl'
        else store_name end as store_name,
        sum(sessions) as sessions
    from scratch.url_parsing
    group by 1,2
    ) 
select DISTINCT 
    a.page_url,
    s.id as store_id,
    a.store_name as store_name_snowplow,
    s.store_name,
    case 
        when a.store_name like 'saturn%' and s.store_name is null then 'Saturn offline'
        when a.store_name like 'mediamarkt%' and s.store_name is null then 'Media Markt offline'
        when a.store_name like 'gravis%' and s.store_name is null then 'Gravis offline'
    else coalesce(store_label,'Others') end as store_label,
    a.sessions
from a 
left join ods_production.store s
on lower(split_part(a.store_name,'_',1))=split_part(s.store_name_normalized,' ',1)
and lower(split_part(a.store_name,'_',2)) = split_part(s.store_name_normalized,' ',2)
and lower(split_part(a.store_name,'_',3)) = (split_part(s.store_name_normalized,' ',3))
order by 5 desc;