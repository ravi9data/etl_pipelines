create temp table web_vitals_dl(like web.web_vitals);
insert into web_vitals_dl
with slug as (
select distinct replace(slug,'-',' ') as slug, product_sku from ods_production.product),
subcategory as (
select distinct case when lower(replace(subcategory_name,'-',' '))='hi-fi audio' then 'hi-fi-audio' else lower(subcategory_name) end as subcategory_name from ods_production.product)
,category as (
select distinct case when lower(replace(category_name,'&','and'))='emobility' then 'e mobility'  else lower(replace(category_name,'&','and')) end as category_name from ods_production.product),
page as(
select event_id,  collector_tstamp ,domain_sessionid , page_url,dvce_type,platform, page_urlpath ,
  case when se_action = 'LCP'
	then se_value
	else null
  end as LCP,
  case when se_action = 'TTFB'
  	then se_value 
  	else null
  end as TTFB,
  case when se_action = 'FID'
  	then se_value 
  	else null
  end as FID,
  case when se_action = 'CLS'
  	then se_value 
  	else null
  end as CLS,
  split_part(page_urlpath,'/',2) as url_split1,
  replace(split_part(page_urlpath,'/',3),'-',' ') as url_split2,
  replace(split_part(page_urlpath,'/',4),'-',' ') as url_split3, --can be order_id
  replace(split_part(page_urlpath,'/',5),'-',' ') as url_split4,
  replace(split_part(page_urlpath,'/',6),'-',' ') as url_split5,
  replace(split_part(page_urlpath,'/',7),'-',' ') as url_split6,
  REGEXP_REPLACE(replace(replace(split_part(page_urlpath,'/',2),'-de',''),'-','_'),'_.[0-9]{3}')  as store_name
from atomic.events e 
where etl_tstamp::date > DATEADD(week, -1, CURRENT_DATE) and se_category ='web_vitals' and page_url not like '%localhost%'
	and e.useragent not like '%Datadog%')
select page.*,
case 
	 when url_split2 =''
	  or url_split2='landing'
	   then 'landing'
	 when url_split2='products'
	  then 'pdp' 
	 when url_split2 in (
	 'for you','featured','trending','deals','collections',
	 'search','browse','join',
	 'how it works','terms','hello',
	 'dashboard','pages','your tech','payment methods',
	 'invite friend',
	 'welcome','cart','checkout','password recovery','auth','payment','pages','favorites','partners','confirmation')
	  then url_split2
	 when url_split2 like ('%g %')
	  then url_split2
	 when  sc.subcategory_name is not null 
	  then 'subcategory'
	 when c.category_name is not NULL
	  then 'category'
	 else 'others'
  end as page_type
  from page 
  left join slug on slug.slug=url_split3
	left join subcategory sc on sc.subcategory_name=url_split3
	left join category c on c.category_name=url_split2
	where collector_tstamp::date > DATEADD(week, -1, CURRENT_DATE);



begin transaction;

delete from web.web_vitals
using web_vitals_dl b
where web_vitals.event_id = b.event_id;

insert into web.web_vitals 
select * from web_vitals_dl;



end transaction;


drop table web_vitals_dl;
