drop table if exists scratch.url_page_type_mapping;
create table scratch.url_page_type_mapping as 
with slug as (
select distinct replace(slug,'-',' ') as slug, product_sku from ods_production.product)
,subcategory as (
select distinct case when lower(replace(subcategory_name,'-',' '))='hi-fi audio' then 'hi-fi-audio' else lower(subcategory_name) end as subcategory_name from ods_production.product)
,category as (
select distinct case when lower(replace(category_name,'&','and'))='emobility' then 'e mobility'  else lower(replace(category_name,'&','and')) end as category_name from ods_production.product)
,a as (
select distinct 
url.page_url,
url_split1,
url_split2,
url_split3,
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
  end as page_type,
  slug.product_sku,
  sc.subcategory_name as subcategory,
  c.category_name as category
from scratch.url_parsing url
left join slug on slug.slug=url_split3
left join subcategory sc on sc.subcategory_name=url_split3
left join category c on c.category_name=url_split2
)
select 
a.page_url,
page_type,
case 
 when page_type ='pdp' 
  then product_sku 
 when page_type in ('dashboard','featured','checkout','payment','g about','g explore','collections','hello','pages','dashboard','partners')
  then url_split3
 when page_type in ('subcategory')
  then subcategory
 when page_type in ('category')
  then category
 end as page_type_detail 
from a;