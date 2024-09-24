drop table if exists staging.event_properties_dl;

create table staging.event_properties_dl (like web.event_properties); 

insert into staging.event_properties_dl 
with a as (
select 
trim(event_id) as event_id, 
se_category,
se_action, 
se_label,
se_property,
trim(domain_sessionid) as session_id,
trim(case 
when IS_VALID_JSON(se_property) 
  then
    case 
      when trim(json_extract_path_text(se_property,'orderId')::varchar(44)) !=''
      then trim(json_extract_path_text(se_property,'orderId')::varchar(44))
      when trim(json_extract_path_text(se_property,'orderID')::varchar(44)) !=''
      then trim(json_extract_path_text(se_property,'orderID')::varchar(44)) 
      end 
  else null 
  end) as order_id,
  trim(case 
when IS_VALID_JSON(se_property) 
  then 
    case 
      when trim(json_extract_path_text(se_property,'userID')::varchar(44))!=''
      then trim(json_extract_path_text(se_property,'userID')::varchar(44))
      when trim(json_extract_path_text(se_property,'userId')::varchar(44))!=''
      then trim(json_extract_path_text(se_property,'userId')::varchar(44))
      end 
  else null 
  end) as customer_id,
trim(case 
 when IS_VALID_JSON(se_property) 
  then trim(json_extract_path_text(se_property,'current_flow')::varchar(666)) 
   else null 
  end) as current_flow,
trim(case 
      when IS_VALID_JSON(se_property) 
      then trim(json_extract_path_text(se_property,'search_term'))::varchar(666) 
      else null 
      end) as Search_term,
trim(case 
when IS_VALID_JSON(se_property) 
  then 
    case 
    when trim(json_extract_path_text(se_property,'product_sku'))::varchar(666) !=''
    then trim(json_extract_path_text(se_property,'product_sku'))::varchar(666) 
    when trim(json_extract_path_text(se_property,'productSKU')::varchar(max))!=''
    then trim(replace(replace(json_extract_path_text(se_property, 'productSKU'),'["',''),'"]',''))
    when IS_VALID_JSON(trim(json_extract_path_text(se_property,'productData')))
    then trim(json_extract_path_text(trim(json_extract_path_text(se_property,'productData')),'productSku')::varchar(44))
  else null end
  else null 
  end) as product_sku
FROM atomic.events
where se_property is not null and etl_tstamp::date > DATEADD(week, -1, CURRENT_DATE)
  and useragent not like '%Datadog%')
select a.*, 
p.product_name, p.category_name, p.brand, p.subcategory_name
from a 
left join ods_production.product p 
 on p.product_sku=a.product_sku
;

    

begin transaction;

delete from web.event_properties
using staging.event_properties_dl s
where event_properties.event_id = s.event_id; 



insert into web.event_properties
select * from staging.event_properties_dl;

end transaction;


-- drop table event_properties_dl;
