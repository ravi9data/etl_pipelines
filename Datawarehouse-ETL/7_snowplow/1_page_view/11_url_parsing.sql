 drop table if exists scratch.url_parsing;
 create table scratch.url_parsing as
 select 
  url.*,
  split_part(page_url_path,'/',2) as url_split1,
  replace(split_part(page_url_path,'/',3),'-',' ') as url_split2,
  replace(split_part(page_url_path,'/',4),'-',' ') as url_split3, --can be order_id
  replace(split_part(page_url_path,'/',5),'-',' ') as url_split4,
  replace(split_part(page_url_path,'/',6),'-',' ') as url_split5,
  replace(split_part(page_url_path,'/',7),'-',' ') as url_split6,
  REGEXP_REPLACE(replace(replace(split_part(page_url_path,'/',2),'-de',''),'-','_'),'_.[0-9]{3}')  as store_name
from scratch.url_details url
;