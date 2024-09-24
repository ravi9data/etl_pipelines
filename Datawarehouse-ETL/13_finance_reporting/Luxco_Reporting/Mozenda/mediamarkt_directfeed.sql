       drop table ods_external.spv_used_asset_price_mediamarkt_directfeed;
        create table ods_external.spv_used_asset_price_mediamarkt_directfeed as 
          select 
          		DISTINCT
           		'MEDIAMARKT_DIRECT_FEED' as src,
           		 m.week_date as reporting_month,
           		'Null' as itemid,
       	    	m.product_sku,
            	'EUR' as currency,
               m.price,
            	'Neu' as asset_condition
          from ods_external.mm_price_data  m 




/* 
create temp table spv_used_asset_price_mediamarkt_directfeed_dl (like ods_external.spv_used_asset_price_mediamarkt_directfeed); 
insert into spv_used_asset_price_mediamarkt_directfeed_dl 
select DISTINCT
	'MEDIAMARKT_DIRECT_FEED' as src,
	am.reporting_month,
	'Null' as itemid,
	m.product_sku,
	'EUR' as currency,
	m.price,
    'Neu' as asset_condition
from ods_external.spv_used_asset_price_amazon1 am 
left join  ods_external.mm_price_data  m 
     on date_trunc('week',reporting_month::Date)=m.week_date
  where am.reporting_month::date> DATEADD(day, -3, CURRENT_DATE);



begin transaction;

delete from ods_external.spv_used_asset_price_mediamarkt_directfeed 
using spv_used_asset_price_mediamarkt_directfeed_dl s
where spv_used_asset_price_mediamarkt_directfeed.reporting_month = s.reporting_month; 

insert into ods_external.spv_used_asset_price_mediamarkt_directfeed 
select * from spv_used_asset_price_mediamarkt_directfeed_dl;

end transaction;



drop table spv_used_asset_price_mediamarkt_directfeed_dl;

*/


