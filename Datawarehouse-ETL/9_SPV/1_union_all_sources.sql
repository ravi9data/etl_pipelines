drop table if exists ods_spv_historical.union_sources;
create table ods_spv_historical.union_sources as
      with a as (
        	SELECT
        		a.region,
        		a.src,
         		a.reporting_month::date as extract_date,
    			(DATEADD( 'month', 1, date_trunc('month',a.reporting_month::date)::date)::date) as reporting_month,
           		a.item_id,
            	a.product_sku,
                a.asset_condition,
            	a.currency,
            	price,
            	price_in_euro            	
           FROM staging_price_collection.ods_amazon a
         	 WHERE --a.price > 0::numeric
          	  reporting_month::date<current_date::date
        	 UNION ALL
        	 SELECT
        	 	e.region,
         	    e.src,
        	    e.reporting_month::date as extract_date,
    			(DATEADD( 'month', 1, date_trunc('month',e.reporting_month::date)::date)::date) as reporting_month,
            	e.item_id,
            	e.product_sku,
                e.asset_condition,
            	e.currency,
            	e.price,
            	e.price_in_euro
           FROM staging_price_collection.ods_ebay e
          	WHERE --e.price > 0::numeric
		  	reporting_month::date<current_date::date
		  	UNION ALL
        	SELECT
        		r.region,
        		r.src,
         		r.reporting_month::date as extract_date,
  			    (DATEADD( 'month', 1, date_trunc('month',r.reporting_month::date)::date)::date) as reporting_month,
            	r.item_id,
            	r.product_sku,
                r.asset_condition,
            	r.currency,
            	r.price,
            	r.price_in_euro
           	FROM staging_price_collection.ods_rebuy r
          	 WHERE reporting_month::date<current_date::date
          	UNION ALL
  			SELECT
  				region,
           		src,
           		week_date AS extract_date,
    			reporting_month,
           		item_id,
           		product_sku,
                'Neu' as asset_condition,
            	currency,
            	price,
            	price_in_euro
         	FROM staging_price_collection.ods_saturn
  			 WHERE --price > 0 and 
  			 reporting_month::date<current_date::date --reporting_month<'2019-12-31'
	      UNION ALL
		SELECT
		region,
		src,
		reporting_month::date as extract_date,
		reporting_month ,
		item_id,
		product_sku,
		'Neu' as asset_condition,
		'EUR' as currency,
		price,
		price_in_euro 
		FROM
		staging_price_collection.ods_mediamarkt
	WHERE reporting_month::date<current_date::date
  )
  select *
  from a
  