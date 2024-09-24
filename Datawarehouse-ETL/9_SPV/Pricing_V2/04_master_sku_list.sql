drop table if exists trans_dev.master_sku_list;
create table trans_dev.master_sku_list as
SELECT 
	a.product_sku ,
	avg(a.initial_price) as ltd_avg_purchase_price,
	datediff('month', max(purchased_date), current_date) as months_since_last_purchase
	FROM 
	master.asset a 	
group by 1
;
