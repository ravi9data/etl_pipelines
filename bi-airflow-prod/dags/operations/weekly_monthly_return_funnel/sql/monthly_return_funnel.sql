drop table if exists dm_recommerce.monthly_return_funnel;
create table dm_recommerce.monthly_return_funnel as
	select * from(
	with scanned as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='Scanned'
	group by 1,2,3),

	return_delivered as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='Return Delivered'
	group by 1,2,3),


	scanned_return_delivered as(
	select  s.month_::date,
	'SCANNED/RETURN DELIVERED' as stage,
	'' as category_name,
	'' as subcategory_name,
	(sum(s.value)/sum(nullif(rd.value,0)))::float as value
	from scanned s
	full outer join return_delivered rd on s.month_ = rd.month_
	group by 1,2,3,4
	),

	in_stock as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='First Move-Stock'
	group by 1,2,3),

	first_move as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage like 'First Move%'
	group by 1,2,3
	),

	happy_pass_quote as(
	select  s.month_::date,
	'HAPPY PASS QUOTE' as stage,
	'' as category_name,
	'' as subcategory_name,
	(sum(s.value)/sum(nullif(fm.value,0)))::float as value
	from in_stock s
	full outer join first_move fm on s.month_ = fm.month_
	group by 1,2,3,4
	)

	select *, 'OVERALL' as section_ from(
	select * from scanned_return_delivered
	union
	select * from happy_pass_quote
	union
	(select
	date_trunc('month',fact_date)::date as month_,
	stage,
	'' as category_name,
	'' as subcategory_name,
	sum(value)
	from dm_recommerce.return_funnel
	group by 1,2,3,4
	)))
	union

	--category based data
	select * from(
	with scanned as(
	select
	date_trunc('month',fact_date) month_,
	category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='Scanned'
	group by 1,2,3),

	return_delivered as(
	select
	date_trunc('month',fact_date) month_,
	category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='Return Delivered'
	group by 1,2,3),


	scanned_return_delivered as(
	select  s.month_::date,
	'SCANNED/RETURN DELIVERED' as stage,
	s.category_name,
	'' as subcategory_name,
	(sum(s.value)/sum(nullif(rd.value,0)))::float as value
	from scanned s
	full outer join return_delivered rd on s.month_ = rd.month_ and s.category_name = rd.category_name
	group by 1,2,3,4
	),

	in_stock as(
	select
	date_trunc('month',fact_date) month_,
	category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='First Move-Stock'
	group by 1,2,3),

	first_move as(
	select
	date_trunc('month',fact_date) month_,
	category_name,
	'' as subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage like 'First Move%'
	group by 1,2,3
	),

	happy_pass_quote as(
	select  s.month_::date,
	'HAPPY PASS QUOTE' as stage,
	s.category_name,
	'' as subcategory_name,
	(sum(s.value)/sum(nullif(fm.value,0)))::float as value
	from in_stock s
	full outer join first_move fm on s.month_ = fm.month_ and s.category_name = fm.category_name
	group by 1,2,3,4
	)


	select *, 'CATEGORY' as section_ from(
	select * from scanned_return_delivered
	union
	select * from happy_pass_quote
	union
	(select
	date_trunc('month',fact_date)::date as month_,
	stage,
	category_name,
	'' as subcategory_name,
	sum(value)
	from dm_recommerce.return_funnel
	group by 1,2,3,4
	)))

	union

	--subcategory based data
	select * from(
	with scanned as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='Scanned'
	group by 1,2,3),

	return_delivered as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='Return Delivered'
	group by 1,2,3),


	scanned_return_delivered as(
	select  s.month_::date,
	'SCANNED/RETURN DELIVERED' as stage,
	'' as category_name,
	s.subcategory_name,
	(sum(s.value)/sum(nullif(rd.value,0)))::float as value
	from scanned s
	full outer join return_delivered rd on s.month_ = rd.month_ and s.subcategory_name = rd.subcategory_name
	group by 1,2,3,4
	),

	in_stock as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage='First Move-Stock'
	group by 1,2,3),

	first_move as(
	select
	date_trunc('month',fact_date) month_,
	'' as category_name,
	subcategory_name,
	sum(value)*1.0 as value
	from dm_recommerce.return_funnel
	where stage like 'First Move%'
	group by 1,2,3
	),

	happy_pass_quote as(
	select  s.month_::date,
	'HAPPY PASS QUOTE' as stage,
	'' as category_name,
	s.subcategory_name,
	(sum(s.value)/sum(nullif(fm.value,0)))::float as value
	from in_stock s
	full outer join first_move fm on s.month_ = fm.month_ and s.subcategory_name = fm.subcategory_name
	group by 1,2,3,4
	)


	select *, 'SUBCATEGORY' as section_ from(
	select * from scanned_return_delivered
	union
	select * from happy_pass_quote
	union
	(select
	date_trunc('month',fact_date)::date as month_,
	stage,
	'' as category_name,
	subcategory_name,
	sum(value)
	from dm_recommerce.return_funnel
	group by 1,2,3,4
	)));

GRANT SELECT ON dm_recommerce.monthly_return_funnel TO matillion;
GRANT SELECT ON dm_recommerce.monthly_return_funnel TO tableau;
GRANT SELECT ON TABLE dm_recommerce.monthly_return_funnel TO GROUP bi;
