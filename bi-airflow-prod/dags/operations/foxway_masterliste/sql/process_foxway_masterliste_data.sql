drop table if exists dm_recommerce.foxway_masterliste;
create table dm_recommerce.foxway_masterliste as
select fm.*
, lag(fm.reporting_date) over (partition by coalesce(fm.grover_id, fm.inventory_id) order by reporting_date) as previous_reporting_date
, nullif(sales_price,'')::double precision as sales_price_converted
, a.asset_name
, a.category_name
, a.subcategory_name
, a.brand
, case fm.grading               when 'bad' then 'Bad'
				when 'Grade A+' 		then 'Like New'
				when 'ok' 				then 'Good'
				when 'good' 			then 'Good'
				when 'Grade C' 			then 'Bad'
				when 'very_good' 		then 'Very Good'
				when 'Incomplete' 		then 'Incomplete'
				when 'Grade A' 			then 'Very Good'
				when 'Grade C+' 		then 'Bad'
				when 'Motherboard only' then 'Recycle'
				when 'new' 				then 'New'
				when 'Scrap'			then 'Recycle'
				when 'like_new' 		then 'Like New'
				when 'Grade B' 			then 'Good' end as grade
from recommerce.foxway_masterliste fm
left join dm_recommerce.foxway_masterliste_mapping fmm on trim(coalesce(fm.inventory_id, fm.grover_id))=trim(coalesce(fmm.inventory_id, fmm.grover_id))
left join master.asset a on a.asset_id = trim(fmm.asset_id);

GRANT SELECT ON TABLE dm_recommerce.foxway_masterliste TO tableau;
GRANT SELECT ON TABLE dm_recommerce.foxway_masterliste TO GROUP bi;
