---web_page_context
create temp table web_page_context_dl (like scratch.web_page_context); 

insert into web_page_context_dl 
  WITH prep AS (
    SELECT
      root_id,
      id AS page_view_id
    FROM atomic.com_snowplowanalytics_snowplow_web_page_1
    where root_tstamp ::date> DATEADD(week, -1, CURRENT_DATE)
    GROUP BY 1,2
  )
  SELECT * 
  FROM prep 
  WHERE root_id NOT IN (SELECT root_id FROM prep GROUP BY 1 HAVING COUNT(*) > 1); -- exclude all root ID with more than one page view ID
	

begin transaction;

delete from scratch.web_page_context 
using web_page_context_dl s
where web_page_context.root_id = s.root_id; 



insert into scratch.web_page_context 
select * from web_page_context_dl;

end transaction;


drop table web_page_context_dl;
