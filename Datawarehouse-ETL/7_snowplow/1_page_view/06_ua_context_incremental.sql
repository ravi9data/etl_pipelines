create temp table web_ua_parser_context_dl (like scratch.web_ua_parser_context); 

insert into web_ua_parser_context_dl 
  -- deduplicate the UA parser context in 2 steps

  WITH prep AS (

    SELECT  distinct

      TRIM(wp.page_view_id) AS page_view_id,

      ua.useragent_family,
      ua.useragent_major,
      ua.useragent_minor,
      ua.useragent_patch,
      ua.useragent_version,
      ua.os_family,
      ua.os_major,
      ua.os_minor,
      ua.os_patch,
      ua.os_patch_minor,
      ua.os_version,
      ua.device_family

    FROM atomic.com_snowplowanalytics_snowplow_ua_parser_context_1 AS ua

    INNER JOIN scratch.web_page_context AS wp
      ON TRIM(ua.root_id) = TRIM(wp.root_id)
    where ua.root_tstamp > DATEADD(day, -7, CURRENT_DATE)

  )

  SELECT  * FROM prep WHERE page_view_id not IN (SELECT page_view_id FROM prep GROUP BY 1 HAVING COUNT(*) > 1); -- exclude all root ID with more than one page view ID


begin transaction;

delete from scratch.web_ua_parser_context 
using web_ua_parser_context_dl s
where scratch.web_ua_parser_context.page_view_id = s.page_view_id; 



insert into scratch.web_ua_parser_context 
select * from web_ua_parser_context_dl;

end transaction;


drop table web_ua_parser_context_dl;