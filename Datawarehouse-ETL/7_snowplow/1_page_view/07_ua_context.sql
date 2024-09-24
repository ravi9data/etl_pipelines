insert into scratch.web_ua_parser_context
select * from scratch.web_ua_parser_context_incremental
where page_view_id not in (Select page_view_id from scratch.web_ua_parser_context);

drop table scratch.web_ua_parser_context_incremental;