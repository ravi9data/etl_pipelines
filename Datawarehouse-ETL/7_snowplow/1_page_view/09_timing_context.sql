insert into scratch.web_timing_context
select * from scratch.web_timing_context_incremental
where page_view_id not in (Select page_view_id from scratch.web_timing_context);

drop table scratch.web_timing_context_incremental;