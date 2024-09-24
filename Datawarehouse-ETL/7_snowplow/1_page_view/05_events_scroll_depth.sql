insert into scratch.web_events_scroll_depth
select * from scratch.web_events_scroll_depth_incremental
where page_view_id not in (Select page_view_id from scratch.web_events_scroll_depth);

drop table scratch.web_events_scroll_depth_incremental;