delete from staging.spectrum_polls_votes
where consumed_at >= current_date-2;

insert into staging.spectrum_polls_votes
select  distinct
consumed_at ,
event_name,
json_extract_path_text(payload,'poll_slug') poll_slug,
json_extract_path_text(payload,'user_id') customer_id,
json_extract_path_text(payload,'poll_version_id') poll_version_id,
json_extract_path_text(payload,'poll_id') poll_id,
json_extract_path_text(payload,'vote_slug') vote_slug,
json_extract_path_text(payload,'comment') "comment",
json_extract_path_text(payload,'entity_id') entity_id,
json_extract_path_text(payload,'entity_type') entity_type,
json_extract_path_text(payload,'created_at') created_at
 from s3_spectrum_kafka_topics_raw.polls_votes s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
AND consumed_at >= current_date -2;
