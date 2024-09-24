drop table if exists dm_commercial.order_decision;
create table dm_commercial.order_decision as
with order_decision as
         (
             select
                 q_.*,
                 row_number() over (partition by order_number order by event_timestamp desc)  as idx
             from stg_kafka_events_full.stream_internal_risk_order_decisions_v3 q_
         ),
     grouping_risk as
         (
             select distinct *
             from order_decision
             where idx = 1
         )
        ,approved as
         (
             select distinct
                 order_number as order_id,
                 decision,
                 decision_message,
                 to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as decision_date,
                 case when decision = 'decline' then decision_message else null end as declined_reason,
                 case when decision = 'decline' then decision_date end as declined_date,
                 case when decision = 'approve' then decision_date end as approved_date
             from grouping_risk
         )
select * from approved;

	GRANT SELECT ON dm_commercial.order_decision TO tableau;
	GRANT SELECT ON dm_commercial.order_decision TO GROUP BI;
