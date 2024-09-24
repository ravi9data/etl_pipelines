
drop table if exists trans_dev.failed_payment_reasons_us;
create table trans_dev.failed_payment_reasons_us as 
with failed_reason_temp as(
select 
	a.account_to as group_uuid,
	a.status as payment_status,
	a.failed_reason AS failedreason,
	ROW_NUMBER() OVER (PARTITION BY account_to ORDER BY a.created_at::timestamp desc) idx
from oltp_billing.transaction a
where a.status = 'failed'
),
failed_reason_ as 
(
select * from failed_reason_temp where idx= 1
)
select 
	payment_id ,
	status ,
	t.group_uuid ,
	t.payment_status as failed_status,
	t.failedreason
from 
master.subscription_payment sp 
	left join
	failed_reason_ t
	on t.group_uuid = SPLIT_PART(sp.payment_id,'_',1) 
where 
	sp.store_label = 'Grover - United States online'
	and status = 'FAILED'
;
