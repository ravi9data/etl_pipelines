truncate table stg_external_apis_dl.sc_ib_rejected_deliveries;

insert into stg_external_apis_dl.sc_ib_rejected_deliveries
select
	"reported date"::timestamp without time zone
	,supplier
	,pr 
	,"reason for rejection"
	,warehouse
from staging.rejected_deliveries;