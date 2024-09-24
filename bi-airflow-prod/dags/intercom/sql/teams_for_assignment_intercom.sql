drop table if exists stg_external_apis.teams_for_assignment_intercom;

create table stg_external_apis.teams_for_assignment_intercom as
select "team assigned id", 
"team assigned name", 
"should we include them in actual list of teams assigned",
"team participated" 
from staging.ab_teams_for_assignment_intercom_sheet1;
