truncate table ods_operations.intercom_admin;
insert into ods_operations.intercom_admin
select admin_id, teams_names, teams_ids
from ods_data_sensitive.v_intercom_admins where admin_id is not null;
