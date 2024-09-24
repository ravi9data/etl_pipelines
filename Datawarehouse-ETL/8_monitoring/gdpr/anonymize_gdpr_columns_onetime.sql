/*
This script masks PII columns with 'XXX..' and in case of integer columns (Age etc.) to NULL.
*/
------------------------------------------------------------------------------------------
/*
Schema: dm_finance
*/
------------------------------------------------------------------------------------------

-- dm_finance.customer_master_datatape_20200229
update dm_finance.customer_master_datatape_20200229
set
age = null,
gender = REPLICATE('x', LEN(t.gender))
from dm_finance.customer_master_datatape_20200229 t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;

------------------------------------------------------------------------------------------
/*
Schema: Master
*/
------------------------------------------------------------------------------------------
-- master.customer
/*
update master.customer
set age = null,
gender = REPLICATE('x', LEN(t.gender))
from master.customer t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


-- master.customer_historical
update master.customer_historical
set age = null,
gender = REPLICATE('x', LEN(t.gender))
from master.customer_historical t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;
*/

------------------------------------------------------------------------------------------
/*
Schema: ods_production
Note: ALl tables in this schema is no longer part of the masking scope (see documentation in Confluence)
*/
------------------------------------------------------------------------------------------
/*
--ods_production.adyen_payment_requests
update ods_production.adyen_payment_requests
set shopper_email = REPLICATE('x', LEN(t.shopper_email))
from ods_production.adyen_payment_requests t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


--ods_production.customer
update ods_production.customer
set age = null,
gender = REPLICATE('x', LEN(t.gender))
from ods_production.customer t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;

--ods_production.payment_info
update ods_production.payment_info
set email = REPLICATE('x', LEN(t.email))
from ods_production.payment_info t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


--ods_production.schufa
update ods_production.schufa
set
schufa_first_name = REPLICATE('x', LEN(t.schufa_first_name)),
schufa_last_name = REPLICATE('x', LEN(t.schufa_last_name)),
schufa_gender = REPLICATE('x', LEN(t.schufa_gender)),
schufa_date_of_birth = REPLICATE('x', LEN(t.schufa_date_of_birth)),
schufa_current_city = REPLICATE('x', LEN(t.schufa_current_city)),
schufa_current_plz = REPLICATE('x', LEN(t.schufa_current_plz)),
schufa_current_street = REPLICATE('x', LEN(t.schufa_current_street)),
from ods_production.schufa sc
inner join ods_data_sensitive.pii_deleted_users pii  on sc.customer_id = pii.customer_id;
*/
------------------------------------------------------------------------------------------
/*
Schema: ods_data_sensitive
Note: ALl tables in this schema is no longer part of the masking scope (see documentation in Confluence)
*/
------------------------------------------------------------------------------------------
/*
-- ods_data_sensitive.customer_pii
update ods_data_sensitive.customer_pii
set
age = null,
birthdate = null,
ip_address_list = REPLICATE('x', LEN(t.ip_address_list)),
paypal_email = REPLICATE('x', LEN(t.paypal_email)),
phone_number = REPLICATE('x', LEN(t.phone_number)),
email = REPLICATE('x', LEN(t.email)),
gender = REPLICATE('x', LEN(t.gender))
from ods_data_sensitive.customer_pii t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


-- ods_data_sensitive.ixopay_transactions
update ods_data_sensitive.ixopay_transactions
set
shopper_email = REPLICATE('x', LEN(t.shopper_email)),
creditcard = REPLICATE('x', LEN(t.creditcard))
from ods_data_sensitive.ixopay_transactions t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


-- ods_data_sensitive.payment_order_pii
update ods_data_sensitive.payment_order_pii
set
ip_address_order = REPLICATE('x', LEN(t.ip_address_order)),
birthdate = null,
first_name = REPLICATE('x', LEN(t.first_name)),
last_name = REPLICATE('x', LEN(t.last_name)),
paypal_email = REPLICATE('x', LEN(t.paypal_email)),
shopper_email = REPLICATE('x', LEN(t.shopper_email)),
customer_paypal_email = REPLICATE('x', LEN(t.customer_paypal_email)),
email = REPLICATE('x', LEN(t.email)),
phone_number = REPLICATE('x', LEN(t.phone_number)),
billingstreet = REPLICATE('x', LEN(t.billingstreet)),
billingcity = REPLICATE('x', LEN(t.billingcity)),
billingpostalcode = REPLICATE('x', LEN(t.billingpostalcode)),
shippingstreet = REPLICATE('x', LEN(t.shippingstreet)),
shippingcity = REPLICATE('x', LEN(t.shippingcity)),
shippingpostalcode = REPLICATE('x', LEN(t.shippingpostalcode))
from ods_data_sensitive.payment_order_pii t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;
*/

------------------------------------------------------------------------------------------
/*
Schema: monitoring
*/
------------------------------------------------------------------------------------------
/*update monitoring.oo2
set
customer_email__c = REPLICATE('x', LEN(oo.customer_email__c))
from monitoring.oo2 oo
inner join ods_data_sensitive.pii_deleted_users pii  on oo.customer_email__c = pii.customer_email;*/


------------------------------------------------------------------------------------------
/*
Schema: skyvia
*/
------------------------------------------------------------------------------------------
/*
update skyvia.customer_crm_segment
set
email = REPLICATE('x', LEN(t.email))
from skyvia.customer_crm_segment t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;
*/


------------------------------------------------------------------------------------------
/*
Schema: stg_api_production
*/
------------------------------------------------------------------------------------------


/*update stg_api_production.adyen_payment_requests
set
email = REPLICATE('x', LEN(t.email))
from stg_api_production.adyen_payment_requests t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_email;*/


/*update stg_api_production.employees
set
email = REPLICATE('x', LEN(emp.email))
from stg_api_production.employees emp
inner join ods_data_sensitive.pii_deleted_users pii  on emp.email = pii.customer_email;*/


/*update stg_api_production.partner_forms
set
phone_number = REPLICATE('x', LEN(pf.phone_number)),
email = REPLICATE('x', LEN(pf.email))
from stg_api_production.partner_forms pf
inner join ods_data_sensitive.pii_deleted_users pii  on pf.email = pii.customer_email;*/

/*
update stg_api_production.spree_addresses
set
firstname = REPLICATE('x', LEN(t.firstname)),
lastname = REPLICATE('x', LEN(t.lastname)),
address1 = REPLICATE('x', LEN(t.address1)),
address2 = REPLICATE('x', LEN(t.address2)),
city = REPLICATE('x', LEN(t.city)),
zipcode = REPLICATE('x', LEN(t.zipcode)),
phone = REPLICATE('x', LEN(t.phone)),
alternative_phone = REPLICATE('x', LEN(t.alternative_phone))
from stg_api_production.spree_addresses t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id=pii.customer_id;


update stg_api_production.spree_orders
set
email = REPLICATE('x', LEN(t.email)),
last_ip_address = REPLICATE('x', LEN(t.last_ip_address))
from stg_api_production.spree_orders t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_email;


update stg_api_production.spree_paypal_express_checkouts
set
payer_email = REPLICATE('x', LEN(t.payer_email))
from stg_api_production.spree_paypal_express_checkouts t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_email;


update stg_api_production.spree_users
set
last_sign_in_ip = REPLICATE('x', LEN(t.last_sign_in_ip)),
current_sign_in_ip = REPLICATE('x', LEN(t.current_sign_in_ip)),
birthdate = null,
first_name = REPLICATE('x', LEN(t.first_name)),
last_name = REPLICATE('x', LEN(t.last_name)),
phone_number = REPLICATE('x', LEN(t.phone_number)),
email = REPLICATE('x', LEN(t.email)),
gender = REPLICATE('x', LEN(t.gender))
from stg_api_production.spree_users t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


update stg_api_production.waiting_list_entries
set
email = REPLICATE('x', LEN(wle.email))
from stg_api_production.waiting_list_entries wle
inner join ods_data_sensitive.pii_deleted_users pii  on wle.email = pii.customer_email;*/


------------------------------------------------------------------------------------------
/*
Schema: stg_detectingfrankfurt
*/
------------------------------------------------------------------------------------------

update stg_detectingfrankfurt.possible_fraudsters
set
first_name = REPLICATE('x', LEN(t.first_name)),
last_name = REPLICATE('x', LEN(t.last_name)),
phone_number = REPLICATE('x', LEN(t.phone_number)),
email_address = REPLICATE('x', LEN(t.email_address))
from stg_detectingfrankfurt.possible_fraudsters t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


------------------------------------------------------------------------------------------
/*
Schema: stg_events
*/
------------------------------------------------------------------------------------------

set
ip_address = REPLICATE('x', LEN(t.ip_address))
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_id;


update stg_events.product_views
set
ip_address = REPLICATE('x', LEN(t.ip_address))
from stg_events.product_views t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_id;


update stg_events.registration_view
set
email = REPLICATE('x', LEN(t.email)),
ip_address = REPLICATE('x', LEN(t.ip_address))
from stg_events.registration_view t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_id;


update stg_events.subscription_plan_impressions
set
ip_address = REPLICATE('x', LEN(t.ip_address))
from stg_events.subscription_plan_impressions t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_id;


-----------------------------------------------------------------------------------------
/*
Schema: stg_external_apis
*/
------------------------------------------------------------------------------------------

/*
update stg_external_apis.aircall_call
set
voicemail = REPLICATE('x', LEN(t.raw_digits))
from stg_external_apis.aircall_call t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_id;


update stg_external_apis.aircall_user
set
name = REPLICATE('x', LEN(t.name)),
email = REPLICATE('x', LEN(t.email))
from stg_external_apis.aircall_user t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_email;
*/

update stg_external_apis.braze_click_event
set
email_address = REPLICATE('x', LEN(t.email_address))
from stg_external_apis.braze_click_event t
inner join ods_data_sensitive.pii_deleted_users pii  on t.external_user_id = pii.customer_id;


update stg_external_apis.braze_open_event
set
email_address = REPLICATE('x', LEN(t.email_address))
from stg_external_apis.braze_open_event t
inner join ods_data_sensitive.pii_deleted_users pii  on t.external_user_id = pii.customer_id;


update stg_external_apis.braze_sent_event
set
email_address = REPLICATE('x', LEN(t.email_address))
from stg_external_apis.braze_sent_event t
inner join ods_data_sensitive.pii_deleted_users pii  on t.external_user_id = pii.customer_id;


update stg_external_apis.braze_unsubscribe_event
set
email_address = REPLICATE('x', LEN(t.email_address))
from stg_external_apis.braze_unsubscribe_event t
inner join ods_data_sensitive.pii_deleted_users pii  on t.external_user_id = pii.customer_id;


update stg_external_apis.customer_survey_data
set
email = REPLICATE('x', LEN(t.email))
from stg_external_apis.customer_survey_data t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


update stg_external_apis.ixopay_transactions
set creditcard = REPLICATE('x', LEN(t.creditcard)),
customer = null
from stg_external_apis.ixopay_transactions t
inner join ods_data_sensitive.pii_deleted_users pii  on SUBSTRING(t.customer,20,6) = pii.customer_id;

update stg_external_apis.intercom_contacts
set
email = REPLICATE('x', LEN(t.email)),
phone = REPLICATE('x', LEN(t.phone)),
name = REPLICATE('x', LEN(t.name))
from stg_external_apis.intercom_contacts t
inner join ods_data_sensitive.pii_deleted_users pii  on t.external_id = pii.customer_id;

------------------------------------------------------------------------------------------
/*
Schema: stg_historical
*/
------------------------------------------------------------------------------------------
update stg_historical.account
set
name = REPLICATE('x', LEN(t.name)),
billingstreet = REPLICATE('x', LEN(t.billingstreet)),
billingcity = REPLICATE('x', LEN(t.billingcity)),
billingstate = REPLICATE('x', LEN(t.billingstate)),
billingpostalcode = REPLICATE('x', LEN(t.billingpostalcode)),
billinglatitude = null,
billinglongitude = null,
shippingstreet = REPLICATE('x', LEN(t.shippingstreet)),
shippingcity = REPLICATE('x', LEN(t.shippingcity)),
shippingstate = REPLICATE('x', LEN(t.shippingstate)),
shippingpostalcode = REPLICATE('x', LEN(t.shippingpostalcode)),
shippinglatitude = null,
shippinglongitude = null,
phone = REPLICATE('x', LEN(t.phone)),
birthdate__c = null,
email__c = REPLICATE('x', LEN(t.email__c)),
name_normalized__c = REPLICATE('x', LEN(t.name_normalized__c))
from stg_historical.account t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;



update stg_historical.api_production_spree_users
set
last_sign_in_ip = REPLICATE('x', LEN(t.last_sign_in_ip)),
current_sign_in_ip = REPLICATE('x', LEN(t.current_sign_in_ip)),
birthdate = null,
first_name = REPLICATE('x', LEN(t.first_name)),
last_name = REPLICATE('x', LEN(t.last_name)),
phone_number = REPLICATE('x', LEN(t.phone_number)),
email = REPLICATE('x', LEN(t.email)),
gender = REPLICATE('x', LEN(t.gender))
from stg_historical.api_production_spree_users t
inner join ods_data_sensitive.pii_deleted_users pii  on t.id = pii.customer_id;

update stg_historical.contact
set
lastname = REPLICATE('x', LEN(t.lastname)),
firstname = REPLICATE('x', LEN(t.firstname)),
middlename = REPLICATE('x', LEN(t.middlename)),
name = REPLICATE('x', LEN(t.name)),
mailingstreet = REPLICATE('x', LEN(t.mailingstreet)),
mailingcity = REPLICATE('x', LEN(t.mailingcity)),
mailingpostalcode = REPLICATE('x', LEN(t.mailingpostalcode)),
mailinglatitude = null,
mailinglongitude = null,
phone = REPLICATE('x', LEN(t.phone)),
fax = REPLICATE('x', LEN(t.fax)),
mobilephone = REPLICATE('x', LEN(t.mobilephone)),
email = REPLICATE('x', LEN(t.email)),
birthdate = null
from stg_historical.contact t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;



update stg_historical.salesforce_contact
set
birthdate = null,
otherphone = REPLICATE('x', LEN(t.otherphone)),
otheraddress = REPLICATE('x', LEN(t.otheraddress)),
homephone = REPLICATE('x', LEN(t.homephone)),
assistantphone = REPLICATE('x', LEN(t.assistantphone)),
email = REPLICATE('x', LEN(t.email)),
mobilephone = REPLICATE('x', LEN(t.mobilephone)),
phone = REPLICATE('x', LEN(t.phone)),
mailingaddress = REPLICATE('x', LEN(t.mailingaddress))
from stg_historical.salesforce_contact t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;

update stg_historical.salesforce_account
set
next_birthday__c = null,
birthdate__c = null,
gender__c = REPLICATE('x', LEN(t.gender__c)),
email__c = REPLICATE('x', LEN(t.email__c)),
phone = REPLICATE('x', LEN(t.phone)),
shippingaddress = REPLICATE('x', LEN(t.shippingaddress)),
billingaddress = REPLICATE('x', LEN(t.billingaddress)),
age__c = null
from stg_historical.salesforce_account t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;

update stg_historical."order"
set
customer_name_shipping__c = REPLICATE('x', LEN(t.customer_name_shipping__c)),
ip_address__c = REPLICATE('x', LEN(t.ip_address__c)),
customer_phone__c = REPLICATE('x', LEN(t.customer_phone__c)),
customer_name__c = REPLICATE('x', LEN(t.customer_name__c)),
customer_email__c = REPLICATE('x', LEN(t.customer_email__c))
from stg_historical."order" t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;

------------------------------------------------------------------------------------------
/*
Schema: stg_postgress_dwh.
*/
------------------------------------------------------------------------------------------
-- Special handling for this table since ID not available in spree_users
update stg_postgress_dwh."address"
set
first_name = REPLICATE('x', LEN(t.first_name)),
last_name = REPLICATE('x', LEN(t.last_name)),
city = REPLICATE('x', LEN(t.city)),
postal_code = REPLICATE('x', LEN(t.postal_code)),
street = REPLICATE('x', LEN(t.street)),
house_number = REPLICATE('x', LEN(t.house_number)),
phone = REPLICATE('x', LEN(t.phone))
from stg_postgress_dwh."address" t
where t.id in (select id from stg_api_production.spree_addresses where deleted_at is not null);

update stg_postgress_dwh.contact
set
first_name = REPLICATE('x', LEN(t.first_name)),
last_name = REPLICATE('x', LEN(t.last_name)),
birth_date = null,
email = REPLICATE('x', LEN(t.email)),
phone = REPLICATE('x', LEN(t.phone))
from stg_postgress_dwh.contact t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


update stg_postgress_dwh.customer_sf
set
customer_name_normalised = REPLICATE('x', LEN(t.customer_name_normalised)),
customer_name = REPLICATE('x', LEN(t.customer_name))
from stg_postgress_dwh.customer_sf t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


update stg_postgress_dwh.order_web
set
last_ip_address = REPLICATE('x', LEN(t.last_ip_address))
from stg_postgress_dwh.order_web t
inner join ods_data_sensitive.pii_deleted_users pii  on t.user_id = pii.customer_id;

------------------------------------------------------------------------------------------
/*
Schema: stg_realtimebrandenburg
*/
------------------------------------------------------------------------------------------

update stg_realtimebrandenburg.boniversum_data
set
public_address = REPLICATE('x', LEN(t.public_address)),
person_summary = REPLICATE('x', LEN(t.person_summary))
from stg_realtimebrandenburg.boniversum_data t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


update stg_realtimebrandenburg.schufa_data
set
addresses = REPLICATE('x', LEN(t.addresses))
from stg_realtimebrandenburg.schufa_data t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;


------------------------------------------------------------------------------------------
/*
Schema: stg_salesforce
*/
------------------------------------------------------------------------------------------

/*update stg_salesforce.account
set
name = REPLICATE('x', LEN(t.name)),
billingstreet = REPLICATE('x', LEN(t.billingstreet)),
billingcity = REPLICATE('x', LEN(t.billingcity)),
billingpostalcode = REPLICATE('x', LEN(t.billingpostalcode)),
shippingstreet = REPLICATE('x', LEN(t.shippingstreet)),
shippingcity = REPLICATE('x', LEN(t.shippingcity)),
shippingpostalcode = REPLICATE('x', LEN(t.shippingpostalcode)),
name_normalized__c = REPLICATE('x', LEN(t.name_normalized__c)),
next_birthday__c = null,
birthdate__c = null,
gender__c = REPLICATE('x', LEN(t.gender__c)),
email__c = REPLICATE('x', LEN(t.email__c)),
phone = REPLICATE('x', LEN(t.phone)),
age__c = REPLICATE('x', LEN(t.age__c))
from stg_salesforce.account t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;



update stg_salesforce.asset
set
offered_customer_name__c = REPLICATE('x', LEN(sf.offered_customer_name__c))
from stg_salesforce.account sf
inner join ods_data_sensitive.pii_deleted_users pii  on sf.spree_customer_id__c = pii.customer_id;


update stg_salesforce.asset_request__c
set
customer_email__c = REPLICATE('x', LEN(sf.customer_email__c)),
customer_name__c = REPLICATE('x', LEN(sf.customer_name__c))
from stg_salesforce.asset_request__c sf
inner join ods_data_sensitive.pii_deleted_users pii  on sf.id = pii.customer_email;


update stg_salesforce.capital_source__c
set
address__c = REPLICATE('x', LEN(sf.address__c))
from stg_salesforce.capital_source__c sf
inner join ods_data_sensitive.pii_deleted_users pii  on sf.spree_customer_id__c = pii.customer_id;


update stg_salesforce.contact
set
lastname = REPLICATE('x', LEN(t.lastname)),
firstname = REPLICATE('x', LEN(t.firstname)),
name = REPLICATE('x', LEN(t.name)),
mailingsteet = REPLICATE('x', LEN(t.mailingsteet)),
mailingcity = REPLICATE('x', LEN(t.mailingcity)),
mailingpostalcode = REPLICATE('x', LEN(t.mailingpostalcode)),
fax = REPLICATE('x', LEN(t.fax)),
birthdate = null,
email = REPLICATE('x', LEN(t.email)),
mobilephone = REPLICATE('x', LEN(t.mobilephone)),
phone = REPLICATE('x', LEN(t.phone))
from stg_salesforce.contact t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;


update stg_salesforce.customer_asset_allocation__c
set
customer_email__c = REPLICATE('x', LEN(sf.customer_email__c))
from stg_salesforce.customer_asset_allocation__c sf
inner join ods_data_sensitive.pii_deleted_users pii  on sf.customer_email__c = pii.customer_email;


update stg_salesforce."order"
set
billingstreet = REPLICATE('x', LEN(t.billingstreet)),
billingcity = REPLICATE('x', LEN(t.billingcity)),
billingpostalcode = REPLICATE('x', LEN(t.billingpostalcode)),
shippingstreet = REPLICATE('x', LEN(t.shippingstreet)),
shippingcity = REPLICATE('x', LEN(t.shippingcity)),
shippingpostalcode = REPLICATE('x', LEN(t.shippingpostalcode)),
customer_name_shipping__c = REPLICATE('x', LEN(t.customer_name_shipping__c)),
ip_address__c = REPLICATE('x', LEN(t.ip_address__c)),
customer_phone__c = REPLICATE('x', LEN(t.customer_phone__c)),
customer_name__c = REPLICATE('x', LEN(t.customer_name__c)),
customer_email__c = REPLICATE('x', LEN(t.customer_email__c))
from stg_salesforce."order" t
inner join ods_data_sensitive.pii_deleted_users pii  on t.spree_customer_id__c = pii.customer_id;


update stg_salesforce.subscription_payment__c
set
customer_email__c = REPLICATE('x', LEN(sf.customer_email__c))
from stg_salesforce.subscription_payment__c sf
inner join ods_data_sensitive.pii_deleted_users pii  on sf.customer_email__c = pii.customer_email;


update stg_salesforce."user"
set
mobilephone = REPLICATE('x', LEN(sf.mobilephone)),
phone = REPLICATE('x', LEN(sf.phone)),
senderemail = REPLICATE('x', LEN(sf.senderemail)),
email = REPLICATE('x', LEN(sf.email))
from stg_salesforce."user" sf
inner join ods_data_sensitive.pii_deleted_users pii  on sf.spree_customer_id__c=pii.customer_id;
*/
------------------------------------------------------------------------------------------
/*
Schema: web
*/
------------------------------------------------------------------------------------------

update traffic.page_views
set
geo_city = REPLICATE('x', LEN(t.geo_city)),
geo_zipcode = REPLICATE('x', LEN(t.geo_zipcode)),
geo_latitude = null,
geo_longitude = null,
ip_address = REPLICATE('x', LEN(t.ip_address))
from traffic.page_views t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id_mapped = pii.customer_id;


update web."sessions"
set
geo_city = REPLICATE('x', LEN(t.geo_city)),
geo_zipcode = REPLICATE('x', LEN(t.geo_zipcode)),
geo_latitude = null,
geo_longitude = null,
ip_address = REPLICATE('x', LEN(t.ip_address))
from web."sessions" t
inner join ods_data_sensitive.pii_deleted_users pii  on t.customer_id = pii.customer_id;
