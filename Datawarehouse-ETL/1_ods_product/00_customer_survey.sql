drop table if exists ods_data_sensitive.customer_survey;
create table ods_data_sensitive.customer_survey as
select 
	customer_id, 
	case 
	  when sd.net_income ='1.000 - 1.999 €' then '€1.000 - €1.999'
	  when sd.net_income ='2.000 - 2.999 €' then '€2.000 - €2.999'
	  when sd.net_income ='3.000 - 3.999 €' then '€3.000 - €3.999'
	  when sd.net_income ='4.000 € und mehr' then '€4.000 and more'
	  when sd.net_income ='bis 999 €' then 'Up to €999'
	  when sd.net_income ='Ich möchte keine Angabe machen' then 'Prefer not to answer'
	  else sd.net_income 
	end as income_range,
	case 
		when sd.education_level = 'Kein allgemeiner Schulabschluss' then 'No school graduation'
		when sd.education_level = 'Haupt-/Volksschule' then 'Middle school'
		when sd.education_level = 'Realschule' then 'Middle school'
		when sd.education_level = '(Fach-)Abitur' then 'High school or equivalent'
		when sd.education_level = 'High school or equivalent (e.g. GED)' then 'High school or equivalent'
		when sd.education_level = 'Abgeschlossenes Studium' then 'University degree'
		when sd.education_level = 'Bachelor''s degree (e.g. BA, BS)' then 'University degree'
		when sd.education_level = 'Master''s degree (e.g. MA, MS, MEd)' then 'University degree'
		when sd.education_level = 'Doktor' then 'Doctorate'
		when sd.education_level = 'Ich möchte keine Angabe machen' then 'Prefer not to answer'
		else sd.education_level end as education_level,
	submitted_at,
	survey_year,
	type,
	language
from stg_external_apis.customer_survey_data sd