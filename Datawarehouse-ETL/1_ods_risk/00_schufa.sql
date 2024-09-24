drop table if exists trans_dev.schufa_data_raw_json_corrected;

create table trans_dev.schufa_data_raw_json_corrected as 
select s.* ,
row_number() over (partition by id order by updated_at  desc) as idx
from s3_spectrum_rds_dwh_order_approval.schufa_data s 
where f_json_ok(raw_data) ;

drop table if exists ods_data_sensitive.schufa;

create table ods_data_sensitive.schufa as 
with a as (
select  
 id::int as id,raw_data,customer_id::int as customer_id,created_at::TIMESTAMP WITHOUT TIME ZONE  as creation_timestamp,
 updated_at::TIMESTAMP WITHOUT TIME ZONE as updated_at ,address_key,
 json_extract_path_text(raw_data,'soapenv:Body','bon:Bonitaetsauskunft','Reaktion') as raw_prep
from trans_dev.schufa_data_raw_json_corrected a
where idx=1
)
, b as (
select distinct a.*
,json_extract_path_text(raw_prep,'Verbraucherdaten','Vorname')::varchar(44) as schufa_first_name
,json_extract_path_text(raw_prep,'Verbraucherdaten','Nachname')::varchar(46) as schufa_last_name
,json_extract_path_text(raw_prep,'Verbraucherdaten','Geschlecht')::varchar(1) as schufa_gender
,json_extract_path_text(raw_prep,'Verbraucherdaten','Geburtsdatum')::varchar(10) as schufa_date_of_birth
,json_extract_path_text(raw_prep,'Verbraucherdaten','AktuelleAdresse','Land')::varchar(3) as schufa_current_country
,json_extract_path_text(raw_prep,'Verbraucherdaten','AktuelleAdresse','Ort')::varchar(44) as schufa_current_city
,json_extract_path_text(raw_prep,'Verbraucherdaten','AktuelleAdresse','PLZ')::varchar(10) as schufa_current_plz
,json_extract_path_text(raw_prep,'Verbraucherdaten','AktuelleAdresse','Strasse')::varchar(46) as schufa_current_street
,json_extract_path_text(raw_prep,'Teilnehmerkennung')::varchar(44) as schufa_teilnehmerkennung
,json_extract_path_text(raw_prep,'Scoreinformationen') as schufa_Scoreinformationen_raw
,COALESCE(json_extract_path_text(raw_prep,'Scoreinformationen','Scoreinformation','Daten','Score','Scorebereich'), s.score_range) as schufa_class
,COALESCE(json_extract_path_text(raw_prep,'Scoreinformationen','Scoreinformation','Daten','Score','Scorewert'), 
  LEFT(s.schufa_score, CASE WHEN POSITION('.' IN s.schufa_score) = 0 
                           THEN LENGTH(s.schufa_score) 
                           ELSE POSITION('.' IN s.schufa_score)-1 
                       END)
 ) as schufa_score
,json_extract_path_text(raw_prep,'Scoreinformationen','Scoreinformation','Daten','Score','Risikoquote') as schufa_risk
,json_extract_path_text(raw_prep,'Scoreinformationen','Scoreinformation','Daten','Score','Scoreinfotext') as schufa_infotext
,json_extract_path_text(raw_prep,'Verarbeitungsinformation','Text') as schufa_Verarbeitungsinformation_text
,json_extract_path_text(raw_prep,'Verarbeitungsinformation','Ergebnistyp') as schufa_Verarbeitungsinformation_Ergebnistyp
,json_extract_path_text(raw_prep,'MerkmalsListe') as schufa_MerkmalsListe
,case when score_details like ('%24 GEPRUEFTE IDENTITAET%') then true else false end as is_identity_confirmed
,case when score_details like ('%MISSBRAUCHSMERKMALE%' ) or score_details LIKE ('%VERTRAGSWIDRIGEM%') then true else false end as is_negative_remarks
,s.paid_debt as schufa_paid_debt
,s.total_debt as schufa_total_debt
from s3_spectrum_rds_dwh_order_approval.schufa_data s 
left join a 
   on a.customer_id=s.customer_id
)
select *
from b;

drop table trans_dev.schufa_data_raw_json_corrected;
