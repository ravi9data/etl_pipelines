truncate stg_external_apis.pfm_survey_result;

                                   p,
                                  id1,
                                  subid,
                                  submittedat,
                                  token)
       p,
       id1,
       subid,
       "submitted at" as submittedat,
       token
from stg_external_apis_dl.pmf_survey_result;
