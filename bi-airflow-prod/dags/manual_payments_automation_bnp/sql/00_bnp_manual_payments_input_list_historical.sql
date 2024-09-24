DELETE FROM payment.bnp_manual_payments_input_list_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_input_list_historical
SELECT 
CURRENT_DATE AS report_date
 ,book_date
 ,value_date
 ,company_name
 ,amount
 ,bank_reference
 ,account_number
 ,supplementary_details
 ,remi_remittance_information
 ,ordp_name_ordering_party_name
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,obk_bic_or_local_code_ordering_bank
FROM payment.bnp_manual_payments_input_list
;