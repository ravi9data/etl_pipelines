bucket_name = "grover-eu-central-1-production-data-raw-sensitive"
experian_query_path = "credit_reporting/input_dir/experian/experian_reporting.sql"
experian_report_path = "/credit_reporting/output_dir/experian/date_re/"
rename_dict = {
    'customer_id': 'customer_unique_reference',
    'customer_initials': 'initials',
    'last_name': 'surname',
    'last_name_prefix': 'surname_prefix',
    'customer_birthdate': 'date_of_birth',
    'shipping_street_name': 'street_name',
    'shipping_postal_code': 'postal_code',
    'shipping_city': 'place_of_residence',
    'customer_phone_number_1': 'telephone_number_1',
    'customer_phone_number_2': 'telephone_number_2',
    'customer_email': 'email_address',
    'subscription_id': 'loan_unique_reference'
}
columns_to_keep = [
    "experian_client_number",
    "activity",
    "record_type",
    "customer_unique_reference",
    "loan_unique_reference",
    "initials",
    "surname_prefix",
    "surname",
    "date_of_birth",
    "street_name",
    "house_number",
    "house_number_extension",
    "postal_code",
    "place_of_residence",
    "telephone_number_1",
    "telephone_number_2",
    "email_address",
    "application_date",
    "credit_limit",
    "capital_amount",
    "residual_amount",
    "scheduled_payment_date",
    "date_of_last_payment",
    "payment_amount_due",
    "last_payment_amount",
    "payment_status",
    "settlement_status",
    "settlement_date"
    ]
