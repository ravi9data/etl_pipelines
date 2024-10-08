gsheet_additional_columns = [
    'DECISION',
    'anomaly_score'
]

columns_mapping = {
    'created_at': 'date',
    'customer_id': 'user_id',
    'order_id': 'order_id',
    'processed': 'PROCESSED',
    'DECISION': 'DECISION',
    'reason': 'REASON',
    'overlimit': 'OVERLIMIT',
    'total_orders': 'TO',
    'declined_orders': 'DO',
    'approved_orders': 'AO',
    'cancelled_orders': 'CO',
    'ffp': 'FFP',
    'precise_id_score': 'experian_score',
    'fpdscore': 'fpd_score',
    'limit': 'LIMIT',
    'reviewer_feedback': 'REVIEWER FEEDBACK',
    'issues': 'issues',
    'comments': 'comments',
    'credit_card_bank_name': 'credit_card',
    'credit_card_match': 'credit_card_match',
    'paypal_match': 'paypal',
    'num_diff_cc': 'n_cc',
    'num_diff_pp': 'n_pp',
    'total_amount': 'total_amount',
    'discount_amount': 'discount_amount',
    'voucher_code': 'discount_code',
    'verification_state': 'id_verification',
    'firstname': 'first_name',
    'lastname': 'last_name',
    'shipping_street': 'Street',
    'shipping_city': 'city',
    'shipping_state': 'state',
    'shipping_zipcode': 'zipcode',
    'billing_street': 'billing_street',
    'billing_city': 'billing_city',
    'billing_state': 'billing_state',
    'billing_zipcode': 'billing_zipcode',
    'ssn_id': 'ssn',
    'birth_date': 'birthdate',
    'phone': 'phone',
    'user_type': 'user_type',
    'age': 'AGE',
    'email': 'email',
    'ip_address': 'ip_address',
    'phone_number': 'phone_number',
    'transaction_id': 'seon reference',
    'signals': 'nethone signals',
    'identity_network_score': 'identity_network_score',
    'identity_risk_score': 'identity_risk_score',
    'primary_address_first_seen_days': 'address_first_seen_days',
    'primary_address_to_name': 'address_to_name',
    'primary_address_validity_level': 'address_validity_level',
    'primary_email_first_seen_days': 'email_first_seen_days',
    'primary_email_to_name': 'email_to_name',
    'primary_phone_line_type': 'phone_line_type',
    'primary_phone_to_name': 'phone_to_name',
    'nbr_line_items': 'num_products',
    'product_variant': 'list_products',
    'nb_addr_matching_customer_id': 'COUNT OF SAME ADDRESS BUT DIFF USER',
    'addr_matching_customer_id': 'users_matching_addrr',
    'anomaly_score': 'anomaly_score',
    'score': 'FICO',
    'asv': 'asv',
    'start_date_of_first_subscription': 'start_date_of_first_subscription',
    'outstanding_payment': 'Outstanding Payments'
}

final_columns = [
    'date',
    'user_id',
    'order_id',
    'PROCESSED',
    'DECISION',
    'REASON',
    'OVERLIMIT',
    'TO',
    'DO',
    'AO',
    'CO',
    'FFP',
    'experian_score',
    'fpd_score',
    'LIMIT',
    'REVIEWER FEEDBACK',
    'issues',
    'comments',
    'credit_card',
    'credit_card_match',
    'paypal',
    'n_cc',
    'n_pp',
    'total_amount',
    'discount_amount',
    'discount_code',
    'id_verification',
    'first_name',
    'last_name',
    'Street',
    'city',
    'state',
    'zipcode',
    'billing_street',
    'billing_city',
    'billing_state',
    'billing_zipcode',
    'ssn',
    'birthdate',
    'phone',
    'user_type',
    'AGE',
    'email',
    'ip_address',
    'phone_number',
    'seon reference',
    'nethone signals',
    'identity_network_score',
    'identity_risk_score',
    'address_first_seen_days',
    'address_to_name',
    'address_validity_level',
    'email_first_seen_days',
    'email_to_name',
    'phone_line_type',
    'phone_to_name',
    'num_products',
    'list_products',
    'COUNT OF SAME ADDRESS BUT DIFF USER',
    'users_matching_addrr',
    'anomaly_score',
    'FICO',
    'asv',
    'start_date_of_first_subscription',
    'Outstanding Payments'
]
