import json
import logging

from pandas import DataFrame

from business_logic.manual_review_us.config.payload import (
    billing_address_config, payment_config, shipping_address_config,
    total_amount_config, user_config)
from business_logic.manual_review_us.helper import (apply_json_load_column,
                                                    remove_spaces_lower_string)

logger = logging.getLogger(__name__)


def voucher_and_discount(row):
    """
    This function helps to get voucher code and discount amount from adjustment
    """
    adj = row['raw_data']['adjustment']
    row['voucher_code'] = None if not adj.get('voucher') else adj.get('voucher', {}).get('code')
    row['discount_amount_in_cents'] = None if not adj.get('voucher') else \
        adj.get('voucher', {}).get('discount_amount', {}).get('in_cents')

    return row


def ip_address_and_additional_address_info(row):
    """
    This function helps to get additional_address_info and ip_address
    from flex log raw_data
    """
    if row['raw_data'] is not None:
        if row['raw_data'].get('payload') is not None:
            payload = row['raw_data'].get('payload')
            if payload.get('shipping_address') is not None:
                row['additional_info'] = payload['shipping_address'].get('additional_info')
            if payload.get('user') is not None:
                row['ip_address'] = payload['user'].get('ip_address')
    else:
        row['additional_info'] = None
        row['ip_address'] = None

    return row


def credit_card_and_paypal(row):
    """
    This function loop through the payment configuration and help to get
    the fields related to credit_carf and paypal from payment_metadata
    """
    payment_load = row['payment_metadata']
    for field in payment_config:
        if payment_load.get(field.get('name')) is not None:
            row[field.get('name')] = payment_load.get(field.get('name'))

        else:
            row[field.get('name')] = None

    return row


def signals(row):
    """
    This function help to ger signals that are in sen email data info
    """
    info_load = row['data']['info']
    if info_load is not None:
        if info_load.get('fingerprint') is not None:
            row['signals'] = info_load['fingerprint'].get('signals')

    else:
        row['signals'] = None

    return row


def line_items_variants(row):
    """
    this function return the list of products we have in line_items list
    """
    list_variants = []
    for ele in row['line_items']:
        quantity = ele.get('quantity')
        committed_months = ele.get('committed_months')
        variant_name = ele.get('variant').get('name')
        line_items_variant = ','.join([
            '(',
            f'Q:{quantity}',
            f'CM:{committed_months}',
            f'product:{variant_name}',
            ')'])
        list_variants.append(line_items_variant)
        list_variants = [item.replace('(,', '(') for item in list_variants]
        list_variants = [item.replace(',)', ')') for item in list_variants]
    row['product_variant'] = list_variants
    return row


def extract_scores_reason(row):
    raw_precise_data = row['raw_precise_data']
    if raw_precise_data:
        summary = raw_precise_data.get('summary')
        if summary is not None:
            reasons = summary['scores']['reasons'].get('reason')
            reason_value = [el.get('value') for el in reasons]
            row['comments'] = ' \n '.join(reason_value)
        else:
            row['comments'] = ''
    else:
        row['comments'] = ''
    return row


def flatten_flex_order(df_input, payload_column) -> DataFrame:
    """
    This function help to flatten all the needed information from flex_order
    args:
        df_input: the dataframe after reading and deduplicating data from flex_order table, datframe
        payload_column: the column we need to flatten, str
    returns:
        df_output: dataframe, with the final columns needed
    """
    final_columns = [
        'created_at',
        'order_id',
        'customer_id',
        'updated_at',
        'firstname',
        'lastname',
        'birthdate',
        'email',
        'phone',
        'user_type',
        'shipping_street',
        'shipping_state',
        'shipping_zipcode',
        'shipping_city',
        'billing_street',
        'billing_city',
        'billing_state',
        'billing_zipcode',
        'voucher_code',
        'discount_amount_in_cents',
        'additional_info',
        'in_cents',
        'nbr_line_items',
        'product_variant'
    ]
    df_output = df_input.copy()
    df_output[payload_column] = apply_json_load_column(df_output[payload_column])
    logger.info('Flattening user information.')
    for field in user_config:
        df_output[field.get('name')] = df_output[payload_column].apply(
            lambda x: remove_spaces_lower_string(x.get('user').get(field.get('name'))))
    logger.info('Flattening shipping address information.')
    for field in shipping_address_config:
        df_output[field.get('name')] = df_output[payload_column].apply(
            lambda x: x.get(field.get('from')).get(field.get('name'))
            if field.get('from') is not None else x.get(field.get('name')))
    df_output.rename(
        columns={
            'address1': 'shipping_street',
            'city': 'shipping_city',
            'state': 'shipping_state',
            'zipcode': 'shipping_zipcode'
        }, inplace=True)
    logger.info('Flattening billing address information')
    for field in billing_address_config:
        df_output[field.get('name')] = df_output[payload_column].apply(
            lambda x: x.get(field.get('from')).get(field.get('name'))
            if field.get('from') is not None else x.get(field.get('name')))
    df_output.rename(
        columns={
            'address1': 'billing_street',
            'city': 'billing_city',
            'state': 'billing_state',
            'zipcode': 'billing_zipcode'
        }, inplace=True)
    logger.info('Flattening voucher.')
    df_output = df_output.apply(voucher_and_discount, axis=1)
    for field in total_amount_config:
        df_output[field.get('name')] = df_output[payload_column].apply(
                lambda x: x.get(field.get('from')).get(field.get('name')))
    df_output['nbr_line_items'] = df_output[payload_column].apply(
        lambda x: len(x['line_items']) if x['line_items'] is not None else 0)
    df_output['line_items'] = df_output[payload_column].apply(
        lambda x: x['line_items'] if x['line_items'] is not None else 0)
    logger.info('Construct the line items product list')
    df_output = df_output.apply(line_items_variants, axis=1)
    return df_output[final_columns]


def flatten_flex_log(df_input, payload_column) -> DataFrame:
    """
    This function help to flatten all the needed information from flex_log
    args:
        df_input: the dataframe after reading and deduplicating data from flex_log table, datframe
        payload_column: the column we need to flatten, str
    returns:
        df_output: dataframe, with the final columns needed
    """
    final_columns = [
        'order_id',
        'ip_address',
        'additional_info'
    ]
    df_output = df_input.copy()
    df_output[payload_column] = apply_json_load_column(df_output[payload_column])
    logger.info('Flattening raw data for ip_address info.')
    df_output = df_output.apply(ip_address_and_additional_address_info, axis=1)
    df_output = df_output.drop_duplicates(subset=['order_id'])
    return df_output[final_columns]


def flatten_order_payment_method(df_input, payload_column) -> DataFrame:
    """
    This function help to flatten all the needed information from order_payment_method
    args:
        df_input: dataframe, after reading and deduplicating data from order_payment_method table
        payload_column: the column we need to flatten, str
    returns:
        df_output: dataframe, with the final columns needed
    """
    final_columns = [
        'order_id',
        'payment_type',
        'credit_card_bin',
        'credit_card_type',
        'credit_card_bank_name',
        'credit_card_holder_name',
        'credit_card_expiry_month',
        'credit_card_expiry_year',
        'credit_card_country_code',
        'paypal_email',
        'paypal_verified',
        'paypal_last_name',
        'paypal_first_name'
    ]
    df_output = df_input.copy()
    df_output[payload_column] = apply_json_load_column(df_output[payload_column])
    logger.info('Flattening credit_card and paypal info.')
    df_output = df_output.apply(credit_card_and_paypal, axis=1)
    return df_output[final_columns]


def flatten_nethone_data(df_input, payload_column) -> DataFrame:
    """
    This function help to flatten all the needed information from nethone_data
    args:
        df_input: dataframe, after reading and deduplicating data from nethone_data table
        payload_column: the column we need to flatten, str
    returns:
        df_output: dataframe, with the final columns needed
    """
    df_output = df_input.copy()
    df_output[payload_column] = df_output[payload_column].apply(
        lambda x: json.loads(x) if len(x) != 0 else None)
    logger.info('Flattening rdata for signals information.')
    df_output = df_output.apply(signals, axis=1)
    return df_output[['customer_id', 'signals']]


def flatten_precise_data(df_input, payload_column) -> DataFrame:
    """
    This function help tp flatten the raw_precise_data column from exparian_us_precise table
    args:
        df_input: dataframe, after reading and deduplicating data
        payload_column: the column to flatten
    returns:
        df_output: the final dataframe with the needed columns
    """
    final_columns = [
        'customer_id',
        'fpdscore',
        'precise_id_score',
        'comments']
    df_output = df_input.copy()
    df_output[payload_column] = apply_json_load_column(df_output[payload_column])
    logger.info('Flattening precise raw data.')
    df_output = df_output.apply(extract_scores_reason, axis=1)
    return df_output[final_columns]
