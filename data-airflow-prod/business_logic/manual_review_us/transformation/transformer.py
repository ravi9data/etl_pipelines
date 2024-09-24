import logging
from datetime import date, datetime

import pandas as pd

from business_logic.manual_review_us.config.config import empty_columns_list
from business_logic.manual_review_us.helper import make_float, make_int

logger = logging.getLogger(__name__)


def define_credit_card_bank_name(row):
    if row['credit_card_bank_name'] is None:
        if row['credit_card_bin'] == '421783':
            return 'CHIME'
        elif row['credit_card_bin'] == '498503':
            return 'STRIDE BANK NATIONAL ASSOCIATION'
        elif row['credit_card_bin'] == '487917':
            return 'CHOICE FINANCIAL GROUP'
        elif row['credit_card_bin'] == '412055':
            return 'VARO BANK NATIONAL ASSOCIATION'
        else:
            return ','.join([
                'UNKOWN BIN: ',
                str(row['credit_card_bin']),
                str(row['credit_card_type']),
                str(row['credit_card_level']),
                str(row['credit_card_holder_name'])
                ])
    else:
        return row['credit_card_bank_name']


def define_processed_orders(row):
    if row['status'] is not None or int(row['code']) <= 2000:
        return True
    else:
        return False


def define_order_status(row):
    if row['status'] is not None:
        return row['status'].strip().upper()
    elif row['code'] == '2000':
        return 'DECLINED'
    else:
        return ''


def define_expiry_date(credit_card_expiry_month, credit_card_expiry_year):
    today = date.today()
    one_or_zero = (
        (today.month, today.year) > (
            credit_card_expiry_month,
            credit_card_expiry_year)
        )
    year_difference = credit_card_expiry_year - today.year
    expires_in = year_difference - one_or_zero
    if expires_in < 1:
        return ', expires_less_than_1_year'
    else:
        return ''


def define_card_holder_match(row):
    if row['credit_card_holder_name'] is not None:
        if (row['firstname'] not in row['credit_card_holder_name'] and
           row['lastname'] not in row['credit_card_holder_name']):
            expiration_date_msg = define_expiry_date(
                    make_int(row['credit_card_expiry_month']),
                    make_int(row['credit_card_expiry_year']))
            return f'mismatch ({row["credit_card_holder_name"]}){expiration_date_msg}'
        if (
            row['firstname'] not in row['credit_card_holder_name'] and
            row['lastname'] in row['credit_card_holder_name']) or (
            row['firstname'] in row['credit_card_holder_name'] and
            row['lastname'] not in row['credit_card_holder_name']
        ):
            expiration_date_msg = define_expiry_date(
                    make_int(row['credit_card_expiry_month']),
                    make_int(row['credit_card_expiry_year']))
            return f'match ({row["credit_card_holder_name"]}) {expiration_date_msg}'
    else:
        return 'We cant find a match'


def define_verified_paypal(paypal_verified):
    if str(paypal_verified) == 'true':
        return 'verified, '
    elif str(paypal_verified) == 'false':
        return 'not_verified, '
    else:
        return None


def get_matched_paypal_users(row):
    if (row['email'] in row['paypal_email'] and row['firstname'] in row['paypal_first_name'] and
       row['lastname'] in row['paypal_last_name']):
        msg = f'{define_verified_paypal(row["paypal_verified"])} match: '
    else:
        msg = f'{define_verified_paypal(row["paypal_verified"])} mismatch: '
    if row['email'] != row['paypal_email']:
        msg = msg + str(row['paypal_email'])

    if row['paypal_last_name'] != row['lastname'] or row['paypal_first_name'] != row['firstname']:
        msg = msg + ', ' + str(row['paypal_last_name']) + ' ' + str(row['paypal_first_name'])

    return msg


def calculate_age(birthdate):
    today = date.today()
    birthdate = datetime.strptime(birthdate, "%Y-%m-%d")
    age = today.year - birthdate.year - (
        (today.month, today.day) < (birthdate.month, birthdate.day))
    return age


def transformer_data(df_input):
    """
    This function is about adding the needed transformation and the new columns to the dataframe
    """
    df_output = df_input.copy()

    logger.info('Calculate total_orders')
    df_output['total_orders'] = df_output.groupby('customer_id')['order_id'].transform('count')

    logger.info('Correct credit card name')
    df_output['credit_card_bank_name'] = df_output.apply(define_credit_card_bank_name, axis=1)

    logger.info('Calculate processed orders')
    df_output['processed'] = df_output.apply(define_processed_orders, axis=1)

    logger.info('define order status')
    df_output['new_order_status'] = df_output.apply(define_order_status, axis=1)

    logger.info('Define credit card matching')
    df_output['credit_card_match'] = df_output.apply(
        define_card_holder_match, axis=1).where(df_output['payment_type'] == 'credit_card')

    logger.info('Define paypal matching')
    df_output['paypal_match'] = df_output.apply(
        get_matched_paypal_users, axis=1).where(df_output['payment_type'] == 'paypal')

    logger.info('Calculate total amount from in_cents total in flex order')
    df_output['total_amount'] = df_output['in_cents'].apply(lambda x: x / 100)

    logger.info('Calculate discount amount from amount used with the voucher')
    df_output['discount_amount_in_cents'] = df_output['discount_amount_in_cents'].fillna(0.0)
    df_output['discount_amount'] = df_output['discount_amount_in_cents'].apply(
        lambda x: make_float(x) / 100)

    logger.info('Format birthdate field to work on Excel sheet')
    df_output['birth_date'] = df_output['birthdate'].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%Y/%m/%d"))

    logger.info('Define phone number with +')
    df_output['phone_number'] = df_output['phone'].apply(lambda x: '+' + str(x))

    logger.info('Remove Leading `1` from phone field')
    df_output['phone'] = df_output['phone'].apply(lambda x: str(x[1:]))

    logger.info('Calculate age of the customers')
    df_output['age'] = df_output['birthdate'].apply(lambda x: calculate_age(x))

    logger.info('Define list of transaction_ids from seon table')
    df_output['transaction_id'] = df_output.groupby(
        ['customer_id', 'order_id'], as_index=False)['seon_transaction_id'].\
        agg({'transaction_id': (lambda x: list(x))})['transaction_id']

    logger.info('assign amount from decison to limit')
    df_output.rename(columns={'amount': 'limit'}, inplace=True)
    logger.info('assign message from decison to reason')
    df_output.rename(columns={'message': 'reason'}, inplace=True)
    del df_output['status']
    del df_output['birthdate']
    df_output.rename(columns={'new_order_status': 'status'}, inplace=True)

    logger.info('Fill the empty columns with empty string')
    df_empty_columns = pd.DataFrame(columns=empty_columns_list)
    df_output = pd.concat([df_output, df_empty_columns], axis=1)

    return df_output
