import datetime
import logging

from plugins.utils.pandas_helpers import (add_uuid_column,
                                          df_flatten_exploding,
                                          empty_cols_to_string_type,
                                          global_df_json_encoder)

logger = logging.getLogger(__name__)


def match_orders(r):

    NoneType = type(None)

    if isinstance(r['orders'], NoneType):
        output = None
    elif len(r['orders']) == 0:
        output = None
    else:
        if r['orders'] is not None:
            for e in r['orders']:
                if e['number'] == r['line_items']['order_number']:
                    output = {
                        "order_tax_rate": e['tax_rate'],
                        "order_tax": e['tax']
                    }

    return output


def v_look_up(df_input):

    df_output = df_input.copy()

    logger.info('mapping orders to line_items.order_number')
    df_output['orders'] = df_output[['orders', 'line_items']].apply(match_orders, axis=1)

    return df_output


def transformation(df_input):

    columns_to_flatten = [
        'uuid',
        'type',
        'contract_type',
        'entity_type',
        'status',
        'due_date',
        'consolidated',
        'payment_method',
        'user_id',
        'line_items',
        'billing_account_id',
        'amount_due',
        'last_amount_refunded',
        'shipping_cost',
        'net_shipping_cost',
        'tax',
        'orders',
        'country_code',
        'payment_failed_reason',
        'transaction_id',
        'transaction_reference_id'
        ]

    df_output = df_input.copy()
    df_output_uuid = add_uuid_column(
        df_output,
        uuid_column_name="line_item_group_uuid")
    df_output_flatten = df_flatten_exploding(
            df_input=df_output_uuid,
            columns_to_flatten=columns_to_flatten,
            columns_to_explode=['line_items'])

    df_output_v_look = v_look_up(df_output_flatten)

    df_encode_json = global_df_json_encoder(df_output_v_look)

    logger.info('Adding processed_at column as current_timestamp')
    df_encode_json['processed_at'] = datetime.datetime.now()

    df_output_strings = empty_cols_to_string_type(df_encode_json, all_columns_as_string=False)

    logger.info('Dropping payload Column')
    df_output_strings.drop(labels=['payload'], axis=1, inplace=True)

    return df_output_strings
