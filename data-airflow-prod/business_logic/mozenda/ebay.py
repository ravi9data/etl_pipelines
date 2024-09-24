from plugins.mozenda_utils import get_latest_data_available_from_bucket


def get_latest_data_available_for_ebay_eu(**kwargs):

    return get_latest_data_available_from_bucket(
                bucket_name='mozendaoutput',
                prefix='price_collection_ebay_eu/',
                file_name_pattern='price-collection-eu-output-ebay-Default-YYYY-MM-DD.csv',
                days_threshold=4,
                date_format='%Y-%m-%d',
                date_variable='mozenda_ebay_eu_date',
                **kwargs)


def get_latest_data_available_for_ebay_us(**kwargs):

    return get_latest_data_available_from_bucket(
                bucket_name='mozendaoutput',
                prefix='price_collection_ebay_us/',
                file_name_pattern='price-collection-us-output-ebay-Default-YYYY-MM-DD.csv',
                days_threshold=4,
                date_format='%Y-%m-%d',
                date_variable='mozenda_ebay_us_date',
                **kwargs)
