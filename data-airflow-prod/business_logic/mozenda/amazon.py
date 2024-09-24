from plugins.mozenda_utils import get_latest_data_available_from_bucket


def get_latest_data_available_for_amazon_eu(**kwargs):

    return get_latest_data_available_from_bucket(
                bucket_name='mozendaoutput',
                prefix='price_collection_amazon/',
                file_name_pattern='price-collection-amazon-Default-YYYY-MM-DD.csv',
                days_threshold=4,
                date_format='%Y-%m-%d',
                date_variable='mozenda_amazon_date',
                **kwargs)


def get_latest_data_available_for_amazon_us(**kwargs):

    return get_latest_data_available_from_bucket(
                bucket_name='mozendaoutput',
                prefix='price_collection_amazon_us/',
                file_name_pattern='price-collection-us-output-amazon-Default-YYYY-MM-DD.csv',
                days_threshold=4,
                date_format='%Y-%m-%d',
                date_variable='mozenda_amazon_us_date',
                **kwargs)
