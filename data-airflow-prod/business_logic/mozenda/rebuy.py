from plugins.mozenda_utils import get_latest_data_available_from_bucket


def get_latest_data_available_for_rebuy(**kwargs):

    return get_latest_data_available_from_bucket(
                bucket_name='mozendaoutput',
                prefix='price_collection_rebuy/',
                file_name_pattern='price-collection-rebuy-Default-YYYY-MM-DD.csv',
                days_threshold=4,
                date_format='%Y-%m-%d',
                date_variable='mozenda_rebuy_date',
                **kwargs)
