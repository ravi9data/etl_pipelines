import datetime

from dags.mm_saturn_price_dataloader.download_util import download_csv_to_s3


def load_mm_data(**kwargs):
    MM_DE_CSV_URL = 'https://transport.productsup.io/fcdef3a90ef33ae1fed8/' \
                    'channel/321577/INT_DE_MM_ON_grover.csv'

    MM_ES_CSV_URL = 'https://transport.productsup.io/357df1bd8856512cb363/' \
                    'channel/440797/MM_ES_grover.csv'

    current_date = datetime.datetime.now().strftime('%Y-%m-%d')

    mm_de_file_path = f'price_data/media_market/de/{current_date}/mm_download.csv'
    mm_es_file_path = f'price_data/media_market/es/{current_date}/mm_download.csv'

    download_csv_to_s3(MM_DE_CSV_URL, mm_de_file_path)
    download_csv_to_s3(MM_ES_CSV_URL, mm_es_file_path)

    ti = kwargs['task_instance']
    ti.xcom_push(key="mm_de_file_path", value=mm_de_file_path)
    ti.xcom_push(key="mm_es_file_path", value=mm_es_file_path)

    return mm_de_file_path
