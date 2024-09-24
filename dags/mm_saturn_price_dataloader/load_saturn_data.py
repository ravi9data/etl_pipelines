import datetime

from dags.mm_saturn_price_dataloader.download_util import download_csv_to_s3


def load_saturn_data(**kwargs):
    SAT_DE_CSV_URL = 'https://transport.productsup.io/caddb83fd780372e275e/' \
                     'channel/290916/SAT_MOA_DE_DIV_grover.csv'

    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    sat_de_file_path = f'price_data/saturn/de/{current_date}/sat_download.csv'

    download_csv_to_s3(SAT_DE_CSV_URL, sat_de_file_path)

    ti = kwargs['task_instance']
    ti.xcom_push(key='sat_de_file_path', value=sat_de_file_path)
    return sat_de_file_path
