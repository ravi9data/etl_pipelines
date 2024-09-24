import logging

from business_logic.recommerce.ingram_micro.commons import process_raw_files

logger = logging.getLogger(__name__)


def process_and_transform_raw_files(dag_config, **context):
    ti = context['ti']
    s3_processed_partition = process_raw_files(dag_config=dag_config,
                                               object_suffix='.xlsx',
                                               ti=ti,
                                               skiprows=1)
    return s3_processed_partition
