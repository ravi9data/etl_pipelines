import logging

import awswrangler as wr
from airflow.models import Variable

logger = logging.getLogger(__name__)

s3_bucket = Variable.get('s3_bucket_curated_bi',
                         default_var='grover-eu-central-1-production-data-bi-curated')
staging_path_daisycon = f"s3://{s3_bucket}/affiliate/daisycon/fetch/"
# staging_path_everflow = f"s3://{s3_bucket}/affiliate/everflow/fetch/"
# staging_path_tradedoubler = f"s3://{s3_bucket}/affiliate/tradedoubler/fetch/"

staging_path = [staging_path_daisycon]


def cleanup_temp_affiliate_files_s3():
    for path in staging_path:
        logger.info(f'Deleting objects in {path}')
        wr.s3.delete_objects(path)
