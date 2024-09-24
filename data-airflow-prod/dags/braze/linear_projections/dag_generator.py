import logging

from dags.braze.linear_projections.config import (
    default_args_braze_linear_projections, list_of_tables)
from plugins.utils.linear_projections.dag_generator import create_dag

REDSHIFT_CONN = 'redshift'

logger = logging.getLogger(__name__)

for config in list_of_tables:
    globals()[config["source_table"]] = create_dag(
                                            default_args=default_args_braze_linear_projections,
                                            redshift_con=REDSHIFT_CONN,
                                            configuration=config,
                                            tags=['braze',
                                                  REDSHIFT_CONN,
                                                  config['source_table'],
                                                  'linear projection'])
