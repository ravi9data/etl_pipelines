import logging

from airflow.models import Variable

from dags.curated.kafka.config import curated_operations, default_args
from plugins.dag_utils import get_schedule
from plugins.date_utils import get_hours
from plugins.utils.curated import create_dag, transformation_rewriter

logger = logging.getLogger(__name__)

kafka_curated_shared_config = Variable.get('kafka_curated', deserialize_json=True, default_var={})

for configuration in curated_operations:
    hours = get_hours(configuration['config_variable_name'])
    globals()[configuration["dag_id"]] = create_dag(
                dag_id=configuration["dag_id"],
                schedule=get_schedule(configuration["dag_schedule"]),
                default_args=default_args,
                python_callable=transformation_rewriter,
                hours=enumerate(hours),
                executor_config=configuration.get("executor_config"),
                configuration=configuration | kafka_curated_shared_config)
