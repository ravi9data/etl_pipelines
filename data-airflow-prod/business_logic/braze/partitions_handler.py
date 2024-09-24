from typing import Dict, List

from airflow.models import Variable
from pandas._libs.tslibs.timestamps import Timestamp

from plugins.date_utils import get_hours


def partition_formatter(ts: Timestamp) -> str:
    """gets a pandas timestamp and returns a string date with format YYYY-MM-DD-HH

    Args:
        timestamp (Timestamp):

    Returns:
        str: string date with format YYYY-MM-DD-HH
    """
    return f"{ts.year}-{str(ts.month).zfill(2)}-{str(ts.day).zfill(2)}-{str(ts.hour).zfill(2)}"


def get_partitions_values(config_variable,
                          default_delta_hours=3,
                          default_end_hour_delta=1) -> List[Dict[str, str]]:
    """
    Args:
        config_variable (Airflow variable): Provide the start_hours and end_hours.

    Returns:
        list: List of dicts containing the partition values, e.g.;
            [
                {'date': '2022-07-02-06'},
                {'date': '2022-07-02-07'},
            ]
    """
    config = Variable.get(config_variable, deserialize_json=True, default_var={})
    delta_hours = config.get('delta_hours') or default_delta_hours
    end_hour_delta = config.get('end_hour_delta') or default_end_hour_delta
    hours = get_hours(config_variable_name=config_variable,
                      delta_hours=int(delta_hours),
                      end_hour_delta=int(end_hour_delta))

    return [{"date":
            partition_formatter(h)}
            for h in hours]
