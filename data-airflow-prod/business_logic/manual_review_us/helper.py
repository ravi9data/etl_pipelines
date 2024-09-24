import json
import logging

logger = logging.getLogger(__name__)


def remove_spaces_lower_string(parsed_string):
    """
    This function helps to remove extra spaces from columns
    """
    return parsed_string.strip().lower() if parsed_string is not None else None


def apply_json_load_column(column):
    """
    This function helps to convert str json payload to dict
    """
    return column.apply(lambda x: json.loads(x))


def make_int(s):
    """
    this function converts string value to int
    """
    s = s.strip()
    return int(s) if s else 0


def make_float(s):
    """
    this function convert string value to float
    """
    if isinstance(s, str):
        return float(s) if s else 0.0
    if isinstance(s, float):
        return s if s is not None else 0.0
