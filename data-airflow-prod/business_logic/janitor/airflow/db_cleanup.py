import datetime
import logging

import dateutil.parser
from airflow import settings
from airflow.jobs.base_job import BaseJob
from airflow.models import (DagRun, ImportError, Log, SlaMiss, TaskFail,
                            TaskInstance, TaskReschedule, Variable, XCom)
from airflow.utils import timezone
from sqlalchemy import and_, func
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

DATABASE_OBJECTS = [
    {
        "airflow_db_model": BaseJob,
        "age_check_column": BaseJob.latest_heartbeat,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": False,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": TaskFail,
        "age_check_column": TaskFail.end_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": ImportError,
        "age_check_column": ImportError.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.end_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": TaskReschedule,
        "age_check_column": TaskReschedule.reschedule_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
]


def print_configuration_function(enable_delete, print_deletes, **context):
    now = timezone.utcnow
    default_max_db_entry_age_in_days = int(
        Variable.get("max_db_entry_age_in_days", 60)
    )
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get(
            "max_db_entry_age_in_days", None
        )
    logging.info("max_db_entry_age_in_days from dag_run.conf: " + str(dag_run_conf))
    if max_db_entry_age_in_days is None or max_db_entry_age_in_days < 1:
        logging.info(
            "maxDBEntryAgeInDays conf variable isn't included or Variable " +
            "value is less than 1. Using Default '" +
            str(default_max_db_entry_age_in_days) + "'"
        )
        max_db_entry_age_in_days = default_max_db_entry_age_in_days
    max_date = now() + datetime.timedelta(-max_db_entry_age_in_days)
    logging.info("Finished Loading Configurations")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(enable_delete))
    logging.info("print_deletes:            " + str(print_deletes))
    logging.info("")

    logging.info("Setting max_execution_date to XCom for Downstream Processes")
    context["ti"].xcom_push(key="max_date", value=max_date.isoformat())


def cleanup_function(enable_delete, print_deletes, **context):
    session = settings.Session()
    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(
        task_ids='print_configuration', key="max_date"
    )
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    airflow_db_model = context["params"].get("airflow_db_model")
    state = context["params"].get("state")
    age_check_column = context["params"].get("age_check_column")
    keep_last = context["params"].get("keep_last")
    keep_last_filters = context["params"].get("keep_last_filters")
    keep_last_group_by = context["params"].get("keep_last_group_by")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(enable_delete))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("state:                    " + str(state))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("keep_last:                " + str(keep_last))
    logging.info("keep_last_filters:        " + str(keep_last_filters))
    logging.info("keep_last_group_by:       " + str(keep_last_group_by))

    logging.info("")

    logging.info("Running Cleanup Process...")

    try:
        query = session.query(airflow_db_model).options(
            load_only(age_check_column)
        )

        logging.info("INITIAL QUERY : " + str(query))

        if keep_last:
            subquery = session.query(func.max(DagRun.execution_date))
            # workaround for MySQL "table specified twice" issue
            # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
            if keep_last_filters is not None:
                for entry in keep_last_filters:
                    subquery = subquery.filter(entry)

                logging.info("SUB QUERY [keep_last_filters]: " + str(subquery))

            if keep_last_group_by is not None:
                subquery = subquery.group_by(keep_last_group_by)
                logging.info(
                    "SUB QUERY [keep_last_group_by]: " + str(subquery))

            subquery = subquery.from_self()

            query = query.filter(
                and_(age_check_column.notin_(subquery)),
                and_(age_check_column <= max_date)
            )

        else:
            query = query.filter(age_check_column <= max_date,)

        if print_deletes:
            entries_to_delete = query.all()

            logging.info("Query: " + str(query))
            logging.info(
                "Process will be Deleting the following " +
                str(airflow_db_model.__name__) + "(s):"
            )
            for entry in entries_to_delete:
                logging.info(
                    "\tEntry: " + str(entry) + ", Date: " +
                    str(entry.__dict__[str(age_check_column).split(".")[1]])
                )

            logging.info(
                "Process will be Deleting " + str(len(entries_to_delete)) + " " +
                str(airflow_db_model.__name__) + "(s)"
            )
        else:
            logging.warning(
                "You've opted to skip printing the db entries to be deleted."
                "Set PRINT_DELETES to True to show entries!!!")

        if enable_delete:
            logging.info("Performing Delete...")
            # using bulk delete
            query.delete(synchronize_session=False)
            session.commit()
            logging.info("Finished Performing Delete")
        else:
            logging.warning(
                "You've opted to skip deleting the db entries."
                "Set ENABLE_DELETE to True to delete entries!!!")

        logging.info("Finished Running Cleanup Process")

    except ProgrammingError as e:
        logging.error(e)
        logging.error(str(airflow_db_model) +
                      " is not present in the metadata. Skipping...")
