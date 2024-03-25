"""Airflow DAG to feed CalendarDate table.

DAG is configured to run one a year.
"""
import runners
from airflow import DAG
from textwrap import dedent
import datetime
from dateutil import tz
import pendulum
import os
from dateutil.parser import isoparse
from airflow.operators.python import ExternalPythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

# Data extract lower bound (as datetime) - computed thanks to Airflow macros and Jinja2 templating.
# Three options available, in order of priority:
# 1. Set to a custom parameter "start" when a DAG is (manually) triggered through the UI:
#   Uses a parameter parsed as ISO format date as lower bound, irrespective of the existence of a previous successful
#   DAG.
# 2. Set to last successful DAGRun start datetime:
#   Uses last successful DAGRun start datetime (with macro `prev_start_date_success`), when a previous successful
#   DAGRun exists.
#   Note : this returns operator start datetime of the last successful DAGRun - not DAG start datetime.
# 3. Set to a default value `default_extract_start_datetime`, provided with custom macro (see DAG parameters):
#   Uses the default value, when there are neither previous successful DAGRun, nor custom "start" input.

_extract_start_datetime = """{{ 
    (dag_run.conf['start'] | to_datetime) if 'start' in dag_run.conf and dag_run.conf['start']
    else (prev_start_date_success if prev_start_date_success else default_extract_start_datetime)
}}"""
# Data extract end datetime.
# Set to current date + 1 day (to get all modifications from start_date to now)
_extract_end_datetime = datetime.datetime.now(tz=tz.UTC) + datetime.timedelta(days=1)

# Airflow DAG start datetime (when DAG schedules should start) - set to value of environment variable `DAG_START_DATE`:
_dag_start_date = isoparse(os.environ.get("DAG_START_DATE")).astimezone(pendulum.timezone("Europe/Paris"))


default_args = {
    'owner': 'airflow',
    'retries': 0
}

with DAG(
        'sandbox ETL',
        description='ETL to build sandbox business objects"',
        default_args=default_args,
        schedule=CronTriggerTimetable(
            "0 2 * * *",  # each night at 02:00, Paris timezone.
            timezone=pendulum.timezone("Europe/Paris"),
        ),
        # Trigger since logical year 2019 to catchup data since 2020.
        start_date=_dag_start_date,
        params={"start": None},
        max_active_runs=1,
        catchup=False,
        # Provides a custom macro `default_extract_start_datetime`, useful for _extract_start_datetime rendering.
        user_defined_macros={
            'default_extract_start_datetime': isoparse("2023-02-01T00:00:00.000000+00:00")
        },
        # Provides a custom Jinja filter to cast string as datetime, useful for _extract_start_datetime rendering.
        user_defined_filters=dict(to_datetime=lambda string: isoparse(string)),
        # Jinja2 template rendering to Python objects instead of strings.
        render_template_as_native_obj=True,
) as dag:
    dag.doc_md = dedent("""
        Feed table "clients" in database "sandbox".
        """)

    feed_clients = ExternalPythonOperator(
        python="/opt/airflow/airbnb_concierge_etl_venv/bin/python",
        expect_airflow=False,
        task_id='feed_clients',
        python_callable=runners.run_clients_upsert_pipe,
        doc_md="""Feed table "clients" in database "sandbox".""",
        trigger_rule="all_done",
        op_kwargs={"start_date": _extract_start_datetime}
    )
