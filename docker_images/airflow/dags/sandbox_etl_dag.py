"""Airflow DAG to feed CalendarDate table.

DAG is configured to run one a year.
"""
from helpers import post_failure_to_slack
import runners
from airflow import DAG
from textwrap import dedent
import pendulum
from airflow.operators.python import ExternalPythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'on_failure_callback': post_failure_to_slack,
}

# Get the scheduled execution date.
_logical_date = "{{ ds }}"

with DAG(
        'business_objects_calendar_date_etl',
        description='Feed table "calendar_dates" in database "business_objects"',
        default_args=default_args,
        schedule=CronTriggerTimetable(
            "0 0 1 12 *",  # Every year on December 1st.
            timezone="UTC",
        ),
        # Trigger since logical year 2019 to catchup data since 2020.
        start_date=pendulum.datetime(2019, 12, 1, tz="UTC"),
        max_active_runs=1,
        catchup=True,
) as dag:
    dag.doc_md = dedent("""
        Feed table "calendar_dates" in database "business_objects".
        """)

    feed_calendar_dates = ExternalPythonOperator(
        python="/opt/airflow/business_objects_feed_venv/bin/python",
        expect_airflow=False,
        task_id='feed_calendar_dates',
        python_callable=runners.run_calendar_dates_upsert_pipe,
        doc_md="""Feed table "calendar_dates" in database "business_objects".""",
        trigger_rule="all_done",
        op_kwargs={"year": _logical_date},
    )
