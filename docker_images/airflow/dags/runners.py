"""This module offers functions to call "operator" defined in custom packages.

All these function are supposed to be callables in Airflow ExternalPythonOperator.
So they are executed in a subprocess in a virtual env. with these packages in requirements, and not in Airflow
Python env. directly (where the packages are not installed)

The process is always the same:
 - import of the required Python packages (business_objects_feed or covoitgo_reporting_feed).
 -  Define log level to INFO (else subprocess only logs WARN and ERROR logs, and Airflow doesn't get INFO logs)
 - Execute function from package.
Note that input are serialized with pickle.

Documentation:
https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#using-externalpythonoperator
https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#externalpythonoperator
"""


def run_calendar_dates_upsert_pipe(year):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.calendar_dates_upsert_pipe(year=year)


def run_registered_users_upsert_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.registered_users_upsert_pipe(start_datetime=start_datetime)


def run_registered_users_delete_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.registered_users_delete_pipe(start_datetime=start_datetime)


def run_driver_tracks_upsert_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.driver_tracks_upsert_pipe(start_datetime=start_datetime)


def run_carpool_requests_upsert_from_rides_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.carpool_requests_upsert_from_rides_pipe(start_datetime=start_datetime)


def run_carpool_requests_delete_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.carpool_requests_delete_pipe(start_datetime=start_datetime)


def run_carpool_requests_update_from_ride_proofs_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.carpool_requests_update_from_ride_proofs_pipe(start_datetime=start_datetime)


def run_backend_communications_insert_pipe(start_datetime):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.backend_communications_insert_pipe(start_datetime=start_datetime)


def run_zendesk_communications_insert_pipe(zendesk_cursor):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    return operator.zendesk_communications_insert_pipe(zendesk_cursor=zendesk_cursor)


def run_mailchimp_communications_insert_pipe(start_datetime, audiences_whitelist):
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.mailchimp_communications_insert_pipe(
        start_datetime=start_datetime,
        audiences_whitelist=audiences_whitelist
    )


def run_active_users_agg_pipe():
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.active_users_agg_pipe()


def run_communications_agg_pipe():
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.communications_agg_pipe()


def run_planned_od_agg_pipe():
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.planned_od_agg_pipe()


def run_registered_users_agg_pipe():
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.registered_users_agg_pipe()


def run_carpool_requests_successive_cancel_agg_pipe():
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.carpool_requests_successive_cancel_pipe()


def run_network_view_split_pipe():
    import business_objects_feed.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.network_view_split_pipe()


def run_compute_covoitgo_tables():
    import covoitgo_reporting_feed.compute_covoitgo_tables as compute_covoitgo_tables
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    compute_covoitgo_tables.run()


def run_create_covoitgo_view_for_networks():
    import covoitgo_reporting_feed.create_covoitgo_view_for_networks as create_covoitgo_view_for_networks
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    create_covoitgo_view_for_networks.run()


def run_compute_panels_tables():
    import covoitgo_reporting_feed.compute_panels_tables as compute_panels_tables
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    compute_panels_tables.run()


def run_rnpc_update_etl():
    from data_toolbox.utils import ConnectionDetails
    import data_toolbox.rnpc_brick as rnpc_brick
    import logging
    import os
    logging.basicConfig(level=logging.INFO)
    connection_details = ConnectionDetails(
        user=os.environ.get("RNPC_DATABASE_USER", "postgres"),
        host=os.environ.get("RNPC_DATABASE_HOST", "db"),
        password=os.environ.get("RNPC_DATABASE_PASSWORD", "dev"),
        port=os.environ.get("RNPC_DATABASE_PORT", 5432),
        database=os.environ.get("RNPC_DATABASE_NAME", "geodatabase"),
        schema=os.environ.get("RNPC_DATABASE_SCHEMA", "public"),
    )
    rnpc_update = rnpc_brick.RnpcUpdate(
        connection_details=connection_details,
        table_name=os.environ.get("RNPC_TABLE_NAME", "rnpc"),
        dry_run=False,
        confirm=False,
        save_path=None,
        data_ids=None,
        min_date="2023-09-01",
        chunk_size=10000,
    )
    rnpc_update.run()


def run_gps_tracks():
    from gps_tracks.operator import run
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    run()


def run_departure_table(start_datetime, brand_icon):
    from metabase_toolkit import departure_table
    import locale
    import logging
    import os
    import json
    from datetime import datetime

    logging.basicConfig(level=logging.INFO)
    locale.setlocale(category=locale.LC_ALL, locale=('fr_FR', 'UTF-8'))
    dashboard_filter = {'min_hour': 4, 'max_hour': 9} \
        if datetime.strptime(start_datetime[:19], '%Y-%m-%dT%H:%M:%S').hour < 12 else {'min_hour': 16, 'max_hour': 22}
    departure_table_feed = departure_table.DepartureTable(metabase_url=os.environ.get("METABASE_URL"),
                                                          metabase_user=os.environ.get("METABASE_USER"),
                                                          metabase_pwd=os.environ.get("METABASE_PWD"),
                                                          dashboard_id=1357,
                                                          dashboard_filter=dashboard_filter,
                                                          brand_icon=json.loads(brand_icon),
                                                          slack_token=os.environ.get("REPORTING_BOT_SLACK_TOKEN"),
                                                          channel_id=os.environ.get("DEPARTURE_TABLE_CHANNEL_ID"))

    departure_table_feed.run()
