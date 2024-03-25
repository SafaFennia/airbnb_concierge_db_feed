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


def run_clients_upsert_pipe(start_date):
    import airbnb_concierge_etl.airflow_operator.operators as operator
    import logging.config
    import yaml
    with open('config/logging_config.yml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    operator.clients_upsert_pipe(start_date=start_date)
