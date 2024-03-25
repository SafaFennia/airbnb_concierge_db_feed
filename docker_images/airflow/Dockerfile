FROM apache/airflow:2.7.2-python3.10

# Airflow config ; see configuration reference at
# https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
ENV AIRFLOW__CORE__EXECUTOR="LocalExecutor"
ENV AIRFLOW__CORE__LOAD_EXAMPLES="False"
ENV AIRFLOW__WEBSERVER__WORKERS=1
ENV AIRFLOW__CORE__PARALLELISM=3
ENV AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
ENV AIRFLOW__CORE__HOSTNAME_CALLABLE="hostname_resolver.hostname"
ENV AIRFLOW__WEBSERVER__WARN_DEPLOYMENT_EXPOSURE="False"

# Create venv for tasks execution
ENV PIP_USER=false
RUN python3 -m venv /opt/airflow/airbnb_concierge_etl_venv

USER root
RUN apt-get update

USER airflow

# Install custom packages in this venv
# Install airbnb_concierge_etl
COPY --chown=airflow:root ./airbnb_concierge_etl /tmp/airbnb_concierge_etl
COPY --chown=airflow:root ./airbnb_concierge_etl /tmp/airbnb_concierge_etl
RUN /opt/airflow/airbnb_concierge_etl_venv/bin/pip install \
    --no-cache-dir /tmp/airbnb_concierge_etl/. && \
    rm -r /tmp/airbnb_concierge_etl

# Install DAGs files
COPY ./docker_images/airflow/dags /opt/airflow/dags
COPY ./docker_images/airflow/config /opt/airflow/config

