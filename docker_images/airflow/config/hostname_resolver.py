import os
import socket


def hostname() -> str:
    """Resolve hostname from environment variable AIRFLOW2_SCHEDULER_SERVICE if defined, or from default Airflow method.

    Callable to be used in Airflow config AIRFLOW__CORE__HOSTNAME_CALLABLE (see
    https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#hostname-callable) to define
    hostname of Airflow component.

    Airflow executor exposes a webservice on port 8793 to serve tasks logs. Airflow webserver calls this webservice when
    displaying logs in GUI, resolving executor hostname. Airflow default (socket.getfqdn) returns pod name in k8s,
    which is not consistent. To resolve this issue, we need a k8s Service (map on executor port 8793). We set
    AIRFLOW2_SCHEDULER_SERVICE with the name of the Service.

    If environment variable AIRFLOW2_SCHEDULER_SERVICE is defined, return its value (k8s Service name)
    Else returns socket.getfqdn() (Airflow default).

    :return: hostname as str
    """
    airflow_scheduler_service = os.getenv("AIRFLOW2_SCHEDULER_SERVICE", None)
    if not airflow_scheduler_service:
        return socket.getfqdn()
    else:
        return airflow_scheduler_service
