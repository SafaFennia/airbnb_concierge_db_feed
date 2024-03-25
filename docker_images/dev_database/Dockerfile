FROM postgres:14.7
COPY 1-create-db.sql /docker-entrypoint-initdb.d/
COPY 2-create_airflow_db.sql /docker-entrypoint-initdb.d/
COPY 3-init-database.sql /docker-entrypoint-initdb.d/

