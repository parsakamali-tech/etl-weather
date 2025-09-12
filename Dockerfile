# airflow.Dockerfile
FROM apache/airflow:2.9.3

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PY_VER=3.11
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres==5.6.0 \
    -c ${CONSTRAINT_URL}
