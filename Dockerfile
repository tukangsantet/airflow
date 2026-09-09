# syntax=docker/dockerfile:1
ARG AIRFLOW_VERSION=2.11.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11

USER airflow

# Keep provider dependencies aligned with the exact Airflow/Python image.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# DAG source is mounted read-only from the separate Architron repository by
# docker-compose.yml. Keeping it outside this image makes the installer repo
# independent from the application/DAG repo.
ENV PYTHONPATH=/opt/airflow/dags
