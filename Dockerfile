FROM apache/airflow:latest



USER root

RUN apt-get update && \

    apt-get -y install git && \

    apt-get clean



USER airflow

FROM apache/airflow:2.10.3
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential libopenmpi-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "mpi4py==3.1.6"