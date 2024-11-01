FROM apache/airflow:2.9.0b2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    nano \
    default-jre \
    procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
RUN pip install \
    apache-airflow==2.9.0 \
    pyspark \
    grpcio-status  \
    apache-airflow-providers-apache-spark
USER root
