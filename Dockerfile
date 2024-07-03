FROM python:3.11-slim

ARG SPARK_VERSION=3.4.3

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        sudo \
        curl \
        nano \
        unzip \
        rsync \
        build-essential \
        software-properties-common \
        ssh \
        gcc libpq-dev \
        openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*do
ENV SPARK_HOME=${SPARK_HOME:-/opt/spark}
ENV HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz
