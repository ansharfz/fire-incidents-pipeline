FROM python:3.11-slim AS spark-base

ARG SPARK_VERSION="3.5.3"

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
    openssh-server \
    default-jre && \
    apt-get clean

ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}

WORKDIR ${SPARK_HOME}

RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

FROM spark-base AS pyspark

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST="spark-master"
ENV SPARK_MASTER_PORT="7077"
ENV PYSPARK_PYTHON="python3"

COPY ./conf/spark-defaults.conf "$SPARK_HOME/conf"
RUN curl https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o ${SPARK_HOME}/jars/postgresql-42.7.3.jar

ENV PYTHONPATH=$SPARK_HOME/python/:${PYTHONPATH:-}

RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

RUN useradd -m -s /bin/bash airflow

RUN mkdir -p /opt/spark/spark-events

RUN echo "airflow:airflow" | chpasswd

RUN chown -R airflow:airflow ${SPARK_HOME}

EXPOSE 22

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]