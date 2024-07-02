FROM ubuntu:24.10

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev \
openjdk-17-jre-headless

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
