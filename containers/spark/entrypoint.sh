#!/bin/bash

service ssh start

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  start-worker.sh spark://spark-master:7077  --cores 1 --memory 1g
elif [ "$SPARK_WORKLOAD" == "history-server"];
then
  start-history-server.sh
fi