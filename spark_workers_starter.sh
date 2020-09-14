#!/bin/bash

# SPARK_HOME
# SPARK_WORKERS_STARTER_SLEEP_TIME
# SPARK_WORKERS_STARTER_MAX

for (( i=0; i<$SPARK_WORKERS_STARTER_MAX; ++i))
do

    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $@ &
    sleep $SPARK_WORKERS_STARTER_SLEEP_TIME

done
