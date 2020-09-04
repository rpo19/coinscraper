#!/bin/bash

export SPARK_HOME=/spark

exec /spark/bin/spark-class \
org.apache.spark.deploy.SparkSubmit \
--master $SPARK_MASTER \
/app/target/scala-2.12/TrainApp-assembly-1.0.jar $@