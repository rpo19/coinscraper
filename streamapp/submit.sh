#!/bin/bash

export SPARK_HOME=/spark

exec /spark/bin/spark-class \
org.apache.spark.deploy.SparkSubmit \
--master $SPARK_MASTER \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,\
org.apache.opennlp:opennlp-tools:1.9.3,\
info.picocli:picocli:4.5.1,\
org.postgresql:postgresql:42.2.14 \
/app/target/scala-2.12/StreamApp-assembly-1.0.jar $@