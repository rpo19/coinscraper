# FROM mozilla/sbt:8u232_1.3.13 as builder

# COPY ./src /app/src
# COPY ./build.sbt /app/build.sbt
# COPY ./project/build.properties /app/project/build.properties
# COPY ./project/plugins.sbt /app/project/plugins.sbt

# RUN cd /app && sbt assembly

# FROM bitnami/spark:3-debian-10

# COPY --from=builder /app/target/scala-2.12/StreamApp-assembly-1.0.jar /app/target/scala-2.12/StreamApp-assembly-1.0.jar

# ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.opennlp:opennlp-tools:1.9.3,info.picocli:picocli:4.5.1,org.postgresql:postgresql:42.2.14", "/app/target/scala-2.12/StreamApp-assembly-1.0.jar"]

FROM bde2020/spark-base:3.0.0-hadoop3.2

ARG SBT_VERSION
ENV SBT_VERSION=${SBT_VERSION:-1.3.13}

RUN wget -O - https://piccolo.link/sbt-${SBT_VERSION}.tgz | gunzip | tar -x -C /usr/local

ENV PATH /usr/local/sbt/bin:${PATH}

WORKDIR /spark/bin

COPY ./submit.sh /submit.sh
RUN chmod 755 /submit.sh

COPY src /app/src
COPY build.sbt /app/build.sbt
COPY project/build.properties /app/project/build.properties
COPY project/plugins.sbt /app/project/plugins.sbt

RUN cd /app && sbt clean assembly

ENTRYPOINT ["/submit.sh"]

CMD [""]
