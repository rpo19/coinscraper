FROM bde2020/spark-base:3.0.0-hadoop3.2

ARG SBT_VERSION
ENV SBT_VERSION=${SBT_VERSION:-1.3.13}

RUN wget -O - https://piccolo.link/sbt-${SBT_VERSION}.tgz | gunzip | tar -x -C /usr/local

ENV PATH /usr/local/sbt/bin:${PATH}

WORKDIR /spark/bin

ENV SPARK_EXECUTOR_MEMORY 1g

COPY ./submit.sh /submit.sh
RUN chmod 755 /submit.sh

COPY src /app/src
COPY build.sbt /app/build.sbt
COPY project/build.properties /app/project/build.properties
COPY project/plugins.sbt /app/project/plugins.sbt

RUN cd /app && sbt clean assembly

ENTRYPOINT ["/submit.sh"]

CMD [""]
