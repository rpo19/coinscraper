# Big Data Velocity
## Ottenimento e analisi di dati in streaming

### Installazione con docker

Dipendenze:  
* docker https://docs.docker.com/get-docker/
* docker-compose https://docs.docker.com/compose/install/

Una volta installati docker e docker compose
creare un file `.env` a partire da `env-sample.txt` nella stessa cartella del `docker-compose.yml` e in seguito eseguire
```
docker-compose up -d
```

Purtroppo non è possibile con l'attuale configurazione eseguire nodi Spark worker su più macchine utilizzando docker.

Per sopperire a questa mancanza non è necessaria l'installazione di ogni componente al di fuori di docker ma è sufficiente installare Apache Spark nei vari nodi (https://spark.apache.org/docs/latest/) ed in seguito eseguire un master:
```
./sbin/start-master.sh --host 0.0.0.0
```
e dei worker:
```
./sbin/start-slave.sh spark://$SPARK_MASTER_ADDRESS:7077 -m 1g -c 1
# oppure se si necessità eseguirne più di uno
./bin/spark-class org.apache.spark.deploy.worker.Worker \
-c 1 \
-m 1g \
spark://$SPARK_MASTER_ADDRESS:7077
```
infine è sufficiente eseguire la `spark-submit` per inviare l'applicazione al master:
```
./bin/spark-submit \
--executor-memory 1g \
--master spark://$SPARK_MASTER_ADDRESS:7077 \
$PATH_TO_APP_JAR.jar.jar \
-k $KAFKA_ADDRESS:9092 \
--lrmodel /home/rpo/spark/models/spark-logistic-regression-model \
--vcmodel /home/rpo/spark/models/spark-cv-model \
--jdbcurl jdbc:postgresql://$TIMESCALE_ADDRESS:5432/postgres
```
N.B: ogni nodo worker necessita i file dei modelli in locale.