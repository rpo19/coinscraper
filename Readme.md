# Big Data Velocity
## Ottenimento e analisi di dati in streaming

### Codice sorgente

#### StreamApp

```
streamapp/src/main/scala/Stream.scala
```

#### TrainApp

```
trainapp/src/main/scala/Train.scala
```

#### Producers
```
binance/binance_producer.py
tweepy/tweets_producer.py
```

### Installazione con docker

Dipendenze:  
* docker https://docs.docker.com/get-docker/
* docker-compose https://docs.docker.com/compose/install/
* account Twitter https://twitter.com/
* account Binance https://www.binance.com/

Una volta installati docker e docker compose:
* creare un file `.env` a partire da `env-sample.txt` nella stessa cartella del `docker-compose.yml`
* creare i file binance/secrets.py e tweepy/secrets.py a partire dai sample inserendo le proprie credenziali

In seguito eseguire
```
docker-compose up -d
```
per avviare tutti i componenti attraverso docker.


Inizialmente l'applicazione acquisirà semplicemente i dati, in seguito eseguire un restart di trainapp
```
docker-compose restart trainapp
```
in modo da allenare i modelli e di streamapp
```
docker-compose restart streamapp
```
in modo che i modelli vengano usati per classificare tweets.

Purtroppo non è possibile con l'attuale configurazione eseguire nodi Spark worker su più macchine utilizzando docker.

## Installazione con Spark Cluster non in Docker

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
infine è sufficiente eseguire la `spark-submit` per inviare l'applicazione al master (presuppone che le app siano già state assemblete in un jar):
```
./bin/spark-submit \
--executor-memory 1g \
--master spark://$SPARK_MASTER_ADDRESS:7077 \
$PATH_TO_APP_JAR.jar \
-k $KAFKA_ADDRESS:9092 \
--lrmodel /home/rpo/spark/models/spark-logistic-regression-model \
--vcmodel /home/rpo/spark/models/spark-cv-model \
--jdbcurl jdbc:postgresql://$TIMESCALE_ADDRESS:5432/postgres
```

È disponibile visualizzare un help eseguendo:
```
./bin/spark-submit $PATH_TO_APP_JAR.jar --help
```

N.B:
* ogni nodo worker necessita i file dei modelli in locale.
* perchè kafka funzioni correttamente è necessario modificare il file `.env` in modo che `KAFKA_CFG_ADVERTISED_LISTENERS` contenga un indirizzo raggiungibile "dall'esterno", ad esempio l'ip locale della macchina dove viene eseguito.

### Build delle applicazioni Spark

#### Estrarre il jar dall'immagine docker

Siccome le immagini docker durante la fase di build creano il jar la soluzione più semplice è quella
di estrarre il jar dall'immagine docker; ad esempio eseguendo:
```
docker-compose run --rm --entrypoint '' -T streamapp cat /app/target/scala-2.12/StreamApp-assembly-1.0.jar > /path/to/StreamApp-assembly-1.0.jar
# oppure per TrainApp
docker-compose run --rm --entrypoint '' -T streamapp cat /app/target/scala-2.12/TrainApp-assembly-1.0.jar > /path/to/TrainApp-assembly-1.0.jar
```
Questi comandi creranno i file
```
/path/to/StreamApp-assembly-1.0.jar
/path/to/TrainApp-assembly-1.0.jar
```
che possono poi essere eseguito tramite spark-submit.

#### Installare sbt

Il processo di build necessita di sbt (https://www.scala-sbt.org/).

Una volta installato posizionarsi nella cartella del progetto (e.g streamapp o trainapp) ed eseguire
```
sbt clean assembly
```
A fine processo il jar si troverà ad esempio in `target/scala-2.12/StremApp-assembly-1.0.jar`.

### Dashboard

Grafana è stato usato come dashboard ed è raggiungile all'indirizzo http://localhost:3000
se eseguito il docker-compose.

Per visualizzare la dashboard è sufficiente aggiungere timescale come datasource utilizzando
PostgreSQL sia come tipo che come nome.

In seguito importare la dashboard dal file `Main.json`