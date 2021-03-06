import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{
  StructType,
  DateType,
  TimestampType,
  LongType,
  DoubleType,
  BooleanType
}
import org.apache.spark.sql.functions._
import _root_.java.sql.Timestamp
import org.apache.spark.ml.feature.Tokenizer
import opennlp.tools.stemmer.PorterStemmer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{Dataset,Row}

import picocli.CommandLine
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine._

import java.util.concurrent.Callable

/*
* Command line stuff
*/

@Command(
  name = "StreamApp",
  version = Array("StreamApp v1.0"),
  mixinStandardHelpOptions = true, // add --help and --version options
  description = Array("Spark streaming tweets prices analyzer")
)
class Main extends Callable[Int] {

  @Option(
    names = Array("--lrmodel"),
    paramLabel = "LOGISTIC_REGRESSION_MODEL_PATH",
    description = Array("Logistic regression model path")
  )
  var lrModelPath : String = _

  @Option(
    names = Array("--vcmodel"),
    paramLabel = "VECTORIZER_MODEL_PATH",
    description = Array("Vectorizer model path")
  )
  var vectorizerModelPath : String = _

  @Option(
    names = Array("--jdbcurl"),
    paramLabel = "JDBCURL",
    description = Array("Jdbc url to reach the database")
  )
  var jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/postgres"

  @Option(
    names = Array("--jdbcpassword"),
    paramLabel = "JDBCPASSWORD",
    description = Array("Jdbc password")
  )
  var jdbcPassword = "password"

  @Option(
    names = Array("-k", "--kafka"),
    paramLabel = "KAFKA_SERVERS",
    description = Array(
      "Kafka bootstrap servers (comma separeted if more than one)"
    )
  )
  var kafkaBootstrapServers = "localhost:9092"

  @Option(
    names = Array("--tweets-topic"),
    paramLabel = "TWEETS_TOPIC",
    description = Array("Tweets kafka topic")
  )
  var tweetsTopic = "tweets-bitcoin"

  @Option(
    names = Array("--prices-topic"),
    paramLabel = "PRICES_TOPIC",
    description = Array("Prices kafka topic")
  )
  var pricesTopic = "binance-BTCUSDT"

  def call(): Int = {

    // Corpo dell'applicazione

    var loadedModels = true
    var lrModel : LogisticRegressionModel = null
    var vectorizerModel : Word2VecModel = null

    try {

      // ?? necessario che l'app funzioni anche senza i modelli per ottenere dati iniziali

      lrModel = LogisticRegressionModel.load(lrModelPath)
      vectorizerModel = Word2VecModel.load(vectorizerModelPath)

    } catch {
      case _: Throwable => {
        loadedModels = false
      }
    }

    println("loadedModels: " + loadedModels)

    // schema dati

    val tweets_schema = new StructType()
      .add("created_at", "string")
      .add("id", "int")
      .add("id_str", "string")
      .add("text", "string")
      .add("source", "string")
      .add("truncated", "boolean")
      .add("in_reply_to_status_id", "string")
      .add("in_reply_to_status_id_str", "string")
      .add("in_reply_to_user_id", "string")
      .add("in_reply_to_user_id_str", "string")
      .add("in_reply_to_screen_name", "string")
      .add("user", "string")
      .add("geo", "string")
      .add("coordinates", "string")
      .add("place", "string")
      .add("contributors", "string")
      .add("is_quote_status", "string")
      .add("quote_count", "int")
      .add("reply_count", "int")
      .add("retweet_count", "int")
      .add("favorite_count", "int")
      .add("entities", "string")
      .add("favorited", "string")
      .add("retweeted", "string")
      .add("possibly_sensitive", "string")
      .add("filter_level", "string")
      .add("lang", "string")
      .add("timestamp_ms", "string")
      .add("receivedat", "string")

    val binance_schema = new StructType()
      .add("u", "long")
      .add("s", "string")
      .add("b", "string")
      .add("B", "string")
      .add("a", "string")
      .add("A", "string")
      .add("timestamp", "string")

    // spark session
    val spark = SparkSession
      .builder()
      .appName("Cons test")
      .config("spark.sql.caseSensitive", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("Startup completed")

    // databases
    val pricesDB = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", "prices")
      .option("user", "postgres")
      .option("password", jdbcPassword)
      .load()

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")

    val remover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("words")

    // Tweets Streaming Query
    val tweets = spark.readStream
      // ottenimento tweets da kafka
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", tweetsTopic)
      .load
      // applica lo schema corretto
      .select(
        from_json($"value".cast("string"), tweets_schema).alias("value")
      )
      .withColumn(
        "timestamp",
        ($"value.timestamp_ms".cast(LongType) / 1000).cast(TimestampType)
      )
      .withColumn(
        "receivedat",
        ($"value.receivedat".cast(DoubleType)).cast(TimestampType)
      )
      .select("timestamp", "value.text", "receivedat")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        var toWrite: Dataset[Row] = null
        if (loadedModels) {
          // preprocess: tokenization, stopwords
          val preprocessed = remover
            .transform(tokenizer.transform(batchDF))
            .select("timestamp", "receivedat", "text", "words")
            .map(x =>
              (
                x.getAs[Timestamp](0),
                x.getAs[Timestamp](1),
                x.getAs[String](2),
                x.getAs[Seq[String]](3)
                  // remove empty sets
                  .filter(_.length > 0)
                  // stemming
                  .map(y => new PorterStemmer().stem(y))
              )
            )
            .toDF
            .select(
              $"_1".as("timestamp"),
              $"_2".as("receivedat"),
              $"_3".as("text"),
              $"_4".as("words")
            )

          // predict tweet polarity
          toWrite = lrModel
            .transform(vectorizerModel.transform(preprocessed))
            .withColumn("b_prediction", $"prediction".cast(BooleanType))
            .drop("prediction")
            .withColumnRenamed("b_prediction", "prediction")
            .select("timestamp", "text", "prediction", "receivedat")
            .withColumn("processedat", current_timestamp())

        } else {
          // nel caso i modelli non esistono viene saltata la parte di predizione
          toWrite = batchDF
        }

        // scrittura sul database
        toWrite.write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcUrl)
          .option("dbtable", "tweets")
          .option("user", "postgres")
          .option("password", jdbcPassword)
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    // Prices/Financial Data Streaming Query
    val binance = spark.readStream
      // kafka
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", pricesTopic)
      .load
      // schema
      .select(
        from_json($"value".cast("string"), binance_schema).alias("value")
      )
      .withColumn("askprice", $"value.a".cast(DoubleType))
      .withColumn("askqty", $"value.A".cast(DoubleType))
      .withColumn("bidprice", $"value.b".cast(DoubleType))
      .withColumn("bidqty", $"value.B".cast(DoubleType))
      .withColumn("symbol", $"value.s")
      .withColumn(
        "timestamp",
        ($"value.timestamp".cast(DoubleType)).cast(TimestampType)
      )
      .drop("value")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // write to db
        batchDF
          .withColumn("processedat", current_timestamp())
          .write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcUrl)
          .option("dbtable", "prices")
          .option("user", "postgres")
          .option("password", jdbcPassword)
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    // query per ottenere l'ultimo minuto aggiunto al database trend per minuto.
    // per non ricalcolare ci?? che ?? gi?? stato aggiunto
    val lastTrendPerMinDB = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", "trendperminute")
      .option("user", "postgres")
      .option("password", jdbcPassword)
      .load()
      .agg(max("timestamp"))

    // minuto corrente. 11:33:12 -> 11:33:00
    def currentmin(): Timestamp = {
      val now = System.currentTimeMillis()
      return new Timestamp(now - now % 60000L)
    }

    // timer periodico che calcola trend per minuto
    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = {
        // ultimo minuto aggiunto al database trend per minuto; esegue la query
        val latestTime = lastTrendPerMinDB.take(1)(0)(0).asInstanceOf[Timestamp]

        // filtra il dataframe rimuovendo i minuti gi?? calcolati fino al minuto corrente escluso
        val step1 =
          if (latestTime == null)
            pricesDB.filter($"timestamp" < currentmin())
          else
            pricesDB.filter(
              $"timestamp" < currentmin()
                && $"timestamp" >= new Timestamp(latestTime.getTime() + 60000L)
            )

        // aggreaga calcolando media per minuto
        val step2 = step1
          .groupBy(window($"timestamp", "1 minute"))
          .agg(avg("askprice"))
          .select("window.start", "avg(askprice)")
          .withColumnRenamed("start", "timestamp")
          .withColumnRenamed("avg(askprice)", "avgaskprice")

        // join con se stesso al minuto successivo per capire se il prezzo medio
        // al minuto successivo ?? maggiore o minore
        val trendPerMin = step2
          .join(
            step2
              .withColumnRenamed("timestamp", "new_timestamp")
              .withColumnRenamed("avgaskprice", "new_avgaskprice")
          )
          // minuto positivo se il prossimo minuto ha un valore maggiore
          .filter(expr("timestamp + interval '1 minute' = new_timestamp"))
          // calcolo del trend
          .withColumn("asktrend", $"new_avgaskprice" >= $"avgaskprice")
          .select("timestamp", "asktrend", "avgaskprice")
          // scrive su database
          .write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcUrl)
          .option("dbtable", "trendperminute")
          .option("user", "postgres")
          .option("password", jdbcPassword)
          .mode(SaveMode.Append)
          .save()
      }
    }
    // eseguito ogni minuto
    timer.schedule(task, 0, 60000L)

    tweets.awaitTermination
    binance.awaitTermination
    tweets.stop
    binance.stop
    timer.cancel()

    // commandline exit code
    0
  }
}

object Main {
  def main(args: Array[String]) {
    System.exit(new CommandLine(new Main()).execute(args: _*))
  }
}
