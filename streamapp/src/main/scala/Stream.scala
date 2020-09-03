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

import picocli.CommandLine
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine._

import java.util.concurrent.Callable

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
  var lrModelPath =
    "hdfs://localhost:9000/tmp/models/spark-logistic-regression-model"

  @Option(
    names = Array("--vcmodel"),
    paramLabel = "VECTORIZER_MODEL_PATH",
    description = Array("Vectorizer model path")
  )
  var vectorizerModelPath = "hdfs://localhost:9000/tmp/models/spark-cv-model"

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
    description = Array("Kafka bootstrap servers (comma separeted if more than one)")
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

    val lrModel = LogisticRegressionModel.load(lrModelPath)
    // val vectorozerModel = CountVectorizerModel.load("hdfs://localhost:9000/tmp/models/spark-cv-model")
    val vectorizerModel = Word2VecModel.load(vectorizerModelPath)

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

    val binance_schema = new StructType()
      .add("u", "long")
      .add("s", "string")
      .add("b", "string")
      .add("B", "string")
      .add("a", "string")
      .add("A", "string")
      .add("timestamp", "string")

    val spark = SparkSession
      .builder()
      .appName("Cons test")
      .config("spark.sql.caseSensitive", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("Startup completed")

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

    val tweets = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", tweetsTopic)
      .load
      .withColumn("receivedat", current_timestamp())
      .select(
        $"receivedat",
        from_json($"value".cast("string"), tweets_schema).alias("value")
      )
      .withColumn(
        "timestamp",
        ($"value.timestamp_ms".cast(LongType) / 1000).cast(TimestampType)
      )
      .drop("value.timestamp_ms")
      .select("timestamp", "value.text", "receivedat")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val preprocessed = remover
          .transform(tokenizer.transform(batchDF))
          .select("timestamp", "receivedat", "text", "words")
          .map(x =>
            (
              x.getAs[Timestamp](0),
              x.getAs[Timestamp](1),
              x.getAs[String](2),
              x.getAs[Seq[String]](3)
                .filter(_.length > 0)
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

        lrModel
          .transform(vectorizerModel.transform(preprocessed))
          .withColumn("b_prediction", $"prediction".cast(BooleanType))
          .drop("prediction")
          .withColumnRenamed("b_prediction", "prediction")
          .select("timestamp", "text", "prediction", "receivedat")
          .withColumn("processedat", current_timestamp())
          .write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcUrl)
          .option("dbtable", "tweets")
          .option("user", "postgres")
          .option("password", "password")
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    val binance = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", pricesTopic)
      .load
      .withColumn("receivedat", current_timestamp())
      .select(
        $"receivedat",
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
        batchDF
          .withColumn("processedat", current_timestamp())
          .write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcUrl)
          .option("dbtable", "prices")
          .option("user", "postgres")
          .option("password", "password")
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    val lastTrendPerMinDB = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", "trendperminute")
      .option("user", "postgres")
      .option("password", "password")
      .load()
      .agg(max("timestamp"))

    def currentmin(): Timestamp = {
      val now = System.currentTimeMillis()
      return new Timestamp(now - now % 60000L)
    }

    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = {
        val latestTime = lastTrendPerMinDB.take(1)(0)(0).asInstanceOf[Timestamp]

        val step1 =
          if (latestTime == null)
            pricesDB.filter($"timestamp" < currentmin())
          else
            pricesDB.filter(
              $"timestamp" < currentmin()
                && $"timestamp" >= new Timestamp(latestTime.getTime() + 60000L)
            )

        val step2 = step1
          .groupBy(window($"timestamp", "1 minute"))
          .agg(avg("askprice"))
          .select("window.start", "avg(askprice)")
          .withColumnRenamed("start", "timestamp")
          .withColumnRenamed("avg(askprice)", "avgaskprice")

        val trendPerMin = step2
          .join(
            step2
              .withColumnRenamed("timestamp", "old_timestamp")
              .withColumnRenamed("avgaskprice", "old_avgaskprice")
          )
          .filter(expr("old_timestamp = timestamp - interval '1 minute'"))
          .withColumn("asktrend", $"avgaskprice" >= $"old_avgaskprice")
          .select("timestamp", "asktrend")
          .write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcUrl)
          .option("dbtable", "trendperminute")
          .option("user", "postgres")
          .option("password", "password")
          .mode(SaveMode.Append)
          .save()
      }
    }
    timer.schedule(task, 0, 60000L)

    tweets.awaitTermination
    binance.awaitTermination
    tweets.stop
    binance.stop
    timer.cancel()

    0
  }
}

object Main {
  def main(args: Array[String]) {
    System.exit(new CommandLine(new Main()).execute(args: _*))
  }
}
