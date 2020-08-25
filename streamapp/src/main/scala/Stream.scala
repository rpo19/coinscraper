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

object Main {
  def main(args: Array[String]) {

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
      .add("timestamp", "double")

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
      .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
      .option("dbtable", "prices")
      .option("user", "postgres")
      .option("password", "password")
      .load()

    val tweets = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "tweets-bitcoin")
      .load
      .select(from_json($"value".cast("string"), tweets_schema).alias("value"))
      .withColumn(
        "timestamp",
        ($"value.timestamp_ms".cast(LongType) / 1000).cast(TimestampType)
      )
      .drop("value.timestamp_ms")
      .select("timestamp", "value.text")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
          .option("dbtable", "tweets")
          .option("user", "postgres")
          .option("password", "password")
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    val binance = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "binance-BTCUSDT")
      .load
      .select(from_json($"value".cast("string"), binance_schema).alias("value"))
      .withColumn("askprice", $"value.a".cast(DoubleType))
      .withColumn("askqty", $"value.A".cast(DoubleType))
      .withColumn("bidprice", $"value.b".cast(DoubleType))
      .withColumn("bidqty", $"value.B".cast(DoubleType))
      .withColumn("symbol", $"value.s")
      .withColumn("timestamp", current_timestamp())
      .drop("value")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          .write
          .format("jdbc")
          .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
          .option("dbtable", "prices")
          .option("user", "postgres")
          .option("password", "password")
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    val lastTrendPerMinDB = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
      .option("dbtable", "trendperminute")
      .option("user", "postgres")
      .option("password", "password")
      .load()
      .agg(max("timestamp"))

    def currentmin() : Timestamp = {
      val now = System.currentTimeMillis()
      return new Timestamp(now - now % 60000L)
    }

    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = {
        val latestTime = lastTrendPerMinDB.take(1)(0)(0).asInstanceOf[Timestamp]

        val step1 = if (latestTime == null)
            pricesDB.filter($"timestamp" < currentmin())
          else
            pricesDB.filter($"timestamp" < currentmin()
              && $"timestamp" >= new Timestamp(latestTime.getTime() + 60000L))

        val step2 = step1
          .groupBy(window($"timestamp", "1 minute"))
          .agg(avg("askprice"))
          .select("window.start", "avg(askprice)")
          .withColumnRenamed("start", "timestamp")
          .withColumnRenamed("avg(askprice)", "avgaskprice")

        val trendPerMin = step2
          .join(step2
            .withColumnRenamed("timestamp", "old_timestamp")
            .withColumnRenamed("avgaskprice", "old_avgaskprice"))
          .filter(expr("old_timestamp = timestamp - interval '1 minute'"))
          .withColumn("asktrend", $"avgaskprice" >= $"old_avgaskprice")
          .select("timestamp", "asktrend")
          .write
          .format("jdbc")
          .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
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

  }

}
