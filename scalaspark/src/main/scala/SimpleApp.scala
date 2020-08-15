import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import org.apache.spark.sql.types.{
  StructType,
  DateType,
  TimestampType,
  LongType,
  DoubleType
}
import org.apache.spark.sql.functions._

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

    //https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-kafka-data-source.html
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
      // .withColumnRenamed("value.timestamp_ms_new", "value.timestamp_ms")
      .select("timestamp", "value.text")
      .writeStream
      .format("console")
      .start()

    val binance = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "binance-BTCUSDT")
      .load
      .select(from_json($"value".cast("string"), binance_schema).alias("value"))
      .withColumn("askPrice", $"value.a".cast(DoubleType))
      .withColumn("askQty", $"value.A".cast(DoubleType))
      .withColumn("bidPrice", $"value.b".cast(DoubleType))
      .withColumn("bidQty", $"value.B".cast(DoubleType))
      .withColumn("symbol", $"value.s")
      .withColumn("timestamp", current_timestamp())
      .drop("value")
      .writeStream
      .format("console")
      .start()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
      .option("dbtable", "public.conditions")
      .option("user", "postgres")
      .option("password", "password")
      .load()

    tweets.awaitTermination
    binance.awaitTermination
    tweets.stop
    binance.stop

    // val df2 = df
    //   .selectExpr("CAST(value AS STRING)")

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    //https://www.thetopsites.net/article/51617823.shtml
    // val query = df2.writeStream
    //   .format("console")
    //   .option("truncate", "false")
    //   .start()
    //   .awaitTermination()

  }

}
