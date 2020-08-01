import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Cons test").getOrCreate()
    import spark.implicits._

    //https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-kafka-data-source.html
    val sq = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load
      .withColumn("value", $"value" cast "string")
      .writeStream
      .format("console")
      .option("truncate", false)
      .option("checkpointLocation", "checkpointLocation-kafka2console")
      .queryName("kafka2console-continuous")
      .trigger(Trigger.Continuous(1.seconds))
      .start

    sq.awaitTermination
    sq.stop

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
