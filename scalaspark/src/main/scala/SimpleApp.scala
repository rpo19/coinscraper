import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Cons test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("Startup completed")

    //https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-kafka-data-source.html
    val tweets = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load
      .selectExpr("cast (value as string) value")

    val words = tweets.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count().orderBy($"count".desc)

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination
    query.stop

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
