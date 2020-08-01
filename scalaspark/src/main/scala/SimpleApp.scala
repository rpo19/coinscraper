import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Cons test").getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
    val df2 = df
      .selectExpr("CAST(value AS STRING)")

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    //https://www.thetopsites.net/article/51617823.shtml
    val query = df2.writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }

}
