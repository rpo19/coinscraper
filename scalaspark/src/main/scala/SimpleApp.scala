import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Cons test").getOrCreate()

    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load()

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    df.writeStream.start()

  }

}