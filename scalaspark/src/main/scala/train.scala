import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

import org.apache.spark.sql.types.{
  StructType,
  DateType,
  TimestampType,
  LongType,
  DoubleType,
  BooleanType,
  StringType
}

// Input data: Each row is a bag of words from a sentence or document.
val tweetsDB = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
    .option("dbtable", "tweets")
    .option("user", "postgres")
    .option("password", "password")
    .load()

val pricesDB = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
    .option("dbtable", "prices")
    .option("user", "postgres")
    .option("password", "password")
    .load()

val tweetsTrain = tweetsDB.select("text").limit(10).as[String].map(str => str.split(" "))

// Learn a mapping from words to Vectors.
val word2Vec = new Word2Vec()
  .setInputCol("value")
  .setOutputCol("word2vec")
  .setVectorSize(3)
  .setMinCount(0)
val model = word2Vec.fit(tweetsTrain)

val result = model.transform(tweetsTrain)
result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
  println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

// trend per minute
val trendPerMin1 = pricesDB.filter(expr("lastmasktrend is not null"))
    .groupBy(window($"timestamp", "1 minute"), $"lastmasktrend").count()

val trendPerMin2 = trendPerMin1.groupBy("window")
    .agg(max($"count")).withColumnRenamed("max(count)", "max")

val trendPerMin = trendPerMin1
    .join(trendPerMin2, "window")
    .filter(expr("max = count"))
    .withColumn("timestamp", $"window.start")
    .select("timestamp", "lastmasktrend")