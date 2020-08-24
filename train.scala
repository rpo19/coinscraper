import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.types.{
  StructType,
  DateType,
  TimestampType,
  LongType,
  DoubleType,
  BooleanType,
  StringType,
  IntegerType
}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

val pricesDB = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
    .option("dbtable", "prices")
    .option("user", "postgres")
    .option("password", "password")
    .load()

// Input data: Each row is a bag of words from a sentence or document.
val tweetsDB = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
    .option("dbtable", "tweets")
    .option("user", "postgres")
    .option("password", "password")
    .load()

// val tweetsTrain = tweetsDB
//   .map(x => (x.getAs[Long](0), x.getAs[Timestamp](1), x.getAs[String](2).split(" ")))
//   .withColumnRenamed("_1", "id")
//   .withColumnRenamed("_2", "timestamp")
//   .withColumnRenamed("_3", "words")
val tokenizer = new Tokenizer()
  .setInputCol("text")
  .setOutputCol("tokens")

val remover = new StopWordsRemover()
  .setInputCol("tokens")
  .setOutputCol("words")

val tweetsTrain = remover.transform(tokenizer.transform(tweetsDB))

// Learn a mapping from words to Vectors.
val word2Vec = new Word2Vec()
  .setInputCol("words")
  .setOutputCol("word2vec")
  .setVectorSize(3)
  .setMinCount(0)
val model = word2Vec.fit(tweetsTrain)

val result = model.transform(tweetsTrain)

// trend per minute
// val trendPerMin1 = pricesDB.filter(expr("lastmasktrend is not null"))
//     .groupBy(window($"timestamp", "1 minute"), $"lastmasktrend").count()

// val trendPerMin2 = trendPerMin1.groupBy("window")
//     .agg(max($"count")).withColumnRenamed("max(count)", "max")

// val trendPerMin = trendPerMin1
//     .join(trendPerMin2, "window")
//     .filter(expr("max = count"))
//     .withColumn("timestamp", $"window.start")
//     .select("timestamp", "lastmasktrend")

val trendperminDB = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
    .option("dbtable", "trendperminute")
    .option("user", "postgres")
    .option("password", "password")
    .load()


val allData = result
  .join(trendperminDB
    .withColumnRenamed("timestamp", "trend_timestamp")
    .withColumnRenamed("id", "trend_id")
    .withColumnRenamed("asktrend", "nextminasktrend"))
  .filter(expr("timestamp < trend_timestamp and timestamp > trend_timestamp - interval '1 minute'"))
  .select("id", "timestamp", "words", "word2vec", "nextminasktrend")
  .withColumnRenamed("word2vec", "features")
  .withColumn("label", $"nextminasktrend".cast(IntegerType))

val seed = 5043
val Array(trainingData, testData) = allData.randomSplit(Array(0.7, 0.3), seed)

val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)
  .setFeaturesCol("features")
  .setLabelCol("label")

val lrModel = lr.fit(trainingData)

val prediction = lrModel.transform(testData)

val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("prediction")
  .setMetricName("areaUnderROC")

val evaluation = evaluator.evaluate(prediction)