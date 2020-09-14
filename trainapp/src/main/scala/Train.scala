import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions._
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
import opennlp.tools.stemmer.PorterStemmer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

import picocli.CommandLine
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine._

import java.util.concurrent.Callable

@Command(
  name = "StreamApp TrainApp",
  version = Array("StreamApp TrainApp v1.0"),
  mixinStandardHelpOptions = true, // add --help and --version options
  description = Array("StreamApp model training app")
)
class Main extends Callable[Int] {

  @Option(
    names = Array("--lrmodel"),
    paramLabel = "LOGISTIC_REGRESSION_MODEL_PATH",
    description = Array("Logistic regression model path")
  )
  var lrModelPath : String = "/tmp/models/spark-logistic-regression-model"

  @Option(
    names = Array("--vcmodel"),
    paramLabel = "VECTORIZER_MODEL_PATH",
    description = Array("Vectorizer model path")
  )
  var vectorizerModelPath : String = "/tmp/models/spark-cv-model"

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

  def call(): Int = {

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

    // Input data: Each row is a bag of words from a sentence or document.
    val tweetsDB = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", "tweets")
      .option("user", "postgres")
      .option("password", jdbcPassword)
      .load()

    val trendperminDB = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", "trendperminute")
      .option("user", "postgres")
      .option("password", jdbcPassword)
      .load()

    // def myf(x: Row) : (Long, Timestamp, String, Integer) = {
    //   var trend :String = " malissimo "
    //   if (x.getAs[Integer](4) == 1) {
    //       trend = " benissimo "
    //   }
    //   return (x.getAs[Long](0), x.getAs[Timestamp](1), trend + x.getAs[String](2), x.getAs[Integer](4))
    // }

    val joined = tweetsDB
      .join(trendperminDB
        .withColumnRenamed("timestamp", "trend_timestamp")
        .withColumnRenamed("id", "trend_id")
        .withColumnRenamed("asktrend", "nextminasktrend"))
      // .filter(expr("timestamp < trend_timestamp and timestamp >= trend_timestamp - interval '1 minute'"))
      // prendo tweets per ogni minuto
      .filter(expr("timestamp >= trend_timestamp and timestamp < trend_timestamp + interval '1 minute'"))
      .select("id", "timestamp", "text", "nextminasktrend")
      .withColumn("label", $"nextminasktrend".cast(IntegerType))
      // .map(x => myf(x)).toDF.select($"_1".as("id"), $"_2".as("timestamp"), $"_3".as("text"), $"_4".as("label"))

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

    val preprocessed = remover.transform(tokenizer.transform(joined))
                        .select("id", "timestamp", "words", "label")
                        .map(x => (x.getAs[Long](0), x.getAs[Timestamp](1),
                            x.getAs[Seq[String]](2)
                              .filter(_.length>0)
                              .map(y => new PorterStemmer().stem(y)),
                            x.getAs[Integer](3)))
                        .toDF
                        .select($"_1".as("id"), $"_2".as("timestamp"), $"_3".as("words"), $"_4".as("label"))

    // Learn a mapping from words to Vectors.
    val vectorizerModel = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")
      .setVectorSize(3)
      .setMinCount(0)
      .fit(preprocessed)
    val allData = vectorizerModel.transform(preprocessed)
    
    // val vectorizerModel: CountVectorizerModel = new CountVectorizer()
    //   .setInputCol("words")
    //   .setOutputCol("features")
    //   .setVocabSize(3)
    //   .setMinDF(2)
    //   .fit(preprocessed)
    // val allData = vectorizerModel.transform(preprocessed)

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

    // val newdata = allData.map(x => {
    //       val toadd = if (x.getAs[Boolean](4)) "up" else "down"
    //       (x.getAs[Long](0), x.getAs[Timestamp](1), x.getAs[WrappedArray[String]](2) :+ toadd,
    //       x.getAs[Integer](5))
    //   }).withColumnRenamed("_1", "id").withColumnRenamed("_2", "timestamp").withColumnRenamed("_3", "words").withColumnRenamed("_4", "label")


    // allData = vectorizerModel.transform(newdata)

    val seed = 1287638
    val Array(trainingData, testData) = allData.randomSplit(Array(0.7, 0.3), seed)

    val lr = new LogisticRegression()
      .setMaxIter(1000)
      .setFeaturesCol("features")
      .setLabelCol("label")

    val lrModel = lr.fit(trainingData)

    val trainPrediction = lrModel.transform(trainingData)
    val testPrediction = lrModel.transform(testData)
    val allPrediction = lrModel.transform(allData)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    println("train evaluation: " + evaluator.evaluate(trainPrediction))
    println("test evaluation: " + evaluator.evaluate(testPrediction))
    println("all evaluation: " + evaluator.evaluate(allPrediction))

    val finalModel = lr.fit(allData)

    finalModel.write.overwrite().save(lrModelPath)
    vectorizerModel.write.overwrite().save(vectorizerModelPath)

    // val loadedModel = LogisticRegressionModel.load("hdfs://localhost:9000/tmp/models/spark-logistic-regression-model")
    // val loadedvectorizerModel = CountVectorizerModel.load("hdfs://localhost:9000/tmp/models/spark-cv-model")
    
    0
  }

}

object Main {
  def main(args: Array[String]) {
    System.exit(new CommandLine(new Main()).execute(args: _*))
  }
}
