import pyspark

spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("cons") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

print("hello world!!!!!!!!!!!!!!!!!!!")

print(df)