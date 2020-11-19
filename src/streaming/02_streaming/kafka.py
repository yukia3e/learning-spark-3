from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("kafka").getOrCreate()

# 1. Define input sources (Create DataStreamReader)
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# ----- for restart ------
# filteredLines = lines.filter("isCorruptedUdf(value) = false")
# words = filteredLines.select(split(col("value"), "\\s").alias("word"))

# 2. Transform Data
# Stateless transformations like select(), filter(), map() is supported
words = lines.select(F.split(F.col("value"), "\\s").alias("word"))

# A few Stateful transformations is not supported, but count is supported
counts = words.groupBy("word").count()

checkpointDir = ""

streamingQuery = (
    counts.selectExpr("cast(word as string) as key", "cast(count as string) as value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("topic", "wordCounts")
    .outputMode("update")
    .option("checkpointLocation", checkpointDir)
    .start()
)
