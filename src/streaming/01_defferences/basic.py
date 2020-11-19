# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("streaming_basic").getOrCreate()

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


# 3. Define output sink and output mode
# outputMode: Append / Complete / Update
writer = counts.writeStream.format("console").outputMode("complete")

# 4. Specify processing details
checkpointDir = ""
writer2 = writer.trigger(processingTime="1 second").option(
    "checkpointLocation", checkpointDir
)

# 5. Start the query
streamingQuery = writer2.start()
# You can wait up to a timeout duration using awaitTermination(timeoutMillis),
# and you can explicitly stop the query with streamingQuery.stop().
