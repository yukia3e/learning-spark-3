from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("cassandra_foreachbatch").getOrCreate()

hostAddr = "<ip address>"
keyspaceName = "<keyspace>"
tableName = "<tableName>"
spark.conf.set("spark.cassandra.connection.host", hostAddr)


def writeCountsToCassandra(updatedCountsDF, batchId):
    (
        updatedCountsDF.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=tableName, keyspace=keyspaceName)
        .save()
    )


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
    counts.writeStream.foreachBatch(writeCountsToCassandra)
    .outputMode("update")
    .option("checkpointLocation", checkpointDir)
    .start()
)
