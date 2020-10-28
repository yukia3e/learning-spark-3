from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_save").getOrCreate()

# ...
# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter
(df.write
  .format("csv")  # "parquet", "csv", "txt", "json", "jdbc","orc","avro", etc.
  .option("mode", "overwrite") # append | overwrite | ignore | error or errorifex ists
  .save(location))
