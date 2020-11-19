from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_save").getOrCreate()

schema = "`Id` INT, `First` STRING, `last` STRING, `Url` STRING, `Published` STRING, '\
  '`Hits` INT, `Campaings` ARRAY<STRING>"

data = [
    [
        1,
        "Jules",
        "Damji",
        "https://tinyurl.1",
        "1/4/2016",
        4535,
        ["twitter", "LinkedIn"],
    ],
]

df = spark.createDataFrame(data, schema=schema)

location = ""

# ...
# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter
(
    df.write.format("csv")  # "parquet", "csv", "txt", "json", "jdbc","orc","avro", etc.
    .option(
        "mode", "overwrite"
    )  # append | overwrite | ignore | error or errorifex ists
    .save(location)
)
