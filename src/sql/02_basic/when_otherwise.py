from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("with_new_column").getOrCreate()

df = spark.createDataFrame(
    [
        (1, 120.2, 19.6, 11.6, 13.2),
        (2, 8.6, 2.1, 1.0, 4.8),
        (3, 214.7, 24.0, 4.0, 17.4),
        (4, 97.5, 7.6, 7.2, 9.7),
    ],
    ["ID", "TV", "Radio", "Newspaper", "Sales"],
)

df.withColumn(
    "cond",
    F.when((df.TV > 100) & (df.Radio < 40), 1).when(df.Sales > 10, 2).otherwise(3),
).show(4)
