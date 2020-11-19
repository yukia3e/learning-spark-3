from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("concat").getOrCreate()

data_list = [
    ("a", 2, 3),
    ("b", 5, 6),
    ("c", 8, 9),
    ("a", 2, 3),
    ("b", 5, 6),
    ("c", 8, 9),
]
col_name = ["col1", "col2", "col3"]

df = spark.createDataFrame(data_list, schema=col_name)

# concat
df.withColumn("concat", F.concat("col1", "col2")).show()

# groupBy -> agg
df.groupBy(["col1"]).agg({"col2": "min", "col3": "avg"}).show()
