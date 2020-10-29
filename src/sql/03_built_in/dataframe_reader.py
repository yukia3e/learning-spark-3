from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dataframe_reader").getOrCreate()

# Use Parquet
file = "data/parquet/test.paruquet"
df = spark.read.format("parquet").load(file)

# Use Parquet
# you can omit format("parquet") if you wish as it's the default
df2 = spark.read.load(file)

# Use CSV
df3 = spark.read.format("csv")
.option("inferSchema", "true")
.option("header", "true")
.option("mode", "PERMISSIVE")
.load("data/csv/*")

# Use JSON
df4 = spark.read.format("json") .load("data/json/*")
