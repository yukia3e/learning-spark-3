from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("transform").getOrCreate()

# Create TestData
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]

t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

t_c.show()

# transform
spark.sql("""
SELECT
  celsius,
  transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
FROM tC
""").show()

# filter
spark.sql("""
SELECT
  celsius,
  filter(celsius, t -> t > 38) as high
FROM tC
""").show()

# exists
spark.sql("""
SELECT
  celsius,
  exists(celsius, t -> t=38) as threshold
FROM tC
""").show()

# reduce
spark.sql("""
SELECT
  celsius,
  reduce(
    celsius,
    0,
    (t, acc) -> t + acc,
    acc -> (acc div size(celsius) * 9 div 5) + 32
  ) as avgFahrenheit
FROM tC
""").show()
