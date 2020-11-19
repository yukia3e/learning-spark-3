from pyspark.sql import SparkSession
from pyspark.sql.types import LongType


def cubed(s):
    return s * s * s


spark = SparkSession.builder.appName("simples_samples").getOrCreate()

# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# Query the cubed UDF
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
