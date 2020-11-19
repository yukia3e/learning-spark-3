from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from . import get_fire_df

spark = SparkSession.builder.appName("dataframe_reader").getOrCreate()

fire_df = get_fire_df.get_fire_df(spark)
fire_df.show(5)

few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
    F.col("CallType") != "Medical Incident"
)

few_fire_df.show(5, truncate=False)

(
    fire_df.select("CallType")
    .where(F.col("CallType").isNotNull())
    .agg(F.countDistinct("CallType").alias("DistinctCallTypes"))
    .show()
)

(
    fire_df.select("CallType")
    .where(F.col("CallType").isNotNull())
    .distinct()
    .show(10, False)
)
