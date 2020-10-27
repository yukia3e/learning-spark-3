from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import get_fire_df

spark = SparkSession.builder.appName("aggregations").getOrCreate()

fire_df = get_fire_df.get_fire_df(spark)
