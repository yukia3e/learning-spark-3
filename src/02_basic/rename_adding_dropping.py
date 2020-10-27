from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import get_fire_df

if __name__ == "__main__":
  spark = SparkSession.builder.appName("rename_adding_dropping").getOrCreate()

  fire_df = get_fire_df.get_fire_df(spark)
  fire_df.show(5)

  # Rename
  new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedMins")
  new_fire_df.show(5)
  (new_fire_df
    .select("ResponseDelayedMins")
    .where(F.col("ResponseDelayedMins") > 5)
    .show(5, False)
  )

  # Convert string to timestamp/date, and drop
  fire_ts_df = (new_fire_df
                .withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))
                .drop("CallDate")
                .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
                .drop("WatchDate")
                .withColumn("AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                .drop("AvailableDtTm"))

  (fire_ts_df.select("IncidentDate", "OnWatchDate",
                     "AvailableDtTS").where(F.col("IncidentDate").isNotNull()).show(5, False))


# In Python
  (fire_ts_df
   .select(F.year('IncidentDate'))
   .distinct()
   .orderBy(F.year('IncidentDate'))
   .show())
