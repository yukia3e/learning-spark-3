from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import get_fire_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("aggregations").getOrCreate()

    fire_df = get_fire_df.get_fire_df(spark)

    # Rename
    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    (
        new_fire_df.select("ResponseDelayedinMins")
        .where(F.col("ResponseDelayedinMins") > 5)
        .show(5, False)
    )

    # Convert string to timestamp/date, and drop
    fire_ts_df = (
        new_fire_df.withColumn(
            "IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy")
        )
        .drop("CallDate")
        .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn(
            "AvailableDtTS",
            F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"),
        )
        .drop("AvailableDtTm")
    )

    # groupBy and count
    (
        fire_ts_df.select("CallType")
        .where(F.col("CallType").isNotNull())
        .groupBy("CallType")
        .count()
        .orderBy("count", ascending=False)
        .show(n=10, truncate=False)
    )

    (
        fire_ts_df.select(
            F.sum("NumAlarms"),
            F.avg("ResponseDelayedinMins"),
            F.min("ResponseDelayedinMins"),
            F.max("ResponseDelayedinMins"),
        ).show()
    )

    # Basic Statistics
    # https://spark.apache.org/docs/latest/ml-statistics.html
    # stat
    fire_ts_df.stat()

    # describe （要約統計量）
    fire_ts_df.describe(["FinalPriority", "NumAlarms"]).show()

    # correlation （2変数のピアソン相関係数）
    print(fire_ts_df.corr("FinalPriority", "NumAlarms"))

    # covariance （2変数の標本共分散）
    print(fire_ts_df.cov("FinalPriority", "NumAlarms"))

    # sampleBy
    # approxQuantile
    # frequentItems
