from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("creating_databases_and_tables").getOrCreate()

# In SQL ver.
# creating database
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# creating table
spark.sql(
    "CREATE TABLE managed_us_delay_flights_tbl(date STRING, delay INT, distance INT, "
    "origin STRING, destination STRING)"
)

# unmanaged table with csv
spark.sql(
    """CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
      distance INT, origin STRING, destination STRING)
      USING csv OPTIONS (PATH
      'src/03_sql/data/*.csv')"""
)


# additional: In Dataframe ver.
csv_file = "src/03_sql/data/*.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")


# create view
# In Python
df_sfo = spark.sql(
    """
      SELECT
            date,
            delay,
            origin,
            destination
      FROM us_delay_flights_tbl
      WHERE
            origin='SFO'
    """
)
df_jfk = spark.sql(
    """
      SELECT
            date,
            delay,
            origin,
            destination
      FROM us_delay_flights_tbl
      WHERE
            origin='JFK'
    """
)

# Create a temporary and global temporary view
# (a global temporary view is visible across multiple SparkSessions)
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# drop
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
