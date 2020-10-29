from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

"""
spark-submit mnmcount.py data/mnm_dataset.csv
"""
if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: mnmcount <file>", file=sys.stderr)
    sys.exit

  # Build a SparkSession using the SparkSession APIs.
  spark = (SparkSession
    .builder
    .appName("PythonMnMCount")
    .getOrCreate()
  )

  # Read the file into a Spark DataFrame using the CSV
  mnm_file = sys.argv[1]

  mnm_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(mnm_file)
  )

  # We use the DataFrame high-level APIs. 
  count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False)
  )

  # Show the resulting aggregations for all the states and colors
  count_mnm_df.show(n=60, truncate=False)
  print("Total Rows = %d" % (count_mnm_df.count()))

  ca_count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False)
  )

  ca_count_mnm_df.show(n=10, truncate=False)
  spark.stop()

  # Check explain
  count_mnm_df.explain(True)
