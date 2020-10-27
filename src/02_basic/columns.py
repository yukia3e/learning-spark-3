from __future__ import print_function
from pyspark.sql import SparkSession, types as T, functions as F

# Access a particular column with col and it returns a Column type
blogs_df.columns

# Use an expression to compute a value
blogs_df.select(F.expr("Hits * 2")).show(2)

# or use col to compute value
blogs_df.select(F.col("Hits") * 2).show(2)

# Use an expression to compute big hitters for blogs
# This adds a new column, Big Hitters, based on the conditional expression
blogs_df.withColumn("Big Hitters", (F.expr("Hits > 10000"))).show()

# Concatenate three columns, create a new column, and show the
# newly created concatenated column
(blogs_df
  .withColumn("AuthorsId", (F.concat(F.expr("First"), F.expr("Last"), F.expr("Id"))))
  .select(F.col("AuthorsId"))
  .show(4))

# These statements return the same value, showing that 
# expr is the same as a col method call
blogs_df.select(F.expr("Hits")).show(2)
blogs_df.select(F.col("Hits")).show(2)
blogs_df.select("Hits").show(2)

# Sort by column "Id" in descending order
blogs_df.sort(F.col("Id").desc()).show()
