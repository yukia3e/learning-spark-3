from __future__ import print_function
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("columns").getOrCreate()

schema = "`Id` INT, `First` STRING, `last` STRING, `Url` STRING, `Published` STRING, '\
  '`Hits` INT, `Campaings` ARRAY<STRING>"

data = [
    [
        1,
        "Jules",
        "Damji",
        "https://tinyurl.1",
        "1/4/2016",
        4535,
        ["twitter", "LinkedIn"],
    ],
    [
        2,
        "Brooke",
        "Wenig",
        "https://tinyurl.2",
        "5/5/2018",
        8908,
        ["twitter", "LinkedIn"],
    ],
    [
        3,
        "Denny",
        "Lee",
        "https://tinyurl.3",
        "6/7/2019",
        7659,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
    [
        5,
        "Matei",
        "Zaharia",
        "https://tinyurl.5",
        "5/14/2014",
        40578,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [
        6,
        "Reynold",
        "Xin",
        "https://tinyurl.6",
        "3/2/2015",
        25568,
        ["twitter", "LinkedIn"],
    ],
]

blogs_df = spark.createDataFrame(data, schema)

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
(
    blogs_df.withColumn(
        "AuthorsId", (F.concat(F.expr("First"), F.expr("Last"), F.expr("Id")))
    )
    .select(F.col("AuthorsId"))
    .show(4)
)

# These statements return the same value, showing that
# expr is the same as a col method call
blogs_df.select(F.expr("Hits")).show(2)
blogs_df.select(F.col("Hits")).show(2)
blogs_df.select("Hits").show(2)

# Sort by column "Id" in descending order
blogs_df.sort(F.col("Id").desc()).show()
