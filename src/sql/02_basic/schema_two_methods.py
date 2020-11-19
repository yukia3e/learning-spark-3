from __future__ import print_function
from pyspark.sql import types as T

# Define it programmatically
schema = T.StructType(
    [
        T.StructField("author", T.StringType(), False),
        T.StructField("title", T.StringType(), False),
        T.StructField("pages", T.IntegerType(), False),
    ]
)

# Employ a DDL string
schema = "author STRING, title STRING, pages INT"
