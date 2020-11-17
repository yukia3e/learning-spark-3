from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder.appName('window').getOrCreate()

d = {'A': ['a', 'b', 'c', 'd'], 'B': ['m', 'm', 'n', 'n'], 'C': [1, 2, 3, 6]}
dp = pd.DataFrame(d)
df = spark.createDataFrame(dp)

# Window
w = Window.partitionBy('B').orderBy(df.C.desc())
df.withColumn('rank', F.rank().over(w)).show()
