from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sample").getOrCreate()

# In Python
# Loading data from a JDBC source using load
jdbcDF = (
    spark.read.format("jdbc")
    .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "[TABLENAME]")
    .option("user", "[USERNAME]")
    .option("password", "[PASSWORD]")
    .load()
)

# Saving data to a JDBC source using save
(
    jdbcDF.write.format("jdbc")
    .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "[TABLENAME]")
    .option("user", "[USERNAME]")
    .option("password", "[PASSWORD]")
    .save()
)
