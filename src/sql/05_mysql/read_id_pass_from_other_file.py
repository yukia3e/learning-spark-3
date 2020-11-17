from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName('read_id_pass_from_other_file').getOrCreate()

try:
  login = pd.read_csv(r'login.txt', header=None)
  user = login[0][0]
  password = login[0][1]
  print('User information is ready!')
except:
  print('Login information is not available!!')

host = '##.##.##.##'
db_name = 'db_name'
table_name = 'table_name'

jdbcDF = (spark.read
          .format("jdbc")
          .option("url", "jdbc:mysql://{}:3306/{}".format(host, db_name))
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", table_name)
          .option("user", user)
          .option("password", password)
          .load())
