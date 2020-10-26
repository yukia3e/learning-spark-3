# Learning Spark 3.0

## Init
``` sh
pip install -r requirements.txt
```

``` sh
pip freeze > requirements.txt
```

## How to
### Start PySpark
``` sh
source spark3env/bin/activate
SPARK_HOME=./spark3env/lib/python3.7/site-packages/pyspark
pyspark
```

### Exec spark-submit
``` sh
source spark3env/bin/activate
spark-submit ${TARGET_PY}
```
