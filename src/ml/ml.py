from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

filePath = """data/sf-airbnb/sf-airbnb-clean.parquet/"""
airbnbDF = spark.read.parquet(filePath)

airbnbDF.select(
  "neighbourhood_cleansed",
  "room_type",
  "bedrooms",
  "bathrooms",
  "number_of_reviews",
  "price").show(5)

trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
print(f"""There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set""")

vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features") vecTrainDF = vecAssembler.transform(trainDF)
vecTrainDF.select("bedrooms", "features", "price").show(10)

lr = LinearRegression(featuresCol="features", labelCol="price")
lrModel = lr.fit(vecTrainDF)

m = round(lrModel.coefficients[0], 2)
b = round(lrModel.intercept, 2)
print(f"""The formula for the linear regression line is price = {m}*bedrooms + {b}""")

pipeline = Pipeline(stages=[vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)
predDF.select("bedrooms", "features", "price", "prediction").show(10)
