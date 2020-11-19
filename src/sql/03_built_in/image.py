from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("image").getOrCreate()

image_dir = "data/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()
