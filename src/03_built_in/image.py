from pyspark.ml import image

image_dir = "data/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()
