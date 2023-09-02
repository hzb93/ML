import pandas as pd
c = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
a = 1
b = 2




from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.parallelize(c)
