from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("spark-sql-practice").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/Users/peter/SparkCourse/fakefriends-header.csv")

# my answer
tmp = df.select("age","friends")
tmp.groupBy("age").avg("friends").sort("age").show()

# formatted more nicely
tmp.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show(40)

spark.stop()