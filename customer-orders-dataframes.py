from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("customer-orders").getOrCreate()

schema = StructType([ 
                     StructField("cust_id", IntegerType(), True), 
                     StructField("prod_id", IntegerType(), True), 
                     StructField("amount_spent", FloatType(), True)
                     ])

df = spark.read.schema(schema).csv("/Users/peter/SparkCourse/customer-orders.csv")
total = df.groupBy("cust_id").agg(func.round(func.sum("amount_spent"),2).alias("total_spent")).sort("total_spent")
total.show(total.count())

spark.stop()
