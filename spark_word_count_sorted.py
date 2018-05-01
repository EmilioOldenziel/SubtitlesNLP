
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from os import path

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport()
    .getOrCreate()


df_wc = df_xml \
    .withColumn("word", explode(col("w"))) \
    .drop("w") \
    .groupBy("word").agg(count(col("word"))) \
    .select(col("word"),col("count(word)").alias("frequency")) \
    .sort(col("frequency").desc())

print(df_wc.count(), "words found")
print("saving dataframe...")
df_wc.write.mode("overwrite").saveAsTable("wc_nl")
print("saved, language")

spark.sparkContext.stop()
