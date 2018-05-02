# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from os import path

parser = argparse.ArgumentParser()
parser.add_argument('--l', help='give language', required=True)
args = parser.parse_args()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

language = args.l

df_subtitles = spark.sql("SELECT * FROM " + language)

df_wc = df_subtitles \
    .withColumn("word", explode(col("w"))) \
    .drop("w") \
    .groupBy("word").agg(count(col("word"))) \
    .select(col("word"),col("count(word)").alias("frequency")) \
    .sort(col("frequency").desc())

print(df_wc.count(), "words counted for", language)
print("saving dataframe...")
df_wc.write.mode("overwrite").saveAsTable("wc_"+language)
print("saved", language)

spark.sparkContext.stop()
