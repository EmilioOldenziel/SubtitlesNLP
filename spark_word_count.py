# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from os import path, environ

parser = argparse.ArgumentParser()
parser.add_argument('--language', help='give language to parse', required=True)
args = parser.parse_args()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

# set driver memory
submit_args = [
    '--driver-memory 15g',
    'pyspark-shell'
]
environ['PYSPARK_SUBMIT_ARGS'] =  " ".join(submit_args)

spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

language = args.language

df_subtitles = spark.sql("SELECT * FROM " + language)

df_wc = df_subtitles \
    .withColumn("word", explode(col("w"))) \
    .drop("w") \
    .withColumn('word', lower(col('word'))) \
    .groupBy("word").agg(count(col("word"))) \
    .select(col("word"),col("count(word)").alias("frequency")) \

print(df_wc.count(), "words counted for", language)
print("saving dataframe...")
df_wc.write.mode("overwrite").saveAsTable("wc_"+language)
print("saved", language)

spark.sparkContext.stop()
