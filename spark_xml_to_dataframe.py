
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from os import environ, path

# get language to read argument
parser = argparse.ArgumentParser()
parser.add_argument('--l', help='give language', required=True)
args = parser.parse_args()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

# load spark-xml
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.4.1 pyspark-shell' 

spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

schema = StructType([
    StructField('w', ArrayType(StringType()))
])

language = args.l

# read xml files as df
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "s") \
    .option("rootTag", "document") \
    .load("./subtitles/"+ language + "/*/*/*.xml.gz", schema=schema) # spark does not support recursive load

print("saving", language)
df_xml.write.mode("overwrite").saveAsTable(language)

spark.sparkContext.stop()
