
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from os import environ, path

# get language to read argument
parser = argparse.ArgumentParser()
parser.add_argument('--language', help='give language', required=True)
args = parser.parse_args()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

# load spark-xml and set driver memory (150GB)
submit_args = [
    '--packages com.databricks:spark-xml_2.11:0.4.1',
    '--driver-memory 15g',
    'pyspark-shell'
]
environ['PYSPARK_SUBMIT_ARGS'] =  " ".join(submit_args)

# set jvm file encoding to utf-8
environ['JAVA_TOOL_OPTIONS'] = "-Dfile.encoding=UTF8"

spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

schema = StructType([
    StructField('w', ArrayType(StringType()))
])

language = args.language

# read xml files as df
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "s") \
    .option("rootTag", "document") \
    .load("./subtitles/"+ language + "/*/*/*.xml.gz", schema=schema) # spark does not support recursive load

print("saving", language)
df_xml.write.mode("overwrite").saveAsTable(language)

spark.sparkContext.stop()
