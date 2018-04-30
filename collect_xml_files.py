# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, count
from os import environ, path

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 
spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

take_first = udf(lambda l: l[0]) # take first from list

language = "nl"

# read xml files as df
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "s") \
    .option("rootTag", "document") \
    .load("./subtitles/"+ language + "/*/*/*/2015/*/*.xml.gz") # spark does not support recursive load

print(df_wc.count(), "words found")
print("saving dataframe...")
df_wc.write.saveAsTable(language)
print("saved, language")

spark.sparkContext.stop()